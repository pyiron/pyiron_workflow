"""
Mixin classes for classes which offer previews of input and output at the _class_ level.

The intent is for mixing with :class:`pyiron_workflow.node.Node`, and for the inputs
and outputs to be IO channels there, but in principle this should function just fine
independently.

These previews need to be available at the class level so that suggestion menus and
ontologies can know how mixin classes relate to the rest of the world via input and
output without first having to instantiate them.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from functools import lru_cache, wraps
from typing import (
    Any,
    ClassVar,
    get_args,
    get_type_hints,
    TYPE_CHECKING,
)

from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.logging import logger
from pyiron_workflow.output_parser import ParseOutput

if TYPE_CHECKING:
    pass


class HasIOPreview(ABC):
    """
    An interface mixin guaranteeing the class-level availability of input and output
    previews.

    E.g. for :class:`pyiron_workflow.node.Node` that have input and output channels.
    """

    @classmethod
    @abstractmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        pass

    @classmethod
    @abstractmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        pass

    @classmethod
    @lru_cache(maxsize=1)
    def preview_inputs(cls) -> dict[str, tuple[Any, Any]]:
        """
        Gives a class-level peek at the expected inputs.

        Returns:
            dict[str, tuple[Any, Any]]: The input name and a tuple of its
                corresponding type hint and default value.
        """
        return cls._build_inputs_preview()

    @classmethod
    @lru_cache(maxsize=1)
    def preview_outputs(cls) -> dict[str, Any]:
        """
        Gives a class-level peek at the expected outputs.

        Returns:
            dict[str, tuple[Any, Any]]: The output name and its corresponding type hint.
        """
        return cls._build_outputs_preview()

    @classmethod
    def preview_io(cls) -> DotDict[str:dict]:
        return DotDict(
            {"inputs": cls.preview_inputs(), "outputs": cls.preview_outputs()}
        )


def builds_class_io(subclass_factory: callable[..., type[HasIOPreview]]):
    """
    A decorator for factories producing subclasses of `HasIOPreview` to invoke
    :meth:`preview_io` after the class is created, thus ensuring the IO has been
    constructed at the class level.
    """

    @wraps(subclass_factory)
    def wrapped(*args, **kwargs):
        node_class = subclass_factory(*args, **kwargs)
        node_class.preview_io()
        return node_class

    return wrapped


class ScrapesIO(HasIOPreview, ABC):
    """
    A mixin class for scraping IO channel information from a specific class method's
    signature and returns.

    Requires that the (static and class) method :meth:`_io_defining_function` be
    specified in child classes, as well as :meth:`_io_defining_function_uses_self`.
    Optionally, :attr:`_output_labels` can be overridden at the class level to avoid
    scraping the return signature for channel labels altogether.

    Since scraping returns is only possible when the function source code is available,
    this can be bypassed by manually specifying the class attribute
    :attr:`_output_labels`.

    Attributes:
        _output_labels ():
        _validate_output_labels (bool): Whether to
        _io_defining_function_uses_self (bool): Whether the signature of the IO
            defining function starts with self. When true, the first argument in the
            :meth:`_io_defining_function` is ignored. (Default is False, use the entire
            signature for specifying input.)

    Warning:
        There are a number of class features which, for computational efficiency, get
        calculated at first call and any subsequent calls return that initial value
        (including on other instances, since these are class properties); these
        depend on the :meth:`_io_defining_function` and its signature, which should
        thus be left static from the time of class definition onwards.
    """

    @classmethod
    @abstractmethod
    def _io_defining_function(cls) -> callable:
        """Must return a static method."""

    _output_labels: ClassVar[tuple[str] | None] = None  # None: scrape them
    _validate_output_labels: ClassVar[bool] = True  # True: validate against source code
    _io_defining_function_uses_self: ClassVar[bool] = (
        False  # False: use entire signature
    )

    @classmethod
    def _build_inputs_preview(cls):
        type_hints = cls._get_type_hints()
        scraped: dict[str, tuple[Any, Any]] = {}
        for i, (label, value) in enumerate(cls._get_input_args().items()):
            if cls._io_defining_function_uses_self and i == 0:
                continue  # Skip the macro argument itself, it's like `self` here
            elif label in cls._get_init_keywords():
                # We allow users to parse arbitrary kwargs as channel initialization
                # So don't let them choose bad channel names
                raise ValueError(
                    f"Trying to build input preview for {cls.__name__}, encountered an "
                    f"argument name that conflicts with __init__: {label}. Please "
                    f"choose a name _not_ among {cls._get_init_keywords()}"
                )

            try:
                type_hint = type_hints[label]
            except KeyError:
                type_hint = None

            default = (
                NOT_DATA if value.default is inspect.Parameter.empty else value.default
            )

            scraped[label] = (type_hint, default)
        return scraped

    @classmethod
    def _build_outputs_preview(cls):
        if cls._validate_output_labels:
            cls._validate()  # Validate output on first call

        labels = cls._get_output_labels()
        if labels is None:
            labels = []
        try:
            type_hints = cls._get_type_hints()["return"]
            if len(labels) > 1:
                type_hints = get_args(type_hints)
                if not isinstance(type_hints, tuple):
                    raise TypeError(
                        f"With multiple return labels expected to get a tuple of type "
                        f"hints, but {cls.__name__} got type {type(type_hints)}"
                    )
                if len(type_hints) != len(labels):
                    raise ValueError(
                        f"Expected type hints and return labels to have matching "
                        f"lengths, but {cls.__name__} got {len(type_hints)} hints and "
                        f"{len(labels)} labels: {type_hints}, {labels}"
                    )
            else:
                # If there's only one hint, wrap it in a tuple, so we can zip it with
                # *return_labels and iterate over both at once
                type_hints = (type_hints,)
        except KeyError:  # If there are no return hints
            type_hints = [None] * len(labels)
            # Note that this nicely differs from `NoneType`, which is the hint when
            # `None` is actually the hint!
        return {label: hint for label, hint in zip(labels, type_hints)}

    @classmethod
    def _get_output_labels(cls):
        """
        Return output labels provided for the class, scraping them from the io-defining
        function if they are not already available.
        """
        if cls._output_labels is None:
            cls._output_labels = cls._scrape_output_labels()
        return cls._output_labels

    @classmethod
    @lru_cache(maxsize=1)
    def _get_type_hints(cls) -> dict:
        """
        The result of :func:`typing.get_type_hints` on the io-defining function
        """
        return get_type_hints(cls._io_defining_function())

    @classmethod
    @lru_cache(maxsize=1)
    def _get_input_args(cls):
        return inspect.signature(cls._io_defining_function()).parameters

    @classmethod
    @lru_cache(maxsize=1)
    def _get_init_keywords(cls):
        return list(inspect.signature(cls.__init__).parameters.keys())

    @classmethod
    @lru_cache(maxsize=1)
    def _scrape_output_labels(cls):
        """
        Inspect :meth:`node_function` to scrape out strings representing the
        returned values.

         _Only_ works for functions with a single `return` expression in their body.

        It will return expressions and function calls just fine, thus good practice is
        to create well-named variables and return those so that the output labels stay
        dot-accessible.
        """
        return ParseOutput(cls._io_defining_function()).output

    @classmethod
    def _validate(cls):
        """
        Ensure that output_labels, if provided, are commensurate with graph creator
        return values, if provided, and return them as a tuple.
        """
        try:
            cls._validate_degeneracy()
            cls._validate_return_count()
        except OSError:
            logger.warn(no_output_validation_warning(cls))

    @classmethod
    def _validate_degeneracy(cls):
        output_labels = cls._get_output_labels()
        if output_labels is not None and len(set(output_labels)) != len(output_labels):
            raise ValueError(
                f"{cls.__name__} must not have degenerate output labels: "
                f"{output_labels}"
            )

    @classmethod
    def _validate_return_count(cls):
        output_labels = cls._get_output_labels()
        graph_creator_returns = ParseOutput(cls._io_defining_function()).output
        if graph_creator_returns is not None or output_labels is not None:
            error_suffix = (
                f"but {cls.__name__} got return values: {graph_creator_returns} and "
                f"labels: {output_labels}. If this intentional, you can bypass output "
                f"validation making sure the class attribute `_validate_output_labels` "
                f"is False."
            )
            try:
                if len(output_labels) != len(graph_creator_returns):
                    raise ValueError(
                        "The number of return values must exactly match the number of "
                        "output labels provided, " + error_suffix
                    )
            except TypeError:
                raise TypeError(
                    f"Output labels and return values must either both or neither be "
                    f"present, " + error_suffix
                )


def no_output_validation_warning(cls: type):
    return (
        f"Could not find the source code to validate {cls.__name__} output labels "
        f"against the number of returned values -- proceeding without validation"
    )
