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
import warnings
from abc import ABC, abstractmethod
from typing import Any, get_args, get_type_hints

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.output_parser import ParseOutput


class ScrapesIO(ABC):
    """
    A mixin class for scraping IO channel information from a specific class method.

    Requires that the (static and class) method :meth:`_io_defining_function` be
    specified in child classes, as well as :meth:`_io_defining_function_uses_self`.
    Optionally, :attr:`_output_labels` can be overridden at the class level to avoid
    scraping the return signature for channel labels altogether.

    Class methods:
        preview_input_channels (dict[str, tuple[Any, Any]]): Input channel names paired
            with their type hint (if any, may be `None`) and default value (if any,
            may be `pyiron_workflow.channels.NOT_DATA`).
        preview_output_channels (dict[str, Any]): Output channel names paired with
            their type hint (if any, may be `None`). Channel names are scr

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
        """Must return a static class method."""

    @classmethod
    @abstractmethod
    def _io_defining_function_uses_self(cls) -> bool:
        """Whether the signature of the IO defining function starts with self."""

    _output_labels: tuple[str] | None = None
    _validate_output_labels: bool = True

    __type_hints = None
    __input_args = None
    __init_keywords = None
    __input_preview = None
    __output_preview = None

    @classmethod
    def preview_input_channels(cls) -> dict[str, tuple[Any, Any]]:
        """
        Gives a class-level peek at the expected input.

        Returns:
            dict[str, tuple[Any, Any]]: The channel name and a tuple of its
                corresponding type hint and default value.
        """
        if cls.__input_preview is None:
            cls.__input_preview = cls._build_input_preview()
        return cls.__input_preview

    @classmethod
    def _build_input_preview(cls):
        type_hints = cls._get_type_hints()
        scraped: dict[str, tuple[Any, Any]] = {}
        for i, (label, value) in enumerate(cls._get_input_args().items()):
            if cls._io_defining_function_uses_self() and i == 0:
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
    def preview_output_channels(cls) -> dict[str, Any]:
        """
        Gives a class-level peek at the expected output channels.

        Returns:
            dict[str, tuple[Any, Any]]: The channel name and its corresponding type
                hint.
        """
        if cls.__output_preview is None:
            if cls._validate_output_labels:
                cls._validate()  # Validate output on first call
            cls.__output_preview = cls._build_output_preview()
        return cls.__output_preview

    @classmethod
    def _build_output_preview(cls):
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
    def _get_type_hints(cls) -> dict:
        """
        The result of :func:`typing.get_type_hints` on the io-defining function
        """
        if cls.__type_hints is None:
            cls.__type_hints = get_type_hints(cls._io_defining_function())
        return cls.__type_hints

    @classmethod
    def _get_input_args(cls):
        if cls.__input_args is None:
            cls.__input_args = inspect.signature(cls._io_defining_function()).parameters
        return cls.__input_args

    @classmethod
    def _get_init_keywords(cls):
        if cls.__init_keywords is None:
            cls.__init_keywords = list(
                inspect.signature(cls.__init__).parameters.keys()
            )
        return cls.__init_keywords

    @classmethod
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
        cls._validate_degeneracy()
        try:
            cls._validate_return_count()
        except OSError:
            warnings.warn(
                f"Could not find the source code to validate {cls.__name__} output "
                f"labels against the number of returned values -- proceeding without "
                f"validation"
            )

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
