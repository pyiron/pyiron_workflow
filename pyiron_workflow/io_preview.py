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
from textwrap import dedent
from types import FunctionType
from typing import Any, get_args, get_type_hints, Literal, Optional, TYPE_CHECKING

from pyiron_workflow.channels import InputData, NOT_DATA
from pyiron_workflow.injection import OutputDataWithInjection, OutputsWithInjection
from pyiron_workflow.io import Inputs
from pyiron_workflow.node import Node
from pyiron_workflow.output_parser import ParseOutput
from pyiron_workflow.snippets.dotdict import DotDict

if TYPE_CHECKING:
    from pyiron_workflow.composite import Composite


class HasIOPreview(ABC):
    """
    An interface mixin guaranteeing the class-level availability of input and output
    previews.

    E.g. for :class:`pyiron_workflow.node.Node` that have input and output channels.
    """

    @classmethod
    @abstractmethod
    def preview_inputs(cls) -> dict[str, tuple[Any, Any]]:
        """
        Gives a class-level peek at the expected inputs.

        Returns:
            dict[str, tuple[Any, Any]]: The input name and a tuple of its
                corresponding type hint and default value.
        """

    @classmethod
    @abstractmethod
    def preview_outputs(cls) -> dict[str, Any]:
        """
        Gives a class-level peek at the expected outputs.

        Returns:
            dict[str, tuple[Any, Any]]: The output name and its corresponding type hint.
        """

    @classmethod
    def preview_io(cls) -> DotDict[str:dict]:
        return DotDict(
            {"inputs": cls.preview_inputs(), "outputs": cls.preview_outputs()}
        )


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
        """Must return a static class method."""

    _output_labels: tuple[str] | None = None  # None: scrape them
    _validate_output_labels: bool = True  # True: validate against source code
    _io_defining_function_uses_self: bool = False  # False: use entire signature

    __type_hints = None
    __input_args = None
    __init_keywords = None
    __input_preview = None
    __output_preview = None

    @classmethod
    def preview_inputs(cls) -> dict[str, tuple[Any, Any]]:
        if cls.__input_preview is None:
            cls.__input_preview = cls._build_input_preview()
        return cls.__input_preview

    @classmethod
    def preview_outputs(cls) -> dict[str, Any]:
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
    def _build_input_preview(cls):
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
        try:
            cls._validate_degeneracy()
            cls._validate_return_count()
        except OSError:
            warnings.warn(
                f"Could not find the source code to validate {cls.__name__} output "
                f"labels against the number of returned values -- proceeding without "
                f"validation",
                OutputLabelsNotValidated,
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


class OutputLabelsNotValidated(Warning):
    pass


class StaticNode(Node, HasIOPreview, ABC):
    """
    A node whose IO specification is available at the class level.

    Actual IO is then constructed from the preview at instantiation.
    """

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        **kwargs,
    ):
        super().__init__(
            label=label,
            parent=parent,
            save_after_run=save_after_run,
            storage_backend=storage_backend,
        )

        self._inputs = Inputs(
            *[
                InputData(
                    label=label,
                    owner=self,
                    default=default,
                    type_hint=type_hint,
                )
                for label, (type_hint, default) in self.preview_inputs().items()
            ]
        )

        self._outputs = OutputsWithInjection(
            *[
                OutputDataWithInjection(
                    label=label,
                    owner=self,
                    type_hint=hint,
                )
                for label, hint in self.preview_outputs().items()
            ]
        )

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> OutputsWithInjection:
        return self._outputs


class DecoratedNode(StaticNode, ScrapesIO, ABC):
    """
    A static node whose IO is defined by a function's information (and maybe output
    labels).
    """


def decorated_node_decorator_factory(
    parent_class: type[DecoratedNode],
    io_static_method: callable,
    decorator_docstring_additions: str = "",
    **parent_class_attr_overrides,
):
    """
    A decorator factory for building decorators to dynamically create new subclasses
    of some subclass of :class:`DecoratedNode` using the function they decorate.

    New classes get their class name and module set using the decorated function's
    name and module.

    Args:
        parent_class (type[DecoratedNode]): The base class for the new node class.
        io_static_method: The static method on the :param:`parent_class` which will
            store the io-defining function the resulting decorator will decorate.
            :param:`parent_class` must override :meth:`_io_defining_function` inherited
            from :class:`DecoratedNode` to return this method. This allows
            :param:`parent_class` classes to have unique names for their io-defining
            functions.
        decorator_docstring_additions (str): Any extra text to add between the main
            body of the docstring and the arguments.
        **parent_class_attr_overrides: Any additional attributes to pass to the new,
            dynamically created class created by the resulting decorator.

    Returns:
        (callable): A decorator that takes creates a new subclass of
            :param:`parent_class` that uses the wrapped function as the return value of
            :meth:`_io_defining_function` for the :class:`DecoratedNode` mixin.
    """
    if getattr(parent_class, io_static_method.__name__) is not io_static_method:
        raise ValueError(
            f"{io_static_method.__name__} is not a method on {parent_class}"
        )
    if not isinstance(io_static_method, FunctionType):
        raise TypeError(f"{io_static_method.__name__} should be a static method")

    def as_decorated_node_decorator(
        *output_labels: str,
        validate_output_labels: bool = True,
    ):
        output_labels = None if len(output_labels) == 0 else output_labels

        def as_decorated_node(io_defining_function: callable):
            if not callable(io_defining_function):
                raise AttributeError(
                    f"Tried to create a new child class of {parent_class.__name__}, "
                    f"but got {io_defining_function} instead of a callable."
                )

            decorated_node_class = type(
                io_defining_function.__name__,
                (parent_class,),  # Define parentage
                {
                    io_static_method.__name__: staticmethod(io_defining_function),
                    "__module__": io_defining_function.__module__,
                    "_output_labels": output_labels,
                    "_validate_output_labels": validate_output_labels,
                    **parent_class_attr_overrides,
                },
            )
            decorated_node_class.preview_io()  # Construct everything
            return decorated_node_class

        return as_decorated_node

    as_decorated_node_decorator.__doc__ = dedent(
        f"""
        A decorator for dynamically creating `{parent_class.__name__}` sub-classes by 
        wrapping a function as the `{io_static_method.__name__}`.
        
        The returned subclass uses the wrapped function (and optionally any provided 
        :param:`output_labels`) to specify its IO.
        
        {decorator_docstring_additions}
        
        Args:
            *output_labels (str): A name for each return value of the graph creating
                function. When empty, scrapes output labels automatically from the
                source code of the wrapped function. This can be useful when returned
                values are not well named, e.g. to make the output channel 
                dot-accessible if it would otherwise have a label that requires 
                item-string-based access. Additionally, specifying a _single_ label for 
                a wrapped function that returns a tuple of values ensures that a 
                _single_ output channel (holding the tuple) is created, instead of one 
                channel for each return value. The default approach of extracting 
                labels from the function source code also requires that the function 
                body contain _at most_ one `return` expression, so providing explicit 
                labels can be used to circumvent this (at your own risk). (Default is 
                empty, try to scrape labels from the source code of the wrapped 
                function.)
            validate_output_labels (bool): Whether to compare the provided output labels
                (if any) against the source code (if available). (Default is True.)
                
        Returns:
            (callable[[callable], type[{parent_class.__name__}]]): A decorator that 
                transforms a function into a child class of `{parent_class.__name__}` 
                using the decorated function as 
                `{parent_class.__name__}.{io_static_method.__name__}`.
        """
    )
    return as_decorated_node_decorator
