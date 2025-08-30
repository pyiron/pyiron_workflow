"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""

from __future__ import annotations

import itertools
from abc import ABC
from dataclasses import MISSING
from dataclasses import dataclass as as_dataclass
from typing import Any, ClassVar

from pandas import DataFrame
from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.factory import classfactory

from pyiron_workflow import identifier
from pyiron_workflow.data import NOT_DATA, NotData
from pyiron_workflow.nodes.multiple_distpatch import dispatch_u_kwargs
from pyiron_workflow.nodes.static_io import StaticNode


class Transformer(StaticNode, ABC):
    """
    Transformers are a special case of :class:`StaticNode` nodes that turn many inputs
    into a single output or vice-versa.
    """

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.blue


class InputsToList(Transformer, ABC):
    _length: ClassVar[int]  # Mandatory attribute for non-abstract subclasses
    _content_type_hint: ClassVar[object]
    _output_name: ClassVar[str] = "list"

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {
            cls._output_name: (
                list
                if cls._content_type_hint is None
                else list[cls._content_type_hint]  # type: ignore[name-defined]
            )
        }

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {
            f"item_{i}": (cls._content_type_hint, NOT_DATA) for i in range(cls._length)
        }

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), self.inputs.to_value_dict()

    def _on_run(self, **inputs_to_value_dict):
        return list(inputs_to_value_dict.values())

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs[self._output_name].value = run_output
        return run_output


@classfactory
def inputs_to_list_factory(
    n: int, class_name: str, use_cache: bool = True, content_type_hint: object = None, /
) -> type[InputsToList]:
    return (  # type: ignore[return-value]
        class_name,
        (InputsToList,),
        {
            "_length": n,
            "_content_type_hint": content_type_hint,
            "use_cache": use_cache,
        },
        {},
    )


def inputs_to_list(
    n: int,
    /,
    *node_args,
    use_cache: bool = True,
    content_type_hint: object = NOT_DATA,
    **node_kwargs,
):
    """
    Creates and returns an instance of a dynamically generated :class:`InputsToList`
        subclass with a specified number of inputs.

    Args:
        n (int): Number of input channels.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        content_type_hint (object): Type hint for the content of the list. Applies to
            all input channels and the content type of the returned list. (Default is
            NOT_DATA, which will leave the content type of the list unspecified.)
        *node_args: Positional arguments for the node instance.
        **node_kwargs: Keyword arguments for the node instance.

    Returns:
        InputsToList: An instance of the dynamically created :class:`InputsToList`
            subclass.
    """
    hint_identifier = (
        ""
        if content_type_hint is NOT_DATA
        else identifier.to_identifier(str(content_type_hint))
    )
    class_name = f"{InputsToList.__name__}{n}{hint_identifier}"
    inputs_to_list_factory.clear(class_name)
    cls = inputs_to_list_factory(
        n, class_name, use_cache, _parse_type_hint(content_type_hint)
    )
    cls.preview_io()
    return cls(*node_args, **node_kwargs)


def _parse_type_hint(type_hint: object) -> object:
    if type_hint is NOT_DATA:
        return None
    elif type_hint is None:
        return type(None)
    return type_hint


class ListToOutputs(Transformer, ABC):
    _length: ClassVar[int]  # Mandatory attribute for non-abstract subclasses
    _content_type_hint: ClassVar[object]
    _input_name: ClassVar[str] = "list"

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {f"item_{i}": cls._content_type_hint for i in range(cls._length)}

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {
            cls._input_name: (
                (
                    list
                    if cls._content_type_hint is None
                    else list[cls._content_type_hint]  # type: ignore[name-defined]
                ),
                NOT_DATA,
            )
        }

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (self.inputs[self._input_name].value,), {}

    def _on_run(self, input_object: list):
        return {f"item_{i}": v for i, v in enumerate(input_object)}

    def process_run_result(self, run_output: dict[str, Any]) -> dict[str, Any]:
        for k, v in run_output.items():
            self.outputs[k].value = v
        return run_output


@classfactory
def list_to_outputs_factory(
    n: int, class_name: str, use_cache: bool = True, content_type_hint: object = None, /
) -> type[ListToOutputs]:
    return (  # type: ignore[return-value]
        class_name,
        (ListToOutputs,),
        {
            "_length": n,
            "_content_type_hint": content_type_hint,
            "use_cache": use_cache,
        },
        {},
    )


def list_to_outputs(
    n: int,
    /,
    *node_args,
    use_cache: bool = True,
    content_type_hint: object = NOT_DATA,
    **node_kwargs,
) -> ListToOutputs:
    """
    Creates and returns an instance of a dynamically generated :class:`ListToOutputs`
    subclass with a specified number of outputs.

    Args:
        n (int): Number of output channels.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        content_type_hint (object): Type hint for the content of the list. Applies to
            all output channels and the content type of the provided list. (Default is
            NOT_DATA, which will leave the content type of the list unspecified.)
        *node_args: Positional arguments for the node instance.
        **node_kwargs: Keyword arguments for the node instance.

    Returns:
        ListToOutputs: An instance of the dynamically created :class:`ListToOutputs`
            subclass.
    """
    hint_identifier = (
        ""
        if content_type_hint is NOT_DATA
        else identifier.to_identifier(str(content_type_hint))
    )
    class_name = f"{ListToOutputs.__name__}{n}{hint_identifier}"
    list_to_outputs_factory.clear(class_name)
    cls = list_to_outputs_factory(
        n, class_name, use_cache, _parse_type_hint(content_type_hint)
    )
    cls.preview_io()
    return cls(*node_args, **node_kwargs)


class InputsToDict(Transformer, ABC):
    _output_name: ClassVar[str] = "dict"
    _input_specification: ClassVar[
        list[str] | dict[str, tuple[Any | None, Any | NotData]]
    ]

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any | None, Any | NotData]]:
        if isinstance(cls._input_specification, list):
            return dict.fromkeys(cls._input_specification, (None, NOT_DATA))
        else:
            return cls._input_specification

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        if isinstance(cls._input_specification, list):
            type_hint = dict[str, Any]
        else:
            first_type = next(iter(cls._input_specification.values()))[0] or Any
            type_hint = dict[str, first_type]  # type: ignore[misc, valid-type]
        return {cls._output_name: type_hint}

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), self.inputs.to_value_dict()

    def _on_run(self, **inputs_to_value_dict):
        return inputs_to_value_dict

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs[self._output_name].value = run_output
        return run_output

    @staticmethod
    def hash_specification(
        input_specification: list[str] | dict[str, tuple[Any | None, Any | NotData]],
    ):
        """For generating unique subclass names."""

        if isinstance(input_specification, list):
            return hash(tuple(input_specification))
        else:
            flattened_tuple = tuple(
                itertools.chain.from_iterable(
                    (key, *value) for key, value in input_specification.items()
                )
            )
            try:
                return hash(flattened_tuple)
            except Exception as e:
                raise ValueError(
                    f"To automatically generate a unique name for subclasses of "
                    f"{InputsToDict.__name__}, the input specification must be fully "
                    f"hashable, but it was not. Either pass fully hashable hints and "
                    f"defaults, or explicitly provide a class name suffix. Received "
                    f"specification: {input_specification}"
                ) from e


@classfactory
def inputs_to_dict_factory(
    input_specification: list[str] | dict[str, tuple[Any | None, Any | NotData]],
    class_name_suffix: str | None,
    use_cache: bool = True,
    /,
) -> type[InputsToDict]:
    if class_name_suffix is None:
        class_name_suffix = str(
            InputsToDict.hash_specification(input_specification)
        ).replace("-", "m")
    return (  # type: ignore[return-value]
        f"{InputsToDict.__name__}{class_name_suffix}",
        (InputsToDict,),
        {
            "_input_specification": input_specification,
            "use_cache": use_cache,
        },
        {},
    )


def inputs_to_dict(
    input_specification: list[str] | dict[str, tuple[Any | None, Any | NotData]],
    *node_args,
    class_name_suffix: str | None = None,
    use_cache: bool = True,
    **node_kwargs,
):
    """
    Build a new :class:`InputsToDict` subclass and instantiate it.

    Tries to automatically generate a subclass name by hashing the
    :param:`input_specification`. If such hashing fails, you will instead _need_ to
    provide an explicit :param:`class_name_suffix`

    Args:
        input_specification (list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]]):
            The input channel names, or full input specification in the form
            `{key: (type_hint, default_value))}`.
        *node_args: Other args for the node instance.
        class_name_suffix (str | None): The suffix to use in the class name. (Default
            is None, try to generate the suffix by hashing :param:`input_specification`.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        **node_kwargs: Other kwargs for the node instance.

    Returns:
        (InputsToDict): A new node for transforming inputs into a dictionary.
    """
    cls = inputs_to_dict_factory(input_specification, class_name_suffix, use_cache)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)


class InputsToDataframe(Transformer, ABC):
    """
    Turns inputs of dictionaries (all with the same keys) into a single
    :class:`pandas.DataFrame`.
    """

    _length: ClassVar[int]  # Mandatory attribute for non-abstract subclasses
    _output_name: ClassVar[str] = "df"

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {f"row_{i}": (dict, NOT_DATA) for i in range(cls._length)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {cls._output_name: DataFrame}

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return tuple(self.inputs.to_value_dict().values()), {}

    def _on_run(self, *rows: dict[str, Any]) -> DataFrame:
        df_dict = {}
        for i, row in enumerate(rows):
            for key, value in row.items():
                if i == 0:
                    df_dict[key] = [value]
                else:
                    df_dict[key].append(value)
        return DataFrame(df_dict)

    def process_run_result(self, run_output: DataFrame) -> DataFrame:
        self.outputs[self._output_name].value = run_output
        return run_output


@classfactory
def inputs_to_dataframe_factory(
    n: int, use_cache: bool = True, /
) -> type[InputsToDataframe]:
    return (  # type: ignore[return-value]
        f"{InputsToDataframe.__name__}{n}",
        (InputsToDataframe,),
        {
            "_length": n,
            "use_cache": use_cache,
        },
        {},
    )


def inputs_to_dataframe(n: int, use_cache: bool = True, *node_args, **node_kwargs):
    """
    Creates and returns an instance of a dynamically generated
    :class:`InputsToDataframe` subclass with a specified number of inputs, each being a
    dictionary to form rows of the dataframe.

    Args:
        n (int): Number of input channels.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        *node_args: Positional arguments for the node instance.
        **node_kwargs: Keyword arguments for the node instance.

    Returns:
        InputsToDataframe: An instance of the dynamically created
            :class:`InputsToDataframe` subclass.
    """
    cls = inputs_to_dataframe_factory(n, use_cache)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)


class DataclassNode(Transformer, ABC):
    """
    A base class for a node that converts inputs into a dataclass instance.
    """

    dataclass: ClassVar[type]  # Mandatory in children, must pass `is_dataclass`
    _output_name: ClassVar[str] = "dataclass"
    _semantikon_metadata: tuple[tuple[Any, ...], dict[str, Any]] | None = None

    @classmethod
    def _dataclass_fields(cls):
        return cls.dataclass.__dataclass_fields__

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        # Make a channel for each field
        return {
            name: (f.type, NOT_DATA if f.default is MISSING else f.default)
            for name, f in cls._dataclass_fields().items()
        }

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        if cls._semantikon_metadata is not None:
            from semantikon.metadata import u  # noqa: I001 PLC0415

            # The semantikon import can be expensive, so delay it if possible

            return {cls._output_name: u(cls.dataclass, **cls._semantikon_metadata)}
        return {cls._output_name: cls.dataclass}

    def _setup_node(self) -> None:
        super()._setup_node()
        # Then leverage default factories from the dataclass
        for name, channel in self.inputs.items():
            if (
                channel.value is NOT_DATA
                and self._dataclass_fields()[name].default_factory is not MISSING
            ):
                self.inputs[name] = self._dataclass_fields()[name].default_factory()

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), self.inputs.to_value_dict()

    def _on_run(self, **inputs_to_value_dict):
        return self.dataclass(**inputs_to_value_dict)

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs[self._output_name].value = run_output
        return run_output

    @classmethod
    def _extra_info(cls) -> str:
        return cls.dataclass.__doc__ or ""


@classfactory
def dataclass_node_factory(
    dataclass: type,
    semantikon_metadata: dict[str, Any] | None = None,
    use_cache: bool = True,
    /,
) -> type[DataclassNode]:
    if not isinstance(dataclass, type):
        raise TypeError(
            f"{DataclassNode} expected to get a dataclass but {dataclass} is not "
            f"type `type`."
        )
    dataclass = as_dataclass(dataclass)
    # Classes inheriting from a dataclass will pass the `dataclasses.is_dataclass` test
    # BUT they won't themselves _act_ as dataclass definitions! I.e. if you introduce
    # new fields in a sub-dataclass, or update defaults, this won't register _unless_
    # that new class is _also_ wrapped as a @dataclasses.dataclass!
    # This is not our fault, it's just a python thing. But is our _problem_.
    # To make sure dataclass nodes inheriting from other dataclass nodes still act as
    # dataclasses, just cast everything as a dataclass -- re-casting an
    # already-dataclass is not harmful
    # Composition is preferable over inheritance, but we want inheritance to be possible
    module, qualname = dataclass.__module__, dataclass.__qualname__
    dataclass.__qualname__ += ".dataclass"  # So output type hints know where to find it
    return (  # type: ignore[return-value]
        dataclass.__name__,
        (DataclassNode,),
        {
            "dataclass": dataclass,
            "_semantikon_metadata": semantikon_metadata,
            "__module__": module,
            "__qualname__": qualname,
            "__doc__": dataclass.__doc__,
            "use_cache": use_cache,
        },
        {},
    )


@dispatch_u_kwargs
def as_dataclass_node(**u_kwargs):
    """
    Decorates a dataclass as a dataclass node -- i.e. a node whose inputs correspond
    to dataclass fields and whose output is an instance of the dataclass.

    The underlying dataclass can be accessed on the :attr:`.dataclass` class attribute
    of the resulting node class.

    Leverages defaults (default factories) on dataclass fields to populate input
    channel values at class defintion (instantiation).

    Args:
        dataclass (type): A dataclass, i.e. class passing `dataclasses.is_dataclass`,
            or class definition that will be automatically wrapped with
            `dataclasses.dataclass`.
        use_cache (bool): Whether nodes of this type should default to caching their
            values. (Default is True.)

    Returns:
        (type[DataclassNode]): A :class:`DataclassNode` subclass whose instances
            transform inputs to an instance of that dataclass.

    Examples:
        >>> from dataclasses import dataclass, field
        >>>
        >>> from pyiron_workflow import Workflow
        >>>
        >>> def some_list():
        ...     return [1, 2, 3]
        >>>
        >>> @Workflow.wrap.as_dataclass_node
        ... class Foo:
        ...     necessary: str
        ...     bar: str = "bar"
        ...     answer: int = 42
        ...     complex_: list = field(default_factory=some_list)
        >>>
        >>> f = Foo()
        >>> print(f.readiness_report)
        Foo readiness report:
        ready: False
        running: False
        failed: False
        inputs.necessary: False
        inputs.bar: True
        inputs.answer: True
        inputs.complex_: True
        <BLANKLINE>

        >>> f(necessary="input as a node kwarg")
        Foo.dataclass(necessary='input as a node kwarg', bar='bar', answer=42, complex_=[1, 2, 3])
    """

    def decorator(dataclass: type):
        dataclass_node_factory.clear(dataclass.__name__)  # Force a fresh class
        module, qualname = dataclass.__module__, dataclass.__qualname__
        cls = dataclass_node_factory(
            dataclass,
            u_kwargs if u_kwargs else None,
        )
        cls._reduce_imports_as = (module, qualname)
        cls.preview_io()
        return cls

    return decorator


def dataclass_node(dataclass: type, use_cache: bool = True, *node_args, **node_kwargs):
    """
    Builds a dataclass node from a dataclass -- i.e. a node whose inputs correspond
    to dataclass fields and whose output is an instance of the dataclass.

    The underlying dataclass can be accessed on the :attr:`.dataclass` class attribute
    of the resulting node.

    Leverages defaults (default factories) on dataclass fields to populate input
    channel values at class defintion (instantiation).

    Args:
        dataclass (type): A dataclass, i.e. class passing `dataclasses.is_dataclass`,
            or class variable that will be automatically passed to
            `dataclasses.dataclass`.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        *node_args: Other :class:`Node` positional arguments.
        **node_kwargs: Other :class:`Node` keyword arguments.

    Returns:
        (DataclassNode): An instance of the dynamically created :class:`DataclassNode`
            subclass.

    Examples:
        >>> from dataclasses import dataclass, field
        >>>
        >>> from pyiron_workflow import Workflow
        >>>
        >>> def some_list():
        ...     return [1, 2, 3]
        >>>
        >>> #@dataclass  # Works on actual dataclasses as well as dataclass-like classes
        >>> class Foo:
        ...     necessary: str
        ...     bar: str = "bar"
        ...     answer: int = 42
        ...     complex_: list = field(default_factory=some_list)
        >>>
        >>> f = Workflow.create.transformer.dataclass_node(Foo, label="my_dc")
        >>> print(f.readiness_report)
        my_dc readiness report:
        ready: False
        running: False
        failed: False
        inputs.necessary: False
        inputs.bar: True
        inputs.answer: True
        inputs.complex_: True
        <BLANKLINE>

        >>> f(necessary="input as a node kwarg")
        Foo.dataclass(necessary='input as a node kwarg', bar='bar', answer=42, complex_=[1, 2, 3])
    """
    cls = dataclass_node_factory(dataclass)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)
