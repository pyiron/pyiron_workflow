"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import is_dataclass, MISSING
import itertools
from typing import Any, ClassVar, Optional

from pandas import DataFrame
from pyiron_snippets.factory import classfactory

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.mixin.preview import builds_class_io
from pyiron_workflow.nodes.static_io import StaticNode


class Transformer(StaticNode, ABC):
    """
    Transformers are a special :class:`Constructed` case of :class:`StaticNode` nodes
    that turn many inputs into a single output or vice-versa.
    """

    def to_dict(self):
        pass  # Vestigial abstract method


class FromManyInputs(Transformer, ABC):
    _output_name: ClassVar[str]  # Mandatory attribute for non-abstract subclasses
    _output_type_hint: ClassVar[Any] = None

    # _build_inputs_preview required from parent class
    # Inputs convert to `run_args` as a value dictionary
    # This must be commensurate with the internal expectations of on_run

    @abstractmethod
    def on_run(self, **inputs_to_value_dict) -> Any:
        """Must take inputs kwargs"""

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), self.inputs.to_value_dict()

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {cls._output_name: cls._output_type_hint}

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs[self._output_name].value = run_output
        return run_output


class ToManyOutputs(Transformer, ABC):
    _input_name: ClassVar[str]  # Mandatory attribute for non-abstract subclasses
    _input_type_hint: ClassVar[Any] = None
    _input_default: ClassVar[Any | NOT_DATA] = NOT_DATA

    # _build_outputs_preview still required from parent class
    # Must be commensurate with the dictionary returned by transform_to_output

    @abstractmethod
    def on_run(self, input_object) -> callable[..., Any | tuple]:
        """Must take the single object to be transformed"""

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (self.inputs[self._input_name].value,), {}

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {cls._input_name: (cls._input_type_hint, cls._input_default)}

    def process_run_result(self, run_output: dict[str, Any]) -> dict[str, Any]:
        for k, v in run_output.items():
            self.outputs[k].value = v
        return run_output


class _HasLength(Transformer, ABC):
    _length: ClassVar[int]  # Mandatory attribute for non-abstract subclasses


class InputsToList(_HasLength, FromManyInputs, ABC):
    _output_name: ClassVar[str] = "list"
    _output_type_hint: ClassVar[Any] = list

    def on_run(self, **inputs_to_value_dict):
        return list(inputs_to_value_dict.values())

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {f"item_{i}": (None, NOT_DATA) for i in range(cls._length)}


class ListToOutputs(_HasLength, ToManyOutputs, ABC):
    _input_name: ClassVar[str] = "list"
    _input_type_hint: ClassVar[Any] = list

    def on_run(self, input_object: list):
        return {f"item_{i}": v for i, v in enumerate(input_object)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {f"item_{i}": None for i in range(cls._length)}


@builds_class_io
@classfactory
def inputs_to_list_factory(n: int, use_cache: bool = True, /) -> type[InputsToList]:
    return (
        f"{InputsToList.__name__}{n}",
        (InputsToList,),
        {
            "_length": n,
            "use_cache": use_cache,
        },
        {},
    )


def inputs_to_list(n: int, /, *node_args, use_cache: bool = True, **node_kwargs):
    """
    Creates and returns an instance of a dynamically generated :class:`InputsToList`
        subclass with a specified number of inputs.

    Args:
        n (int): Number of input channels.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        *node_args: Positional arguments for the node instance.
        **node_kwargs: Keyword arguments for the node instance.

    Returns:
        InputsToList: An instance of the dynamically created :class:`InputsToList`
            subclass.
    """
    return inputs_to_list_factory(n, use_cache)(*node_args, **node_kwargs)


@builds_class_io
@classfactory
def list_to_outputs_factory(n: int, use_cache: bool = True, /) -> type[ListToOutputs]:
    return (
        f"{ListToOutputs.__name__}{n}",
        (ListToOutputs,),
        {
            "_length": n,
            "use_cache": use_cache,
        },
        {},
    )


def list_to_outputs(
    n: int, /, *node_args, use_cache: bool = True, **node_kwargs
) -> ListToOutputs:
    """
    Creates and returns an instance of a dynamically generated :class:`ListToOutputs`
    subclass with a specified number of outputs.

    Args:
        n (int): Number of output channels.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        *node_args: Positional arguments for the node instance.
        **node_kwargs: Keyword arguments for the node instance.

    Returns:
        ListToOutputs: An instance of the dynamically created :class:`ListToOutputs`
            subclass.
    """
    return list_to_outputs_factory(n, use_cache)(*node_args, **node_kwargs)


class InputsToDict(FromManyInputs, ABC):
    _output_name: ClassVar[str] = "dict"
    _output_type_hint: ClassVar[Any] = dict
    _input_specification: ClassVar[
        list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]]
    ]

    def on_run(self, **inputs_to_value_dict):
        return inputs_to_value_dict

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any | None, Any | NOT_DATA]]:
        if isinstance(cls._input_specification, list):
            return {key: (None, NOT_DATA) for key in cls._input_specification}
        else:
            return cls._input_specification

    @staticmethod
    def hash_specification(
        input_specification: list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]]
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
    input_specification: list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]],
    class_name_suffix: str | None,
    use_cache: bool = True,
    /,
) -> type[InputsToDict]:
    if class_name_suffix is None:
        class_name_suffix = str(
            InputsToDict.hash_specification(input_specification)
        ).replace("-", "m")
    return (
        f"{InputsToDict.__name__}{class_name_suffix}",
        (InputsToDict,),
        {
            "_input_specification": input_specification,
            "use_cache": use_cache,
        },
        {},
    )


def inputs_to_dict(
    input_specification: list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]],
    *node_args,
    class_name_suffix: Optional[str] = None,
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


class InputsToDataframe(_HasLength, FromManyInputs, ABC):
    """
    Turns inputs of dictionaries (all with the same keys) into a single
    :class:`pandas.DataFrame`.
    """

    _output_name: ClassVar[str] = "df"
    _output_type_hint: ClassVar[Any] = DataFrame

    def on_run(self, *rows: dict[str, Any]) -> Any:
        df_dict = {}
        for i, row in enumerate(rows):
            for key, value in row.items():
                if i == 0:
                    df_dict[key] = [value]
                else:
                    df_dict[key].append(value)
        return DataFrame(df_dict)

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return tuple(self.inputs.to_value_dict().values()), {}

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {f"row_{i}": (dict, NOT_DATA) for i in range(cls._length)}


@classfactory
def inputs_to_dataframe_factory(
    n: int, use_cache: bool = True, /
) -> type[InputsToDataframe]:
    return (
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


class DataclassNode(FromManyInputs, ABC):
    """
    A base class for a node that converts inputs into a dataclass instance.
    """

    dataclass: ClassVar[type]  # Mandatory in children, must pass `is_dataclass`
    _output_name: ClassVar[str] = "dataclass"

    @classmethod
    @property
    def _dataclass_fields(cls):
        return cls.dataclass.__dataclass_fields__

    def _setup_node(self) -> None:
        super()._setup_node()
        # Then leverage default factories from the dataclass
        for name, channel in self.inputs.items():
            if (
                channel.value is NOT_DATA
                and self._dataclass_fields[name].default_factory is not MISSING
            ):
                self.inputs[name] = self._dataclass_fields[name].default_factory()

    def on_run(self, **inputs_to_value_dict):
        return self.dataclass(**inputs_to_value_dict)

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), self.inputs.to_value_dict()

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        # Make a channel for each field
        return {
            name: (f.type, NOT_DATA if f.default is MISSING else f.default)
            for name, f in cls._dataclass_fields.items()
        }


@classfactory
def dataclass_node_factory(
    dataclass: type, use_cache: bool = True, /
) -> type[DataclassNode]:
    if not is_dataclass(dataclass):
        raise TypeError(
            f"{DataclassNode} expected to get a dataclass but {dataclass} failed "
            f"`dataclasses.is_dataclass`."
        )
    if type(dataclass) is not type:
        raise TypeError(
            f"{DataclassNode} expected to get a dataclass but {dataclass} is not "
            f"type `type`."
        )
    return (
        f"{DataclassNode.__name__}{dataclass.__name__}",
        (DataclassNode,),
        {
            "dataclass": dataclass,
            "_output_type_hint": dataclass,
            "__doc__": dataclass.__doc__,
            "use_cache": use_cache,
        },
        {},
    )


def as_dataclass_node(dataclass: type, use_cache: bool = True):
    """
    Decorates a dataclass as a dataclass node -- i.e. a node whose inputs correspond
    to dataclass fields and whose output is an instance of the dataclass.

    The underlying dataclass can be accessed on the :attr:`.dataclass` class attribute
    of the resulting node class.

    Leverages defaults (default factories) on dataclass fields to populate input
    channel values at class defintion (instantiation).

    Args:
        dataclass (type): A dataclass, i.e. class passing `dataclasses.is_dataclass`.
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
        ... @dataclass
        ... class Foo:
        ...     necessary: str
        ...     bar: str = "bar"
        ...     answer: int = 42
        ...     complex_: list = field(default_factory=some_list)
        >>>
        >>> f = Foo()
        >>> print(f.readiness_report)
        DataclassNodeFoo readiness: False
        STATE:
        running: False
        failed: False
        INPUTS:
        necessary ready: False
        bar ready: True
        answer ready: True
        complex_ ready: True

        >>> f(necessary="input as a node kwarg")
        Foo(necessary='input as a node kwarg', bar='bar', answer=42, complex_=[1, 2, 3])
    """
    cls = dataclass_node_factory(dataclass, use_cache)
    cls.preview_io()
    return cls


def dataclass_node(dataclass: type, use_cache: bool = True, *node_args, **node_kwargs):
    """
    Builds a dataclass node from a dataclass -- i.e. a node whose inputs correspond
    to dataclass fields and whose output is an instance of the dataclass.

    The underlying dataclass can be accessed on the :attr:`.dataclass` class attribute
    of the resulting node.

    Leverages defaults (default factories) on dataclass fields to populate input
    channel values at class defintion (instantiation).

    Args:
        dataclass (type): A dataclass, i.e. class passing `dataclasses.is_dataclass`.
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
        >>> @dataclass
        ... class Foo:
        ...     necessary: str
        ...     bar: str = "bar"
        ...     answer: int = 42
        ...     complex_: list = field(default_factory=some_list)
        >>>
        >>> f = Workflow.create.transformer.dataclass_node(Foo, label="my_dc")
        >>> print(f.readiness_report)
        my_dc readiness: False
        STATE:
        running: False
        failed: False
        INPUTS:
        necessary ready: False
        bar ready: True
        answer ready: True
        complex_ ready: True

        >>> f(necessary="input as a node kwarg")
        Foo(necessary='input as a node kwarg', bar='bar', answer=42, complex_=[1, 2, 3])
    """
    cls = dataclass_node_factory(dataclass)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)
