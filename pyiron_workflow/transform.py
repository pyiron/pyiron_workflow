"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import itertools
from typing import Any, ClassVar, Optional

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.io_preview import StaticNode, builds_class_io
from pyiron_workflow.snippets.factory import classfactory


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
        return (self.inputs[self._input_name].value, ), {}

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
def inputs_to_list_factory(n: int, /) -> type[InputsToList]:
    return (
        f"{InputsToList.__name__}{n}",
        (InputsToList,),
        {"_length": n},
        {},
    )


def inputs_to_list(n: int, *node_args, **node_kwargs):
    return inputs_to_list_factory(n)(*node_args, **node_kwargs)


@builds_class_io
@classfactory
def list_to_outputs_factory(n: int, /) -> type[ListToOutputs]:
    return (
        f"{ListToOutputs.__name__}{n}",
        (ListToOutputs,),
        {"_length": n},
        {},
    )


def list_to_outputs(n: int, /, *node_args, **node_kwargs) -> ListToOutputs:
    return list_to_outputs_factory(n)(*node_args, **node_kwargs)


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
    /
) -> type[InputsToDict]:
    if class_name_suffix is None:
        class_name_suffix = str(
            InputsToDict.hash_specification(input_specification)
        ).replace("-", "m")
    return (
        f"{InputsToDict.__name__}{class_name_suffix}",
        (InputsToDict,),
        {"_input_specification": input_specification},
        {},
    )


def inputs_to_dict(
    input_specification: list[str] | dict[str, tuple[Any | None, Any | NOT_DATA]],
    *node_args,
    class_name_suffix: Optional[str] = None,
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
        **node_kwargs: Other kwargs for the node instance.

    Returns:
        (InputsToDict): A new node for transforming inputs into a dictionary.
    """
    cls = inputs_to_dict_factory(input_specification, class_name_suffix)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)
