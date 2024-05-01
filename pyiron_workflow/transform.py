"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

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

    @staticmethod
    @abstractmethod
    def transform_from_input(inputs_as_dict: dict):
        pass

    # _build_inputs_preview required from parent class
    # This must be commensurate with the internal expectations of transform_from_input

    def on_run(self, **kwargs) -> callable[..., Any | tuple]:
        return self.transform_from_input(**kwargs)

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), {"inputs_as_dict": self.inputs.to_value_dict()}

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

    @staticmethod
    @abstractmethod
    def transform_to_output(input_data) -> dict[str, Any]:
        pass

    # _build_outputs_preview still required from parent class
    # Must be commensurate with the dictionary returned by transform_to_output

    def on_run(self, **kwargs) -> callable[..., Any | tuple]:
        return self.transform_to_output(**kwargs)

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), {
            "input_data": self.inputs[self._input_name].value,
        }

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

    @staticmethod
    def transform_from_input(inputs_as_dict: dict):
        return list(inputs_as_dict.values())

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {f"item_{i}": (None, NOT_DATA) for i in range(cls._length)}


class ListToOutputs(_HasLength, ToManyOutputs, ABC):
    _input_name: ClassVar[str] = "list"
    _input_type_hint: ClassVar[Any] = list

    @staticmethod
    def transform_to_output(input_data: list):
        return {f"item_{i}": v for i, v in enumerate(input_data)}

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
