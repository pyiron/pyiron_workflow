"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Any

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.meta import Meta, meta_node_class_factory, \
    meta_node_instance_factory


class Transformer(Meta, ABC):
    """
    Transformers are a special case of :class:`Meta` nodes that turn many inputs into
    a single output or vice-versa.
    """


class FromManyInputs(Transformer, ABC):
    _output_name: str = "data"
    _output_type_hint: Optional[Any] = None

    @staticmethod
    @abstractmethod
    def transform_from_input(inputs_as_dict: dict):
        pass

    # _build_inputs_preview required
    # Must be commensurate with the internal expectations of transform_from

    @property
    def on_run(self) -> callable[..., Any | tuple]:
        return self.transform_from_input

    @property
    def run_args(self) -> dict:
        return {"inputs_as_dict": self.inputs.to_value_dict()}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {cls._output_name: cls._output_type_hint}

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs[self._output_name].value = run_output
        return run_output


class ToManyOutputs(Transformer):
    _input_name: str = "data"
    _input_type_hint: Optional[Any] = None
    _input_default: Any | NOT_DATA = NOT_DATA

    @staticmethod
    @abstractmethod
    def transform_to_output(input_data) -> dict[str, Any]:
        pass

    # _build_outputs_preview still required
    # Must be commensurate with the dictionary returned by transform_to_output

    @property
    def on_run(self) -> callable[..., Any | tuple]:
        return self.transform_to_output

    @property
    def run_args(self) -> dict:
        return {
            "input_data": self.inputs[self._input_name].value,
        }

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {cls._input_name: (cls._input_type_hint, cls._input_default)}

    def process_run_result(self, run_output: dict[str, Any]) -> dict[str, Any]:
        for k, v in run_output.items():
            self.outputs[k].value = v
        return run_output


class ListTransformer(Transformer, ABC):
    _length: int = None  # Mandatory

    @property
    def _instance_constructor_args(self) -> tuple:
        return self._length,


def to_list_node_class(length: int, class_name: Optional[str] = None):
    return meta_node_class_factory(
        InputsToList,
        {"_length": length},
        class_name=class_name,
    )


def to_list_node(length, *node_args, class_name: Optional[str] = None, **node_kwargs):
    return meta_node_instance_factory(
        InputsToList,
        {"_length": length},
        *node_args,
        class_name=class_name,
        **node_kwargs
    )


def _to_list_node_constructor(label, class_name, length):
    return to_list_node(length, class_name=class_name, label=label)


def from_list_node_class(length: int, class_name: Optional[str] = None):
    return meta_node_class_factory(
        ListToOutputs,
        {"_length": length},
        class_name=class_name,
    )


def from_list_node(length, *node_args, class_name: Optional[str] = None, **node_kwargs):
    return meta_node_instance_factory(
        ListToOutputs,
        {"_length": length},
        *node_args,
        class_name=class_name,
        **node_kwargs
    )


def _from_list_node_constructor(label, class_name, length):
    return from_list_node(length, class_name=class_name, label=label)


class InputsToList(ListTransformer, FromManyInputs):
    # _instance_constructor = staticmethod(_to_list_node_constructor)
    _output_name = "list"
    _output_type_hint = list

    @property
    def _instance_constructor(self) -> callable[[...], Meta]:
        return _to_list_node_constructor

    @staticmethod
    def transform_from_input(inputs_as_dict: dict):
        return list(inputs_as_dict.values())

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {
            f"item_{i}": (None, NOT_DATA)
            for i in range(cls._length)
        }


class ListToOutputs(ListTransformer, ToManyOutputs, ABC):
    # _instance_constructor = staticmethod(_from_list_node_constructor)
    _input_name = "list"
    _input_type_hint = list

    @property
    def _instance_constructor(self) -> callable[[...], Meta]:
        return _from_list_node_constructor

    @staticmethod
    def transform_to_output(input_data: list):
        return {f"item_{i}": v for i, v in enumerate(input_data)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {f"item_{i}": None for i in range(cls._length)}
