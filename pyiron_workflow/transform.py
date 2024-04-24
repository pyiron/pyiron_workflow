"""
Transformer nodes convert many inputs into a single output, or vice-versa.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Any

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.io_preview import StaticNode, builds_class_io
from pyiron_workflow.snippets.constructed import Constructed
from pyiron_workflow.snippets.singleton import registered_factory


class Transformer(Constructed, StaticNode, ABC):
    """
    Transformers are a special :class:`Constructed` case of :class:`StaticNode` nodes
    that turn many inputs into a single output or vice-versa.
    """

    def to_dict(self):
        pass  # Vestigial abstract method


class FromManyInputs(Transformer, ABC):

    def __init_subclass__(
        cls,
        /,
        output_name: str = "data",
        output_type_hint: Optional[Any] = None,
        **kwargs,
    ):
        super(FromManyInputs, cls).__init_subclass__(**kwargs)
        cls._output_name = output_name
        cls._output_type_hint = output_type_hint

    @staticmethod
    @abstractmethod
    def transform_from_input(inputs_as_dict: dict):
        pass

    # _build_inputs_preview required from parent class
    # This must be commensurate with the internal expectations of transform_from_input

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


class ToManyOutputs(Transformer, ABC):
    def __init_subclass__(
        cls,
        /,
        input_name: str = "data",
        input_type_hint: Optional[Any] = None,
        input_default: Any | NOT_DATA = NOT_DATA,
        **kwargs,
    ):
        super(ToManyOutputs, cls).__init_subclass__(**kwargs)
        cls._input_name = input_name
        cls._input_type_hint = input_type_hint
        cls._input_default = input_default

    @staticmethod
    @abstractmethod
    def transform_to_output(input_data) -> dict[str, Any]:
        pass

    # _build_outputs_preview still required from parent class
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
    def __init_subclass__(
        cls,
        /,
        length: int = None,
        **kwargs,
    ):
        super(ListTransformer, cls).__init_subclass__(**kwargs)
        cls._length = length


@builds_class_io
def _list_transformer_factory(base_class, n):
    return type(
        f"{base_class.__name__}{n}",
        (base_class,),
        {},
        length=n,
        class_factory=_list_transformer_factory,
        class_factory_args=(base_class, n),
        class_factory_kwargs={}
    )


class InputsToList(
    ListTransformer,
    FromManyInputs,
    ABC,
    output_name="list",
    output_type_hint=list
):

    @staticmethod
    def transform_from_input(inputs_as_dict: dict):
        return list(inputs_as_dict.values())

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {
            f"item_{i}": (None, NOT_DATA)
            for i in range(cls._length)
        }


class ListToOutputs(
    ListTransformer, ToManyOutputs, ABC,
    input_name="list", input_type_hint=list
):
    @staticmethod
    def transform_to_output(input_data: list):
        return {f"item_{i}": v for i, v in enumerate(input_data)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {f"item_{i}": None for i in range(cls._length)}


@registered_factory
def inputs_to_list_factory(n, /) -> type[InputsToList]:
    return _list_transformer_factory(InputsToList, n)


def inputs_to_list(n, *args, **kwargs):
    return inputs_to_list_factory(n)(*args, **kwargs)


@registered_factory
def list_to_outputs_factory(n, /) -> type[ListToOutputs]:
    return _list_transformer_factory(ListToOutputs, n)


def list_to_outputs(n, /, *args, **kwargs) -> ListToOutputs:
    return list_to_outputs_factory(n)(*args, **kwargs)
