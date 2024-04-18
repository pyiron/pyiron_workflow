"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Optional

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.io_preview import StaticNode


class ToList(StaticNode, ABC):
    _length = None

    def to_dict(self):
        pass

    @staticmethod
    def node_operation(inputs_as_dict: dict):
        return list(inputs_as_dict.values())

    @property
    def on_run(self) -> callable[..., Any | tuple]:
        return self.node_operation

    @property
    def run_args(self) -> dict:
        return {"inputs_as_dict": self.inputs.to_value_dict()}

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        self.outputs.as_list.value = run_output
        return run_output

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {f"i_{i}": (None, NOT_DATA) for i in range(cls._length)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {"as_list": list}

    def __reduce__(self):
        return (
            meta_node_constructor,
            (
                to_list_node_class_factory,
                {"length": self._length, "class_name": self.__class__.__name__},
                {"label": self.label},
            ),
            self.__getstate__()
        )


def meta_node_constructor(class_factory_method, factory_kwargs, instance_kwargs):
    return class_factory_method(**factory_kwargs)(**instance_kwargs)


def to_list_node_class_factory(length: int, class_name: Optional[str] = None):
    return type(
        f"{ToList.__name__}{length}" if class_name is None else class_name,
        (ToList,),
        {"_length": length}
    )


def to_list_node(length, label, **node_kwargs):
    return to_list_node_class_factory(length)(label=label, **node_kwargs)


class FromList(StaticNode, ABC):
    _length = None

    def to_dict(self):
        pass

    @staticmethod
    def node_operation(input_list: list):
        return {
            f"i_{i}": v for i, v in enumerate(input_list)
        }

    @property
    def on_run(self) -> callable[..., Any | tuple]:
        return self.node_operation

    @property
    def run_args(self) -> dict:
        return {"input_list": self.inputs.as_list.value}

    def process_run_result(self, run_output: Any | tuple) -> Any | tuple:
        for k, v in run_output.items():
            self.outputs[k].value = v
        return run_output

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        return {"as_list": (list, NOT_DATA)}

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {f"i_{i}": None for i in range(cls._length)}

    def __reduce__(self):
        return (
            meta_node_constructor,
            (
                from_list_node_class_factory,
                {"length": self._length, "class_name": self.__class__.__name__},
                {"label": self.label},
            ),
            self.__getstate__()
        )


def from_list_node_class_factory(length: int, class_name: Optional[str] = None):
    return type(
        f"{FromList.__name__}{length}" if class_name is None else class_name,
        (FromList,),
        {"_length": length}
    )


def from_list_node(length, label, **node_kwargs):
    return from_list_node_class_factory(length)(label=label, **node_kwargs)
