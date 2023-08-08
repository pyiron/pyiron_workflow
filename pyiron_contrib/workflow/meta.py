"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from pyiron_contrib.workflow.function import single_value_node, SingleValue


def _input_to_list(n_elements) -> callable:
    string = "def input_to_list("
    for i in range(n_elements):
        string += f"i{i}=None, "
    string += "): return ["
    for i in range(n_elements):
        string += f"i{i}, "
    string += "]"
    exec(string)
    return locals()["input_to_list"]


class MetaNodes:
    """A container class for meta node access"""

    @classmethod
    def input_to_list(cls, n_inputs: int) -> SingleValue:
        return single_value_node("list")(_input_to_list(n_inputs))
