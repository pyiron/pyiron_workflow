"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from pyiron_workflow.function import Function, as_function_node


def list_to_output(length: int, **node_class_kwargs) -> type[Function]:
    """
    A meta-node that returns a node class with :param:`length` input channels and
    maps these to a single output channel with type `list`.
    """

    def _list_to_many(length: int):
        template = f"""
def __list_to_many(input_list: list):
    {"; ".join([f"out{i} = input_list[{i}]" for i in range(length)])}
    return {", ".join([f"out{i}" for i in range(length)])}
        """
        exec(template)
        return locals()["__list_to_many"]

    return as_function_node(*(f"output{n}" for n in range(length)))(
        _list_to_many(length=length), **node_class_kwargs
    )


def input_to_list(length: int, **node_class_kwargs) -> type[Function]:
    """
    A meta-node that returns a node class with :param:`length` output channels and
    maps an input list to these.
    """

    def _many_to_list(length: int):
        template = f"""
def __many_to_list({", ".join([f"inp{i}=None" for i in range(length)])}):
    return [{", ".join([f"inp{i}" for i in range(length)])}]
        """
        exec(template)
        return locals()["__many_to_list"]

    return as_function_node("output_list")(
        _many_to_list(length=length), **node_class_kwargs
    )
