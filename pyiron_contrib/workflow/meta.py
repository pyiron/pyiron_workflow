"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from pyiron_contrib.workflow.function import (
    Function, SingleValue, function_node, single_value_node
)
from pyiron_contrib.workflow.macro import Macro, macro_node
from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.util import DotDict


def list_to_output(length: int, **node_class_kwargs) -> type[Function]:
    """
    A meta-node that returns a node class with `length` input channels and
    maps these to a single output channel with type `list`.
    """

    def _list_to_many(length: int):
        template = f"""
def __list_to_many(l: list):
    {"; ".join([f"out{i} = l[{i}]" for i in range(length)])}
    return [{", ".join([f"out{i}" for i in range(length)])}]
        """
        exec(template)
        return locals()["__list_to_many"]

    return function_node(**node_class_kwargs)(_list_to_many(length=length))


def input_to_list(length: int, **node_class_kwargs) -> type[SingleValue]:
    """
    A meta-node that returns a node class with `length` output channels and
    maps an input list to these.
    """

    def _many_to_list(length: int):
        template = f"""
def __many_to_list({", ".join([f"inp{i}=None" for i in range(length)])}):
    return [{", ".join([f"inp{i}" for i in range(length)])}]
        """
        exec(template)
        return locals()["__many_to_list"]

    return single_value_node(**node_class_kwargs)(_many_to_list(length=length))


def for_loop(
        node_class: type[Node],
        length: int,
        iterate_on: str | tuple[str] | list[str],
        # TODO:
) -> type[Macro]:
    """
    An _extremely rough_ first draft of a for-loop meta-node.

    Takes a node class, how long the loop should be, and which input(s) of the provided
    node class should be looped over (given as strings of the channel labels) and
    builds a macro that
    - Makes copies of the provided node class, i.e. the "body node"
    - For each input channel specified to "loop over", creates a list-to-many node and
      connects each of its outputs to their respective body node inputs
    - For all other inputs, makes a 1:1 node and connects its output to _all_ of the
      body nodes
    - Relables the macro IO to match the passed node class IO so that list-ified IO
      (i.e. the specified input and all output) is all caps

    Examples:
        >>> bulk_loop = for_loop(
        ...     Workflow.create.atomistics.Bulk,
        ...     5,
        ...     iterate_on = ("a",),
        ... )()
        >>>
        >>> [
        ...     struct.cell.volume for struct in bulk_loop(
        ...         name="Al",  # Sent equally to each body node
        ...         A=np.linspace(3.9, 4.1, 5).tolist(),  # Distributed across body nodes
        ...     ).STRUCTURE
        ... ]
        [14.829749999999995,
         15.407468749999998,
         15.999999999999998,
         16.60753125,
         17.230249999999995]

    TODO:
        - Refactor like crazy, it's super hard to read and some stuff is too hard-coded
        - Give some sort of access to flow control??
        - How to handle passing executors to the children? Maybe this is more
          generically a Macro question?
        - Is it possible to somehow dynamically adapt the held graph depending on the
          length of the input values being iterated over? Tricky to keep IO well defined
        - Allow a different mode, or make a different meta node, that makes all possible
          pairs of body nodes given the input being looped over instead of just `length`
        - Provide enter and exit magic methods so we can `for` or `with` this fancy-like
    """
    iterate_on = [iterate_on] if isinstance(iterate_on, str) else iterate_on

    def make_loop(macro):
        macro.inputs_map = {}
        macro.outputs_map = {}
        body_nodes = []

        # Parallelize over body nodes
        for n in range(length):
            body_nodes.append(
                macro.add(node_class(label=f"{node_class.__name__}_{n}"))
            )

        # Make input interface
        for inp in body_nodes[0].inputs:
            # Scatter a list of inputs to each node separately
            if inp.label in iterate_on:
                interface = list_to_output(length)(
                    parent=macro,
                    label=inp.label.upper(),
                    output_labels=[f"{node_class.__name__}__{inp.label}_{i}" for i in
                                   range(length)],
                    l=[inp.default] * length
                )
                # Connect each body node input to the input interface's respective output
                for body_node, out in zip(body_nodes, interface.outputs):
                    body_node.inputs[inp.label] = out
                macro.inputs_map[f"{interface.label}__l"] = interface.label
                # TODO: Don't hardcode __l
            # Or distribute the same input to each node equally
            else:
                interface = macro.create.standard.UserInput(
                    label=inp.label,
                    output_labels=inp.label,
                    user_input=inp.default
                )
                for body_node in body_nodes:
                    body_node.inputs[inp.label] = interface
                macro.inputs_map[f"{interface.label}__user_input"] = interface.label
                # TODO: Don't hardcode __user_input

        # Make output interface: outputs to lists
        for out in body_nodes[0].outputs:
            interface = input_to_list(length)(
                parent=macro,
                label=out.label.upper(),
                output_labels=f"{node_class.__name__}__{out.label}"
            )
            # Connect each body node output to the output interface's respective input
            for body_node, inp in zip(body_nodes, interface.inputs):
                inp.connect(body_node.outputs[out.label])
            macro.outputs_map[
                f"{interface.label}__{node_class.__name__}__{out.label}"] = interface.label
            # TODO: Don't manually copy the output label construction

    return macro_node()(make_loop)


meta_nodes = DotDict({
    for_loop.__name__: for_loop,
    input_to_list.__name__: input_to_list,
    list_to_output.__name__: list_to_output,
})

