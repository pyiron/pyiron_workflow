"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from typing import Optional

from pyiron_workflow.function import (
    Function,
    SingleValue,
    function_node,
    single_value_node,
)
from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node import Node
from pyiron_workflow.util import DotDict


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
    loop_body_class: type[Node],
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
        >>> import numpy as np
        >>> from pyiron_workflow import Workflow
        >>>
        >>> bulk_loop = Workflow.create.meta.for_loop(
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
                macro.add(loop_body_class(label=f"{loop_body_class.__name__}_{n}"))
            )

        # Make input interface
        for label, inp in body_nodes[0].inputs.items():
            # Don't rely on inp.label directly, since inputs may be a Composite IO
            # panel that has a different key for this input channel than its label

            # Scatter a list of inputs to each node separately
            if label in iterate_on:
                interface = list_to_output(length)(
                    parent=macro,
                    label=label.upper(),
                    output_labels=[
                        f"{loop_body_class.__name__}__{inp.label}_{i}"
                        for i in range(length)
                    ],
                    l=[inp.default] * length,
                )
                # Connect each body node input to the input interface's respective output
                for body_node, out in zip(body_nodes, interface.outputs):
                    body_node.inputs[label] = out
                macro.inputs_map[f"{interface.label}__l"] = interface.label
                # TODO: Don't hardcode __l
            # Or distribute the same input to each node equally
            else:
                interface = macro.create.standard.UserInput(
                    label=label, output_labels=label, user_input=inp.default
                )
                for body_node in body_nodes:
                    body_node.inputs[label] = interface
                macro.inputs_map[f"{interface.label}__user_input"] = interface.label
                # TODO: Don't hardcode __user_input

        # Make output interface: outputs to lists
        for label, out in body_nodes[0].outputs.items():
            interface = input_to_list(length)(
                parent=macro,
                label=label.upper(),
                output_labels=f"{loop_body_class.__name__}__{label}",
            )
            # Connect each body node output to the output interface's respective input
            for body_node, inp in zip(body_nodes, interface.inputs):
                inp.connect(body_node.outputs[label])
                if body_node.executor:
                    raise NotImplementedError(
                        "Right now the output interface gets run after each body node,"
                        "if the body nodes can run asynchronously we need something "
                        "more clever than that!"
                    )
            macro.outputs_map[
                f"{interface.label}__{loop_body_class.__name__}__{label}"
            ] = interface.label
            # TODO: Don't manually copy the output label construction

    return macro_node()(make_loop)


def while_loop(
    loop_body_class: type[Node],
    condition_class: type[SingleValue],
    internal_connection_map: dict[str, str],
    inputs_map: Optional[dict[str, str]] = None,
    outputs_map: Optional[dict[str, str]] = None,
) -> type[Macro]:
    """
    An _extremely rough_ first draft of a for-loop meta-node.

    Takes body and condition node classes and builds a macro that makes a cyclic signal
    connection between them and an "if" switch, i.e. when the body node finishes it
    runs the condtion, which runs the switch, and as long as the condition result was
    `True`, the switch loops back to run the body again.
    We additionally allow four-tuples of (input node, input channel, output node,
    output channel) labels to wire data connections inside the macro, e.g. to pass data
    from the body to the condition. This is beastly syntax, but it will suffice for now.
    Finally, you can set input and output maps as normal.

    Args:
        loop_body_class (type[pyiron_workflow.node.Node]): The class for the
            body of the while-loop.
        condition_class (type[pyiron_workflow.function.SingleValue]): A single
            value node returning a `bool` controlling the while loop exit condition
            (exits on False)
        internal_connection_map (list[tuple[str, str, str, str]]): String tuples
            giving (input node, input channel, output node, output channel) labels
            connecting channel pairs inside the macro.
        inputs_map Optional[dict[str, str]]: The inputs map as usual for a macro.
        outputs_map Optional[dict[str, str]]: The outputs map as usual for a macro.
    Examples:
        >>> from pyiron_workflow import Workflow
        >>>
        >>> @Workflow.wrap_as.single_value_node()
        >>> def add(a, b):
        ...     print(f"{a} + {b} = {a + b}")
        ...     return a + b
        >>>
        >>> @Workflow.wrap_as.single_value_node()
        >>> def less_than_ten(value):
        ...     return value < 10
        >>>
        >>> AddWhile = Workflow.create.meta.while_loop(
        ...     loop_body_class=add,
        ...     condition_class=less_than_ten,
        ...     internal_connection_map=[
        ...         ("Add", "a + b", "LessThanTen", "value"),
        ...         ("Add", "a + b", "Add", "a")
        ...     ],
        ...     inputs_map={"Add__a": "a", "Add__b": "b"},
        ...     outputs_map={"Add__a + b": "total"}
        ... )
        >>>
        >>> wf = Workflow("do_while")
        >>> wf.add_while = AddWhile()
        >>>
        >>> wf.inputs_map = {
        ...     "add_while__a": "a",
        ...     "add_while__b": "b"
        ... }
        >>> wf.outputs_map = {"add_while__total": "total"}
        >>>
        >>> print(f"Finally, {wf(a=1, b=2).total}")
        1 + 2 = 3
        3 + 2 = 5
        5 + 2 = 7
        7 + 2 = 9
        9 + 2 = 11
        Finally, 11

        >>> import numpy as np
        >>> from pyiron_workflow import Workflow
        >>>
        >>> np.random.seed(0)
        >>>
        >>> @Workflow.wrap_as.single_value_node("random")
        >>> def random(length: int | None = None):
        ...     return np.random.random(length)
        >>>
        >>> @Workflow.wrap_as.single_value_node()
        >>> def greater_than(x: float, threshold: float):
        ...     gt = x > threshold
        ...     symbol = ">" if gt else "<="
        ...     print(f"{x:.3f} {symbol} {threshold}")
        ...     return gt
        >>>
        >>> RandomWhile = Workflow.create.meta.while_loop(
        ...     loop_body_class=random,
        ...     condition_class=greater_than,
        ...     internal_connection_map=[("Random", "random", "GreaterThan", "x")],
        ...     outputs_map={"Random__random": "capped_result"}
        ... )
        >>>
        >>> # Define workflow
        >>>
        >>> wf = Workflow("random_until_small_enough")
        >>>
        >>> ## Wire together the while loop and its condition
        >>>
        >>> wf.random_while = RandomWhile()
        >>>
        >>> ## Give convenient labels
        >>> wf.inputs_map = {"random_while__GreaterThan__threshold": "threshold"}
        >>> wf.outputs_map = {"random_while__capped_result": "capped_result"}
        >>>
        >>> # Set a threshold and run
        >>> print(f"Finally {wf(threshold=0.1).capped_result:.3f}")
        0.549 > 0.1
        0.715 > 0.1
        0.603 > 0.1
        0.545 > 0.1
        0.424 > 0.1
        0.646 > 0.1
        0.438 > 0.1
        0.892 > 0.1
        0.964 > 0.1
        0.383 > 0.1
        0.792 > 0.1
        0.529 > 0.1
        0.568 > 0.1
        0.926 > 0.1
        0.071 <= 0.1
        Finally 0.071
    """

    def make_loop(macro):
        body_node = macro.add(loop_body_class(label=loop_body_class.__name__))
        condition_node = macro.add(condition_class(label=condition_class.__name__))
        switch = macro.create.standard.If(label="switch")

        switch.inputs.condition = condition_node
        for out_n, out_c, in_n, in_c in internal_connection_map:
            macro.nodes[in_n].inputs[in_c] = macro.nodes[out_n].outputs[out_c]

        switch.signals.output.true > body_node > condition_node > switch
        macro.starting_nodes = [body_node]

        macro.inputs_map = {} if inputs_map is None else inputs_map
        macro.outputs_map = {} if outputs_map is None else outputs_map

    return macro_node()(make_loop)


meta_nodes = DotDict(
    {
        for_loop.__name__: for_loop,
        input_to_list.__name__: input_to_list,
        list_to_output.__name__: list_to_output,
        while_loop.__name__: while_loop,
    }
)
