"""
Meta nodes are callables that create a node class instead of a node instance.
"""

from __future__ import annotations

from textwrap import dedent
from typing import Optional, TYPE_CHECKING

from pyiron_workflow.function import Function, as_function_node
from pyiron_workflow.macro import Macro, as_macro_node

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


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


def for_loop(
    loop_body_class: type[Node],
    length: int,
    iterate_on: str | tuple[str] | list[str],
) -> type[Macro]:
    """
    An _extremely rough_ second draft of a for-loop meta-node.

    Takes a node class, how long the loop should be, and which input(s) of the provided
    node class should be looped over (given as strings of the channel labels) and
    builds a macro that scatters some input and broadcasts the rest, then operates on
    a zip of all the scattered input (so it had better be the same length).

    - Makes copies of the provided node class, i.e. the "body node"
    - Labels in :param:`iterate_on` must correspond to `loop_body_class` input channels,
        and the for-loop node then expects list-like input for these with ALL CAPS
        labeling, and this gets scattered to the children.
    - All other input simply gets broadcast to each child.
    - Output channels correspond to input channels, but are lists of the children and
        labeled in ALL CAPS

    Warnings:
        The loop body class must be importable. E.g. it can come from a node package or
        be defined in `__main__`, but not defined inside the scope of some other
        function.

    Examples:

        >>> from pyiron_workflow import Workflow
        >>>
        >>> denominators = list(range(1, 5))
        >>> bulk_loop = Workflow.create.meta.for_loop(
        ...     Workflow.create.standard.Divide,
        ...     len(denominators),
        ...     iterate_on = ("other",),
        ... )()
        >>> bulk_loop.inputs.obj = 1
        >>> bulk_loop.inputs.OTHER = denominators
        >>> bulk_loop().TRUEDIV
        [1.0, 0.5, 0.3333333333333333, 0.25]

    TODO:

        - Refactor like crazy, it's super hard to read and some stuff is too hard-coded
        - How to handle passing executors to the children? Maybe this is more
          generically a Macro question?
        - Is it possible to somehow dynamically adapt the held graph depending on the
          length of the input values being iterated over? E.g. rebuilding the graph
          every run call.
        - Allow a different mode, or make a different meta node, that makes all possible
          pairs of body nodes given the input being looped over instead of just
          :param:`length`
        - Provide enter and exit magic methods so we can `for` or `with` this fancy-like
    """
    input_preview = loop_body_class.preview_input_channels()
    output_preview = loop_body_class.preview_output_channels()

    # Ensure `iterate_on` is in the input
    iterate_on = [iterate_on] if isinstance(iterate_on, str) else iterate_on
    incommensurate_input = set(iterate_on).difference(input_preview.keys())
    if len(incommensurate_input) > 0:
        raise ValueError(
            f"Cannot loop on {incommensurate_input}, as it is not an input channel "
            f"of {loop_body_class.__name__}; please choose from among "
            f"{list(input_preview)}"
        )

    # Build code components that need an f-string, slash, etc.
    output_labels = ", ".join(f'"{l.upper()}"' for l in output_preview.keys()).rstrip(
        " "
    )
    macro_args = ", ".join(
        l.upper() if l in iterate_on else l for l in input_preview.keys()
    ).rstrip(" ")
    body_label = 'f"body{n}"'
    item_access = "[{n}]"
    body_kwargs = ", ".join(
        f"{l}={l.upper()}[n]" if l in iterate_on else f"{l}={l}"
        for l in input_preview.keys()
    ).rstrip(" ")
    input_label = 'f"inp{n}"'
    returns = ", ".join(
        f'macro.children["{label.upper()}"]' for label in output_preview.keys()
    )
    node_name = f'{loop_body_class.__name__}For{"".join([l.title() for l in sorted(iterate_on)])}{length}'

    # Assemble components into a decorated for-loop macro
    for_loop_code = dedent(
        f"""
        @Macro.wrap.as_macro_node({output_labels})
        def {node_name}(macro, {macro_args}):
            from {loop_body_class.__module__} import {loop_body_class.__name__}

            for label in [{output_labels}]:
                input_to_list({length})(label=label, parent=macro)

            for n in range({length}):
                body_node = {loop_body_class.__name__}(
                    {body_kwargs},
                    label={body_label},
                    parent=macro
                )
                for label in {list(output_preview.keys())}:
                    macro.children[label.upper()].inputs[{input_label}] = body_node.outputs[label]

            return {returns}
        """
    )

    exec(for_loop_code)
    return locals()[node_name]


def while_loop(
    loop_body_class: type[Node],
    condition_class: type[Function],
    internal_connection_map: dict[str, str],
    inputs_map: Optional[dict[str, str]],
    outputs_map: Optional[dict[str, str]],
) -> type[Macro]:
    """
    An _extremely rough_ second draft of a for-loop meta-node.

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
        condition_class (type[pyiron_workflow.function.Function]): A
            single-output function node returning a `bool` controlling the while loop
            exit condition (exits on False)
        internal_connection_map (list[tuple[str, str, str, str]]): String tuples
            giving (input node, input channel, output node, output channel) labels
            connecting channel pairs inside the macro.
        inputs_map (dict[str, str]): Define the inputs for the new macro like
            `{body/condition class name}__{input channel}: {macro input channel name}`
        outputs_map (dict[str, str]): Define the outputs for the new macro like
            `{body/condition class name}__{output channel}: {macro output channel name}`

    Warnings:
        The loop body and condition classes must be importable. E.g. they can come from
        a node package or be defined in `__main__`, but not defined inside the scope of
        some other function.

    Examples:

        >>> from pyiron_workflow import Workflow
        >>>
        >>> AddWhile = Workflow.create.meta.while_loop(
        ...     loop_body_class=Workflow.create.standard.Add,
        ...     condition_class=Workflow.create.standard.LessThan,
        ...     internal_connection_map=[
        ...         ("Add", "add", "LessThan", "obj"),
        ...         ("Add", "add", "Add", "obj")
        ...     ],
        ...     inputs_map={
        ...         "Add__obj": "a",
        ...         "Add__other": "b",
        ...         "LessThan__other": "cap"
        ...     },
        ...     outputs_map={"Add__add": "total"}
        ... )
        >>>
        >>> wf = Workflow("do_while")
        >>> wf.add_while = AddWhile(cap=10)
        >>>
        >>> wf.inputs_map = {
        ...     "add_while__a": "a",
        ...     "add_while__b": "b"
        ... }
        >>> wf.outputs_map = {"add_while__total": "total"}
        >>>
        >>> print(f"Finally, {wf(a=1, b=2).total}")
        Finally, 11

        >>> import random
        >>>
        >>> from pyiron_workflow import Workflow
        >>>
        >>> random.seed(0)  # Set the seed so the output is consistent and doctest runs
        >>>
        >>> RandomWhile = Workflow.create.meta.while_loop(
        ...     loop_body_class=Workflow.create.standard.RandomFloat,
        ...     condition_class=Workflow.create.standard.GreaterThan,
        ...     internal_connection_map=[
        ...         ("RandomFloat", "random", "GreaterThan", "obj")
        ...     ],
        ...     inputs_map={"GreaterThan__other": "threshold"},
        ...     outputs_map={"RandomFloat__random": "capped_result"}
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
        >>> wf.inputs_map = {"random_while__threshold": "threshold"}
        >>> wf.outputs_map = {"random_while__capped_result": "capped_result"}
        >>>
        >>> # Set a threshold and run
        >>> print(f"Finally {wf(threshold=0.3).capped_result:.3f}")
        Finally 0.259
    """

    # Make sure each dynamic class is getting a unique name
    io_hash = hash(
        ",".join(
            [
                "_".join(s for conn in internal_connection_map for s in conn),
                "".join(f"{k}:{v}" for k, v in sorted(inputs_map.items())),
                "".join(f"{k}:{v}" for k, v in sorted(outputs_map.items())),
            ]
        )
    )
    io_hash = str(io_hash).replace("-", "m")
    node_name = f"{loop_body_class.__name__}While{condition_class.__name__}_{io_hash}"

    # Build code components that need an f-string, slash, etc.
    output_labels = ", ".join(f'"{l}"' for l in outputs_map.values()).rstrip(" ")
    input_args = ", ".join(l for l in inputs_map.values()).rstrip(" ")

    def get_kwargs(io_map: dict[str, str], node_class: type[Node]):
        return ", ".join(
            f'{k.split("__")[1]}={v}'
            for k, v in io_map.items()
            if k.split("__")[0] == node_class.__name__
        ).rstrip(" ")

    returns = ", ".join(
        f'macro.{l.split("__")[0]}.outputs.{l.split("__")[1]}'
        for l in outputs_map.keys()
    ).rstrip(" ")

    # Assemble components into a decorated while-loop macro
    while_loop_code = dedent(
        f"""
        @Macro.wrap.as_macro_node({output_labels})
        def {node_name}(macro, {input_args}):
            from {loop_body_class.__module__} import {loop_body_class.__name__}
            from {condition_class.__module__} import {condition_class.__name__}

            body = macro.add_child(
                {loop_body_class.__name__}(
                    label="{loop_body_class.__name__}",
                    {get_kwargs(inputs_map, loop_body_class)}
                )
            )

            condition = macro.add_child(
                {condition_class.__name__}(
                    label="{condition_class.__name__}",
                    {get_kwargs(inputs_map, condition_class)}
                )
            )

            macro.switch = macro.create.standard.If(condition=condition)

            for out_n, out_c, in_n, in_c in {str(internal_connection_map)}:
                macro.children[in_n].inputs[in_c] = macro.children[out_n].outputs[out_c]


            macro.switch.signals.output.true >> body >> condition >> macro.switch
            macro.starting_nodes = [body]

            return {returns}
        """
    )

    exec(while_loop_code)
    return locals()[node_name]

    # def make_loop(macro):
    #     body_node = macro.add_child(loop_body_class(label=loop_body_class.__name__))
    #     condition_node = macro.add_child(
    #         condition_class(label=condition_class.__name__)
    #     )
    #     switch = macro.create.standard.If(label="switch", parent=macro)
    #
    #     switch.inputs.condition = condition_node
    #     for out_n, out_c, in_n, in_c in internal_connection_map:
    #         macro.children[in_n].inputs[in_c] = macro.children[out_n].outputs[out_c]
    #
    #     switch.signals.output.true >> body_node >> condition_node >> switch
    #     macro.starting_nodes = [body_node]
    #
    #     macro.inputs_map = {} if inputs_map is None else inputs_map
    #     macro.outputs_map = {} if outputs_map is None else outputs_map
    #
    # return as_macro_node()(make_loop)
