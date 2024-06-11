from __future__ import annotations

import random
from textwrap import dedent
from typing import Optional

import pyiron_workflow
from pyiron_workflow import Workflow
from pyiron_workflow.function import Function
from pyiron_workflow.macro import Macro
from pyiron_workflow.transform import inputs_to_list, list_to_outputs
from pyiron_workflow.node import Node


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
        f'self.{l.split("__")[0]}.outputs.{l.split("__")[1]}'
        for l in outputs_map.keys()
    ).rstrip(" ")

    # Assemble components into a decorated while-loop macro
    while_loop_code = dedent(
        f"""
        @Macro.wrap.as_macro_node({output_labels})
        def {node_name}(self, {input_args}):
            from {loop_body_class.__module__} import {loop_body_class.__name__}
            from {condition_class.__module__} import {condition_class.__name__}

            body = self.add_child(
                {loop_body_class.__name__}(
                    label="{loop_body_class.__name__}",
                    {get_kwargs(inputs_map, loop_body_class)}
                )
            )

            condition = self.add_child(
                {condition_class.__name__}(
                    label="{condition_class.__name__}",
                    {get_kwargs(inputs_map, condition_class)}
                )
            )

            self.switch = self.create.standard.If(condition=condition)

            for out_n, out_c, in_n, in_c in {str(internal_connection_map)}:
                self.children[in_n].inputs[in_c] = self.children[out_n].outputs[out_c]


            self.switch.signals.output.true >> body >> condition >> self.switch
            self.starting_nodes = [body]

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
