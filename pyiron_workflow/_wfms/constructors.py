from __future__ import annotations

import dataclasses
import re
import types
from typing import TypeAlias, cast

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import atomic, dag, datatypes, flowcontrollers, workflow
from pyiron_workflow._wfms.datatypes import EdgeList, EdgeTuple, StaticNode

RecipeOptions: TypeAlias = (
    frs.AtomicRecipe
    | frs.ForEachRecipe
    | frs.IfRecipe
    | frs.TryRecipe
    | frs.WhileRecipe
    | frs.WorkflowRecipe
)


def node(value: object, label: frs.Label | None = None) -> datatypes.Node:
    """
    Convert a node-like `value` into a `Node` labelled `label`.

    Accepts a `Node`, an `flowrep` recipe, or a plain function. Raises
    `TypeError` otherwise.

    When the passed object is already a node instance, simply attempts to relabel it.

    Functions will be searched for an attached `flowrep` recipe, and otherwise parsed
    as atomic nodes. Un-parseable functions will raise the underlying `flowrep` error.
    """
    if isinstance(value, datatypes.Node):
        if label is not None:
            value.label = label
        return value
    if isinstance(value, RecipeOptions):
        return recipe2node(value, label)
    if isinstance(value, types.FunctionType):
        return function2node(value, label)
    raise TypeError(
        f"Cannot assign {value!r} as node {label!r}: expected a Node, "
        f"flowrep recipe, or function (with or without a flowrep recipe attached)."
    )


def function2node(
    function: types.FunctionType,
    label: frs.Label | None = None,
) -> atomic.Atomic | dag.Macro:
    recipe = getattr(function, "flowrep_recipe", None)
    if recipe:
        # flowrep-decorated functions are all either atomic or workflow recipes
        return cast(
            atomic.Atomic | dag.Macro,
            recipe2node(
                cast(frs.AtomicRecipe | frs.WorkflowRecipe, recipe),
                label or function.__name__,
            ),
        )
    else:
        # Otherwise parse undecorated functions as atomic nodes
        recipe = frt.parse_atomic(function)
        return atomic.Atomic(label or function.__name__, recipe)


def recipe2node(recipe: RecipeOptions, label: frs.Label | None = None) -> StaticNode:
    label = (
        f"{_pascal_to_snake(recipe.__class__.__name__)}_node"
        if label is None
        else label
    )

    if isinstance(recipe, frs.AtomicRecipe):
        return atomic.Atomic(label, recipe)
    elif isinstance(recipe, frs.ForEachRecipe):
        return flowcontrollers.ForEach(label, recipe)
    elif isinstance(recipe, frs.IfRecipe):
        return flowcontrollers.If(label, recipe)
    elif isinstance(recipe, frs.TryRecipe):
        return flowcontrollers.Try(label, recipe)
    elif isinstance(recipe, frs.WhileRecipe):
        return flowcontrollers.While(label, recipe)
    elif isinstance(recipe, frs.WorkflowRecipe):
        return dag.Macro(label, recipe)
    else:
        raise TypeError(
            f"Unknown recipe type: {recipe}. Expected one of {RecipeOptions}."
        )


def _pascal_to_snake(name: str):
    snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return snake_case


def edges2edgelist(
    input_edges: frs.InputEdges,
    edges: frs.Edges,
    output_edges: frs.OutputEdges,
) -> EdgeList:
    return (
        EdgeList(EdgeTuple(s, t) for t, s in input_edges.items())
        + EdgeList(EdgeTuple(s, t) for t, s in edges.items())
        + EdgeList(EdgeTuple(s, t) for t, s in output_edges.items())
    )


def edgelist2edges(
    edges: EdgeList,
    scope: str = "<unknown EdgeList owner>",
) -> tuple[frs.InputEdges, frs.Edges, frs.OutputEdges]:
    inp: frs.InputEdges = {}
    peer: frs.Edges = {}
    out: frs.OutputEdges = {}
    for source, target in edges:
        if isinstance(source, frs.InputSource) and isinstance(target, frs.TargetHandle):
            inp[target] = source
        elif isinstance(source, frs.SourceHandle) and isinstance(
            target, frs.TargetHandle
        ):
            peer[target] = source
        elif isinstance(source, frs.SourceHandle | frs.InputSource) and isinstance(
            target, frs.OutputTarget
        ):
            out[target] = source
        else:
            raise TypeError(
                f"{scope} has an edge that does not fit into known "
                f"input/peer/output buckets: {source!r} -> {target!r}"
            )
    return inp, peer, out


def _copy_port_annotations(
    src: datatypes.PortMap,
    dst: datatypes.PortMap,
) -> None:
    """Overwrite each port in `dst` with one carrying the type_hint and
    type_metadata of the same-labelled port in `src`. Mutates `dst` via the
    `LexicalMap` slot path because `PortMap` exposes no public setter."""
    for label, src_port in src.items():
        dst_port = dst._pwf_lexical_map__data[label]
        dst._pwf_lexical_map__data[label] = dataclasses.replace(
            dst_port,
            type_hint=src_port.type_hint,
            type_metadata=src_port.type_metadata,
        )


def _copy_executors(src: datatypes.Node, dst: datatypes.Node) -> None:
    """Recursively copy `executor` from `src` to `dst`. If both are graphs,
    descend by matching child label."""
    dst.executor = src.executor
    if isinstance(src, datatypes.Graph) and isinstance(dst, datatypes.Graph):
        for label, child in src.nodes.items():
            if label in dst.nodes:
                _copy_executors(child, dst.nodes[label])


def workflow2macro(wf: workflow.Workflow) -> dag.Macro:
    macro = dag.Macro(wf.label, wf.recipe)
    _copy_port_annotations(wf.inputs, macro.inputs)
    _copy_port_annotations(wf.outputs, macro.outputs)
    _copy_executors(wf, macro)
    return macro


def macro2workflow(macro: dag.Macro) -> workflow.Workflow:
    wf = workflow.Workflow(macro.label)
    for label, node_recipe in macro.recipe.nodes.items():
        wf.add_node(recipe2node(node_recipe, label))

    for port_creator, reference in (
        (wf.create_input, macro.inputs),
        (wf.create_output, macro.outputs),
    ):
        for label, port in reference.items():
            port_creator(
                label, type_hint=port.type_hint, type_metadata=port.type_metadata
            )

    wf.add_edge(*macro.edges)

    _copy_port_annotations(wf.inputs, macro.inputs)
    _copy_port_annotations(wf.outputs, macro.outputs)
    _copy_executors(macro, wf)
    return wf
