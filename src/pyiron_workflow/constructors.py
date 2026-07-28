from __future__ import annotations

import dataclasses
import re
import types
from typing import TypeAlias, cast

import flowrep as fr

from pyiron_workflow import (
    atomic_node,
    constant,
    dag,
    datatypes,
    flowcontrollers,
    workflow_node,
)

RecipeOptions: TypeAlias = (
    fr.schemas.AtomicRecipe
    | fr.schemas.ForEachRecipe
    | fr.schemas.IfRecipe
    | fr.schemas.TryRecipe
    | fr.schemas.WhileRecipe
    | fr.schemas.WorkflowRecipe
    | fr.schemas.ConstantRecipe
)


NodeLike: TypeAlias = (
    datatypes.Node | RecipeOptions | types.FunctionType | type | fr.schemas.JSONABLE
)


def node(
    value: NodeLike,
    label: fr.schemas.Label | None = None,
    /,
    **connections: datatypes.Port | datatypes.Node | fr.schemas.JSONABLE,
) -> datatypes.Node:
    """
    Convert a node-like `value` into a `Node` labelled `label`.

    Accepts a `Node`, an `flowrep` recipe, a plain function or class, or a JSONable
    constant. Raises `TypeError` otherwise.

    When the passed object is already a node instance, returns a copy.

    Functions and classes will be searched for an attached `flowrep` recipe, and
    otherwise parsed as atomic nodes. Un-parseable callables will raise the underlying
    `flowrep` error.
    """
    if isinstance(value, datatypes.Node):
        copied = value.copy(label)
        copied.establish_sources(**connections)
        return copied
    elif isinstance(value, RecipeOptions):
        return recipe2node(value, label, **connections)
    elif isinstance(value, type | types.FunctionType):
        return atomictype2node(value, label, **connections)
    elif fr.tools.is_jsonable(value):
        if connections:
            _raise_no_inputs_for_constants()
        return constant.Constant.from_value(value, label)
    else:
        raise TypeError(
            f"Cannot assign {value!r} as node {label!r}: expected a Node, "
            f"flowrep recipe, or function or class (with or without a flowrep recipe "
            f"attached)."
        )


def _raise_no_inputs_for_constants():
    raise TypeError("Constant nodes cannot take incoming connections.")


def _disallow_locals(obj: types.FunctionType | type) -> None:
    """
    Nodes hold a reference to their underlying object and must be able to re-import it,
    e.g. to ship the node to another process, so locally-scoped objects are refused.
    """
    if "<locals>" in obj.__qualname__:
        raise ImportError(
            "To turn functions and classes into nodes, pyiron_workflow needs to be "
            "able to import the underlying object; but "
            f"{obj.__qualname__!r} contains '<locals>'."
        )


def atomictype2node(
    function: types.FunctionType | type,
    label: fr.schemas.Label | None = None,
    /,
    **connections: datatypes.Port | datatypes.Node | fr.schemas.JSONABLE,
) -> atomic_node.Atomic | dag.Macro:
    """
    Convert a function or class into a node labelled `label` (defaulting to its
    `__name__`).

    Only a recipe attached to `function` _itself_ is honoured; an inherited recipe is
    ignored, so that an undecorated subclass of a decorated class gets parsed afresh
    instead of silently referencing its parent.
    """
    _disallow_locals(function)
    recipe = vars(function).get("flowrep_recipe", None)
    if recipe is not None:
        if not isinstance(recipe, fr.schemas.NodeRecipe):
            raise TypeError(
                f"Cannot convert {function!r} to a Node: it has a 'flowrep_recipe' "
                f"attribute, but this is not a '{fr.schemas.NodeRecipe.__name__}' "
                f"-- got {type(recipe)!r}: {recipe}"
            )
        # flowrep-decorated functions are all either atomic or workflow recipes
        return cast(
            atomic_node.Atomic | dag.Macro,
            recipe2node(
                cast(fr.schemas.AtomicRecipe | fr.schemas.WorkflowRecipe, recipe),
                label or function.__name__,
                **connections,
            ),
        )
    else:
        # Otherwise parse undecorated functions as atomic nodes
        recipe = fr.tools.parse_atomic(function)
        return atomic_node.Atomic(recipe, label or function.__name__, **connections)


def recipe2node(
    recipe: RecipeOptions,
    label: fr.schemas.Label | None = None,
    /,
    **connections: datatypes.Port | datatypes.Node | fr.schemas.JSONABLE,
) -> datatypes.StaticNode:
    label = (
        f"{_pascal_to_snake(recipe.__class__.__name__)}_node"
        if label is None
        else label
    )

    if isinstance(recipe, fr.schemas.AtomicRecipe):
        return atomic_node.Atomic(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.ForEachRecipe):
        return flowcontrollers.ForEach(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.IfRecipe):
        return flowcontrollers.If(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.TryRecipe):
        return flowcontrollers.Try(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.WhileRecipe):
        return flowcontrollers.While(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.WorkflowRecipe):
        return dag.Macro(recipe, label, **connections)
    elif isinstance(recipe, fr.schemas.ConstantRecipe):
        if connections:
            _raise_no_inputs_for_constants()
        return constant.Constant(recipe, label, **connections)
    else:
        raise TypeError(
            f"Unknown recipe type: {recipe}. Expected one of {RecipeOptions}."
        )


def _pascal_to_snake(name: str):
    snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return snake_case


def edges2edgelist(
    input_edges: fr.schemas.InputEdges,
    edges: fr.schemas.Edges,
    output_edges: fr.schemas.OutputEdges,
) -> datatypes.EdgeList:
    return (
        datatypes.EdgeList(datatypes.EdgeTuple(s, t) for t, s in input_edges.items())
        + datatypes.EdgeList(datatypes.EdgeTuple(s, t) for t, s in edges.items())
        + datatypes.EdgeList(datatypes.EdgeTuple(s, t) for t, s in output_edges.items())
    )


def edgelist2edges(
    edges: datatypes.EdgeList,
    scope: str = "<unknown EdgeList owner>",
) -> tuple[fr.schemas.InputEdges, fr.schemas.Edges, fr.schemas.OutputEdges]:
    inp: fr.schemas.InputEdges = {}
    peer: fr.schemas.Edges = {}
    out: fr.schemas.OutputEdges = {}
    for source, target in edges:
        if isinstance(source, fr.schemas.InputSource) and isinstance(
            target, fr.schemas.TargetHandle
        ):
            inp[target] = source
        elif isinstance(source, fr.schemas.SourceHandle) and isinstance(
            target, fr.schemas.TargetHandle
        ):
            peer[target] = source
        elif isinstance(
            source, fr.schemas.SourceHandle | fr.schemas.InputSource
        ) and isinstance(target, fr.schemas.OutputTarget):
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


def workflow2macro(wf: workflow_node.Workflow) -> dag.Macro:
    macro = dag.Macro(wf.recipe, wf.label)
    _copy_port_annotations(wf.inputs, macro.inputs)
    _copy_port_annotations(wf.outputs, macro.outputs)
    _copy_executors(wf, macro)
    return macro


def macro2workflow(macro: dag.Macro) -> workflow_node.Workflow:
    wf = workflow_node.Workflow(macro.label)
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
