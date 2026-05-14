from __future__ import annotations

import types
from typing import TypeAlias, cast

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import atomic, dag, flowcontrollers
from pyiron_workflow._wfms.datatypes import Graph, RecipeType, StaticNode


def node(
    function: types.FunctionType, label: frs.Label | None = None
) -> atomic.Atomic | dag.Macro:
    recipe = getattr(function, "flowrep_recipe", None)
    if recipe:
        # flowrep-decorated functions are all either atomic or workflow recipes
        return cast(
            atomic.Atomic | dag.Macro,
            recipe2static(
                label or function.__name__,
                cast(frs.AtomicNode | frs.WorkflowNode, recipe),
            ),
        )
    else:
        # Otherwise parse undecorated functions as atomic nodes
        recipe = frt.parse_atomic(function)
        return atomic.Atomic(label or function.__name__, recipe)


RecipeOptions: TypeAlias = (
    frs.AtomicNode
    | frs.ForEachNode
    | frs.IfNode
    | frs.TryNode
    | frs.WhileNode
    | frs.WorkflowNode
)


def recipe2static(
    label: frs.Label,
    recipe: RecipeType,
    owner: Graph | None = None,
) -> StaticNode:
    if isinstance(recipe, frs.AtomicNode):
        return atomic.Atomic(label, recipe, owner=owner)
    elif isinstance(recipe, frs.ForEachNode):
        return flowcontrollers.ForEach(label, recipe, owner=owner)
    elif isinstance(recipe, frs.IfNode):
        return flowcontrollers.If(label, recipe, owner=owner)
    elif isinstance(recipe, frs.TryNode):
        return flowcontrollers.Try(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WhileNode):
        return flowcontrollers.While(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WorkflowNode):
        return dag.Macro(label, recipe, owner=owner)
    else:
        raise TypeError(
            f"Unknown recipe type: {recipe}. Expected one of {RecipeOptions}."
        )
