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
                cast(frs.AtomicRecipe | frs.WorkflowRecipe, recipe),
            ),
        )
    else:
        # Otherwise parse undecorated functions as atomic nodes
        recipe = frt.parse_atomic(function)
        return atomic.Atomic(label or function.__name__, recipe)


RecipeOptions: TypeAlias = (
    frs.AtomicRecipe
    | frs.ForEachRecipe
    | frs.IfRecipe
    | frs.TryRecipe
    | frs.WhileRecipe
    | frs.WorkflowRecipe
)


def recipe2static(
    label: frs.Label,
    recipe: RecipeType,
    owner: Graph | None = None,
) -> StaticNode:
    if isinstance(recipe, frs.AtomicRecipe):
        return atomic.Atomic(label, recipe, owner=owner)
    elif isinstance(recipe, frs.ForEachRecipe):
        return flowcontrollers.ForEach(label, recipe, owner=owner)
    elif isinstance(recipe, frs.IfRecipe):
        return flowcontrollers.If(label, recipe, owner=owner)
    elif isinstance(recipe, frs.TryRecipe):
        return flowcontrollers.Try(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WhileRecipe):
        return flowcontrollers.While(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WorkflowRecipe):
        return dag.Macro(label, recipe, owner=owner)
    else:
        raise TypeError(
            f"Unknown recipe type: {recipe}. Expected one of {RecipeOptions}."
        )
