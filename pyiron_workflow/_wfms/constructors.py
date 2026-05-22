from __future__ import annotations

import re
import types
from typing import TypeAlias, cast

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import atomic, dag, datatypes, flowcontrollers
from pyiron_workflow._wfms.datatypes import Graph, StaticNode

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
    function: types.FunctionType, label: frs.Label | None = None
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


def recipe2node(
    recipe: RecipeOptions, label: frs.Label | None = None, owner: Graph | None = None
) -> StaticNode:
    label = (
        f"{_pascal_to_snake(recipe.__class__.__name__)}_node"
        if label is None
        else label
    )

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


def _pascal_to_snake(name: str):
    snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return snake_case
