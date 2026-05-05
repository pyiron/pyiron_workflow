import types
from typing import cast

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import atomic, dag


def node(
    function: types.FunctionType, label: frs.Label | None = None
) -> atomic.Atomic | dag.Macro:
    recipe = getattr(function, "flowrep_recipe", None)
    if recipe:
        # flowrep-decorated functions are all either atomic or workflow recipes
        return cast(
            atomic.Atomic | dag.Macro,
            dag.recipe2static(
                label or function.__name__,
                cast(frs.AtomicNode | frs.WorkflowNode, recipe),
            ),
        )
    else:
        # Otherwise parse undecorated functions as atomic nodes
        recipe = frt.parse_atomic(function)
        return atomic.Atomic(label or function.__name__, recipe)
