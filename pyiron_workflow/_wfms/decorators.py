from __future__ import annotations

import functools
import types
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import (
    atomic as atomic_mod,
)
from pyiron_workflow._wfms import dag, execution, validation

if TYPE_CHECKING:
    import rdflib


_RecipeType = TypeVar("_RecipeType", frs.AtomicRecipe, frs.WorkflowRecipe)


class PwfTools(Generic[_RecipeType]):
    node_type: ClassVar[type[atomic_mod.Atomic] | type[dag.Macro]]

    def __init__(self, wrapped: types.FunctionType):
        self.function = wrapped

    @property
    def _recipe(self) -> _RecipeType:
        return self.function.flowrep_recipe  # type: ignore[attr-defined]

    def node(self, label: frs.Label | None = None, /, *positional, **keyword):
        used_label = self.function.__name__ if label is None else label
        return self.node_type(used_label, self._recipe, *positional, **keyword)

    def run(self, config: execution.RunConfig | None = None, **input_data):
        return self.node().run(config, **input_data)


class AtomicTools(PwfTools[frs.AtomicRecipe]):
    node_type = atomic_mod.Atomic


class MacroTools(PwfTools[frs.WorkflowRecipe]):
    node_type = dag.Macro

    def validate(
        self,
        do_types: bool = True,
        do_ontology: bool = True,
        with_io: bool = True,
        with_function: bool = True,
        extra_knowledge: rdflib.Graph | None = None,
    ) -> validation.CombinedValidationReport:
        return self.node().validate(
            do_types=do_types,
            do_ontology=do_ontology,
            with_io=with_io,
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )


_assigned = tuple(
    a
    for a in functools.WRAPPER_ASSIGNMENTS
    if a not in {"__name__", "__module__", "__qualname__"}
)  # Let the wrapping functions keep their identity


@functools.wraps(frt.atomic, assigned=_assigned)
def atomic(*args, **kwargs):
    wrapped = frt.atomic(*args, **kwargs)
    return _double_wrap_if_decorator_got_args(args[0], wrapped, AtomicTools, "@atomic")


atomic.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.Atomic` node instance or create a dynamic node instance and 
 run it.

Base `flowrep` documentation:

""" + (frt.atomic.__doc__ or "")


def _double_wrap_if_decorator_got_args(
    arg0: str | types.FunctionType,
    wrapped: types.FunctionType,
    tool: type[AtomicTools] | type[MacroTools],
    decorator_name: str,
):
    if isinstance(arg0, types.FunctionType):
        if not hasattr(wrapped, "pwf"):  # pragma: no cover
            raise ValueError(
                f"The {decorator_name} decorator must be applied to a function "
                f"decorated with flowrep. This is an internal error and likely flags "
                f"a development bug."
            )
        wrapped.pwf = tool(wrapped)
        return wrapped
    elif isinstance(arg0, str):

        def wrapped_decorator(func):
            double_wrapped = wrapped(func)
            double_wrapped.pwf = tool(double_wrapped)
            return double_wrapped

        return wrapped_decorator
    else:
        raise TypeError(
            f"{decorator_name} can only decorate functions, got {type(arg0).__name__}"
        )


@functools.wraps(frt.workflow, assigned=_assigned)
def workflow(*args, **kwargs):
    wrapped = frt.workflow(*args, **kwargs)
    return _double_wrap_if_decorator_got_args(args[0], wrapped, MacroTools, "@workflow")


workflow.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.Macro` node instance, validate the underlying graph, or 
create a dynamic node instance and run it.

Base `flowrep` documentation:

""" + (frt.workflow.__doc__ or "")
