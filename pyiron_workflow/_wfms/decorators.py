from __future__ import annotations

import abc
import functools
import types
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast

import flowrep as fr

from pyiron_workflow._wfms import (
    atomic as atomic_mod,
)
from pyiron_workflow._wfms import dag, execution, validation

if TYPE_CHECKING:
    import rdflib

_DecoratedType = TypeVar("_DecoratedType", bound=Callable[..., Any])
_RecipeType = TypeVar("_RecipeType", fr.schemas.AtomicRecipe, fr.schemas.WorkflowRecipe)
_NodeType = TypeVar("_NodeType", atomic_mod.Atomic, dag.Macro)


class _PwfTools(Generic[_DecoratedType, _RecipeType, _NodeType], abc.ABC):
    _node_type: type[_NodeType]
    assign_to: ClassVar[str] = "pwf"

    def __init__(self, wrapped: _DecoratedType):
        self._disallow_locals(wrapped)
        self._require_absent(wrapped, self.assign_to)
        self._decorated_object: _DecoratedType = wrapped

    @property
    def recipe(self) -> _RecipeType:
        return self._decorated_object.flowrep_recipe  # type: ignore[attr-defined]

    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label:
        return self._decorated_object.__name__ if label is None else label

    def node(
        self, label: fr.schemas.Label | None = None, /, *positional, **keyword
    ) -> _NodeType:
        return self._node_type(self.recipe, self._label(label), *positional, **keyword)

    def run(self, config: execution.RunConfig | None = None, **input_data):
        return self.node().run(config, **input_data)

    @staticmethod
    def _disallow_locals(func: _DecoratedType):
        if "<locals>" in func.__qualname__:
            raise ImportError(
                "To turn decorated functions into nodes, pyiron_workflow needs to be "
                "able to import the underlying decorated function; "
                f"but {func.__qualname__!r} contains '<locals>'."
            )

    @staticmethod
    def _require_absent(wrapped, *names):
        for name in names:
            if hasattr(wrapped, name):
                raise AttributeError(
                    f"{wrapped.__name__!r} already defines {name!r}; refusing to overwrite"
                )


class DecoratedAtomic(
    _PwfTools[Callable[..., Any], fr.schemas.AtomicRecipe, atomic_mod.Atomic]
):
    _node_type = atomic_mod.Atomic


class DecoratedMacro(
    _PwfTools[types.FunctionType, fr.schemas.WorkflowRecipe, dag.Macro]
):
    _node_type = dag.Macro

    def validate(
        self,
        do_types: bool = True,
        do_ontology: bool = True,
        extra_knowledge: rdflib.Graph | None = None,
    ) -> validation.CombinedValidationReport:
        return self.node().validate(
            do_types=do_types,
            do_ontology=do_ontology,
            extra_knowledge=extra_knowledge,
        )


class DecoratedDataclass(
    _PwfTools[type, fr.schemas.AtomicRecipe, atomic_mod.Atomic], abc.ABC
):
    _node_type = atomic_mod.Atomic
    assign_to: ClassVar[str] = "pwf"


class UnpackDataclass(
    _PwfTools[type, fr.schemas.AtomicRecipe, atomic_mod.Atomic], abc.ABC
):
    _node_type = atomic_mod.Atomic
    assign_to: ClassVar[str] = "pwf_unpacking"

    @property
    def recipe(self) -> atomic_mod.Atomic:
        return self._decorated_object.flowrep_recipe_unpacking  # type: ignore[attr-defined]

    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label:
        return "unpack_" + self._decorated_object.__name__ if label is None else label


_assigned = tuple(
    a
    for a in functools.WRAPPER_ASSIGNMENTS
    if a not in {"__name__", "__module__", "__qualname__"}
)  # Let the wrapping functions keep their identity


@functools.wraps(fr.tools.atomic, assigned=_assigned)
def atomic(*args, **kwargs):
    return _attach_pwf_tool(fr.tools.atomic(*args, **kwargs), DecoratedAtomic)


atomic.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.schemas.Atomic` node instance or create a dynamic node  
instance and run it.

Base `flowrep` documentation:

""" + (fr.tools.atomic.__doc__ or "")


@functools.wraps(fr.tools.workflow, assigned=_assigned)
def workflow(*args, **kwargs):
    return _attach_pwf_tool(fr.tools.workflow(*args, **kwargs), DecoratedMacro)


workflow.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.schemas.Macro` node instance, validate the underlying graph, 
or create a dynamic node instance and run it.

Base `flowrep` documentation:

""" + (fr.tools.workflow.__doc__ or "")


@functools.wraps(fr.tools.dataclass, assigned=_assigned)
def dataclass(*args, **kwargs):
    forwards = _attach_pwf_tool(fr.tools.dataclass(*args, **kwargs), DecoratedDataclass)
    and_backwards = _attach_pwf_tool(
        forwards,
        UnpackDataclass,
    )
    return and_backwards


dataclass.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.schemas.Atomic` node instance, validate the underlying graph, 
or create a dynamic node instance and run it. Attaches the analogous tools for 
the inverse operation of unpacking the dataclass (dataclass -> one output per field).

Base `flowrep` documentation:

""" + (fr.tools.workflow.__doc__ or "")


def _attach_pwf_tool(
    flowrep_result: (
        types.FunctionType | Callable[[types.FunctionType], types.FunctionType]
    ),
    tool: type[DecoratedAtomic] | type[DecoratedMacro],
) -> types.FunctionType | Callable[[types.FunctionType], types.FunctionType]:
    if hasattr(flowrep_result, "flowrep_recipe"):
        # Bare form: flowrep already decorated the function.
        decorated = cast(types.FunctionType, flowrep_result)
        setattr(decorated, tool.assign_to, tool(decorated))
        return decorated

    # Parametrized form: flowrep returned a decorator awaiting the function.
    flowrep_decorator = cast(
        Callable[[types.FunctionType], types.FunctionType], flowrep_result
    )

    def decorator(func: types.FunctionType) -> types.FunctionType:
        decorated = flowrep_decorator(func)
        setattr(decorated, tool.assign_to, tool(decorated))
        return decorated

    return decorator
