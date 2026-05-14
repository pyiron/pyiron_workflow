from __future__ import annotations

import dataclasses
from typing import Any

import semantikon
from flowrep.api import schemas as frs
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import StaticNode


class Atomic(StaticNode[frs.AtomicNode, frs.LiveAtomic]):

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.AtomicNode,
        *,
        owner=None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)
        func = retrieve.import_from_string(recipe.fully_qualified_name)
        self._function_metadata = getattr(func, "_semantikon_metadata", None)

    @classmethod
    def _result_type(cls) -> type[frs.LiveAtomic]:
        return frs.LiveAtomic

    def evaluate(
        self, run: execution.Run[frs.LiveAtomic], config: execution.RunConfig
    ) -> None:
        output = _call_atomic(run.result)
        _store_atomic_outputs(run.result, output)

    @property
    def function_metadata(self) -> semantikon.FunctionMetadata | None:
        return self._function_metadata


def _call_atomic(node: frs.LiveAtomic) -> Any:
    """
    Invoke the underlying function, respecting positional-only parameter kinds.

    Values are drawn from the live input ports; if a port has no value, its
    default is used.  A :class:`ValueError` is raised when neither is available.
    """
    recipe = node.recipe

    positional: list[Any] = []
    keyword: dict[str, Any] = {}

    for name in recipe.inputs:
        port = node.input_ports[name]
        val = port.value if not isinstance(port.value, frs.NotData) else port.default
        if isinstance(val, frs.NotData):
            raise ValueError(f"Input port '{name}' has no value and no default")

        kind = recipe.reference.restricted_input_kinds.get(name)
        if kind == frs.RestrictedParamKind.POSITIONAL_ONLY:
            positional.append(val)
        else:
            keyword[name] = val

    return node.function(*positional, **keyword)


def _store_atomic_outputs(node: frs.LiveAtomic, result: Any) -> None:
    recipe = node.recipe
    output_names = list(node.output_ports.keys())

    if recipe.unpack_mode == frs.UnpackMode.NONE:
        node.output_ports[output_names[0]].value = result

    elif recipe.unpack_mode == frs.UnpackMode.TUPLE:
        if len(output_names) == 1:
            node.output_ports[output_names[0]].value = result
        else:
            for name, val in zip(output_names, result, strict=True):
                node.output_ports[name].value = val

    elif recipe.unpack_mode == frs.UnpackMode.DATACLASS:
        fields = dataclasses.fields(result)
        for label, field in zip(node.recipe.outputs, fields, strict=True):
            node.output_ports[label].value = getattr(result, field.name)
