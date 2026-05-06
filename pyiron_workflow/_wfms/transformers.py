from flowrep.api import schemas as frs
from pyiron_snippets import versions

from pyiron_workflow._wfms import atomic
from pyiron_workflow._wfms.datatypes import Graph


class Transform1toN:
    @staticmethod
    def iterable_to_outputs(items, /):
        return tuple(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> frs.AtomicNode:
        return frs.AtomicNode(
            reference=frs.PythonReference(
                info=versions.VersionInfo.of(self.iterable_to_outputs),
                restricted_input_kinds={
                    "items": frs.RestrictedParamKind.POSITIONAL_ONLY
                },
            ),
            inputs=["items"],
            outputs=[f"output_{i}" for i in range(self.n)],
            unpack_mode=frs.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: frs.Label,
        owner: Graph | None = None,
        history_limit: int = 10,
    ) -> atomic.Atomic:
        return atomic.Atomic(
            label, self.recipe, owner=owner, history_limit=history_limit
        )


class TransformNto1:
    @staticmethod
    def inputs_to_list(*items):
        return list(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> frs.AtomicNode:
        return frs.AtomicNode(
            reference=frs.PythonReference(
                info=versions.VersionInfo.of(self.inputs_to_list),
                restricted_input_kinds={
                    f"item_{i}": frs.RestrictedParamKind.POSITIONAL_ONLY
                    for i in range(self.n)
                },
            ),
            inputs=[f"item_{i}" for i in range(self.n)],
            outputs=["output_0"],
            unpack_mode=frs.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: frs.Label,
        owner: Graph | None = None,
        history_limit: int = 10,
    ) -> atomic.Atomic:
        return atomic.Atomic(
            label, self.recipe, owner=owner, history_limit=history_limit
        )
