from __future__ import annotations

from flowrep import wfms as fr_wfms
from flowrep.api import schemas as frs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import StaticNode


class Atomic(StaticNode[frs.LiveAtomic]):

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.AtomicNode,
        *,
        owner=None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)
        self._function_metadata = getattr(
            self.generate_flowrep_live_node().function, "_semantikon_metadata", None
        )

    @classmethod
    def _result_type(cls) -> type[frs.LiveAtomic]:
        return frs.LiveAtomic

    def evaluate(
        self, run: execution.Run[frs.LiveAtomic], config: execution.RunConfig
    ) -> None:
        output = fr_wfms._call_atomic(run.result)
        fr_wfms._store_atomic_outputs(run.result, output)

    @property
    def function_metadata(self) -> sds.FunctionMetadata | None:
        return self._function_metadata
