from __future__ import annotations

import collections

from flowrep.api import schemas as frs
from flowrep.wfms import _call_atomic, _store_atomic_outputs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import execution, helpers
from pyiron_workflow._wfms.datatypes import (
    InputPort,
    Node,
    OutputPort,
    PortMap,
)


class Atomic(Node[frs.LiveAtomic]):
    function_metadata: sds.FunctionMetadata | None

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.AtomicNode,
        *,
        history_limit: int = 10,
    ):
        self._label = label  # TODO: also accept None and use function name for default
        self._owner = None
        self._recipe = recipe
        live_preview = self.generate_flowrep_live_node()
        self._function = live_preview.function
        self._inputs = helpers.build_inputs(self, live_preview)
        self._outputs = helpers.build_outputs(self, live_preview)
        self.function_metadata = getattr(self._function, "_semantikon_metadata", None)

        self.executor = None
        self.current_run = None
        self.run_history = collections.deque(maxlen=history_limit)

    @property
    def inputs(self) -> PortMap[InputPort, Node]:
        return self._inputs

    @property
    def outputs(self) -> PortMap[OutputPort, Node]:
        return self._outputs

    @property
    def recipe(self) -> frs.AtomicNode:
        return self._recipe

    def generate_flowrep_live_node(self) -> frs.LiveAtomic:
        return frs.LiveAtomic.from_recipe(self.recipe)

    def evaluate(self, run: execution.Run[frs.LiveAtomic]) -> None:
        output = _call_atomic(run.result)
        _store_atomic_outputs(run.result, output)
