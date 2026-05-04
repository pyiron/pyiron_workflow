from __future__ import annotations

import collections
import datetime
from typing import cast

from flowrep.api import schemas as frs
from flowrep.api import tools as frt
from semantikon import converter, flowrep_dict
from semantikon import datastructure as sds

from pyiron_workflow._wfms.datatypes import (
    InputPort,
    Node,
    OutputPort,
    PortMap,
    Run,
    RunStatus,
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
        live_preview = self._empty_live_node()
        self._function = live_preview.function
        self._inputs = self._build_inputs(live_preview)
        self._outputs = self._build_outputs(live_preview)
        self.function_metadata = getattr(self._function, "_semantikon_metadata", None)

        self.executor = None
        self.current_run = None
        self.run_history = collections.deque(maxlen=history_limit)

    def _build_inputs(self, live: frs.LiveAtomic) -> PortMap[InputPort, Node]:
        return PortMap[InputPort, Node](
            self,
            *(
                InputPort(
                    label=label,
                    owner=self,
                    type_hint=(
                        flowrep_dict._unwrap_annotated(flowrep_port.annotation)
                        if flowrep_port.annotation is not None
                        else None
                    ),
                    type_metadata=(
                        converter.parse_metadata(flowrep_port.annotation)
                        if flowrep_port.annotation is not None
                        else None
                    ),
                    has_default=label in self._recipe.inputs_with_defaults,
                )
                for label, flowrep_port in live.input_ports.items()
            ),
        )

    def _build_outputs(self, live: frs.LiveAtomic) -> PortMap[OutputPort, Node]:
        return PortMap[OutputPort, Node](
            self,
            *(
                OutputPort(
                    label=label,
                    owner=self,
                    type_hint=(
                        flowrep_dict._unwrap_annotated(flowrep_port.annotation)
                        if flowrep_port.annotation is not None
                        else None
                    ),
                    type_metadata=(
                        converter.parse_metadata(flowrep_port.annotation)
                        if flowrep_port.annotation is not None
                        else None
                    ),
                )
                for label, flowrep_port in live.output_ports.items()
            ),
        )

    @property
    def inputs(self) -> PortMap[InputPort, Node]:
        return self._inputs

    @property
    def outputs(self) -> PortMap[OutputPort, Node]:
        return self._outputs

    @property
    def recipe(self) -> frs.AtomicNode:
        return self._recipe

    def _empty_live_node(self) -> frs.LiveAtomic:
        return frs.LiveAtomic.from_recipe(self.recipe)

    def _evaluate(
        self, _pwf_node_evaluate__run: Run[frs.LiveAtomic], **input_data
    ) -> Run[frs.LiveAtomic]:
        if self.executor is not None:
            raise NotImplementedError()
        else:
            result = frt.run_recipe(self.recipe, **input_data)
        _pwf_node_evaluate__run.result = cast(frs.LiveAtomic, result)
        _pwf_node_evaluate__run.status = RunStatus.FINISHED
        _pwf_node_evaluate__run.finished_at = datetime.datetime.now()
        return _pwf_node_evaluate__run
