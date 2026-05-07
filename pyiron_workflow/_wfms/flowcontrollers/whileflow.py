from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import FlowControl, NodeMap


class While(FlowControl):

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def prospective_edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    @property
    def prospective_nodes(self) -> NodeMap:
        raise NotImplementedError()

    def evaluate(
        self, run: execution.Run[frs.LiveWorkflow], config: execution.RunConfig
    ) -> None:
        raise NotImplementedError()

    def _build_retrospective_input_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.InputEdges:
        raise NotImplementedError()

    def _build_retrospective_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.Edges:
        raise NotImplementedError()

    def _build_retrospective_output_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.OutputEdges:
        raise NotImplementedError()

    def _build_retrospective_nodes(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> NodeMap:
        raise NotImplementedError()
