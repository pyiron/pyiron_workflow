from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import FlowControl, NodeMap


class Try(FlowControl[frs.LiveTry]):
    _recipe: frs.TryNode

    @classmethod
    def _result_type(cls) -> type[frs.LiveTry]:
        return frs.LiveTry

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
        self, run: execution.Run[frs.LiveTry], config: execution.RunConfig
    ) -> None:
        raise NotImplementedError()

    def _build_retrospective_input_edges(
        self, run: execution.Run[frs.LiveTry]
    ) -> frs.InputEdges:
        raise NotImplementedError()

    def _build_retrospective_edges(self, run: execution.Run[frs.LiveTry]) -> frs.Edges:
        raise NotImplementedError()

    def _build_retrospective_output_edges(
        self, run: execution.Run[frs.LiveTry]
    ) -> frs.OutputEdges:
        raise NotImplementedError()

    def _build_retrospective_nodes(self, run: execution.Run[frs.LiveTry]) -> NodeMap:
        raise NotImplementedError()
