from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import FlowControl, NodeMap


class While(FlowControl[frs.LiveWhile]):
    _recipe: frs.WhileNode

    @classmethod
    def _result_type(cls) -> type[frs.LiveWhile]:
        return frs.LiveWhile

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
        self, run: execution.Run[frs.LiveWhile], config: execution.RunConfig
    ) -> None:
        raise NotImplementedError()

    def _build_retrospective_input_edges(
        self, run: execution.Run[frs.LiveWhile]
    ) -> frs.InputEdges:
        raise NotImplementedError()

    def _build_retrospective_edges(
        self, run: execution.Run[frs.LiveWhile]
    ) -> frs.Edges:
        raise NotImplementedError()

    def _build_retrospective_output_edges(
        self, run: execution.Run[frs.LiveWhile]
    ) -> frs.OutputEdges:
        raise NotImplementedError()

    def _build_retrospective_nodes(self, run: execution.Run[frs.LiveWhile]) -> NodeMap:
        raise NotImplementedError()
