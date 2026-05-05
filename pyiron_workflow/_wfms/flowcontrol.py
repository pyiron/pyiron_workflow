from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import FlowControl, Node, NodeMap


class ForEach(FlowControl):  # Not implemented
    body_node: Node

    @property
    def input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    @property
    def nodes(self) -> NodeMap:
        raise NotImplementedError()

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def prospective_edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    def evaluate(self, run: execution.Run[frs.LiveWorkflow]) -> None:
        raise NotImplementedError()


class If(FlowControl):
    @property
    def input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    @property
    def nodes(self) -> NodeMap:
        raise NotImplementedError()

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def prospective_edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    def evaluate(self, run: execution.Run[frs.LiveWorkflow]) -> None:
        raise NotImplementedError()


class Try(FlowControl):
    @property
    def input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    @property
    def nodes(self) -> NodeMap:
        raise NotImplementedError()

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def prospective_edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    def evaluate(self, run: execution.Run[frs.LiveWorkflow]) -> None:
        raise NotImplementedError()


class While(FlowControl):
    @property
    def input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    @property
    def nodes(self) -> NodeMap:
        raise NotImplementedError()

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        raise NotImplementedError()

    @property
    def prospective_edges(self) -> frs.Edges:
        raise NotImplementedError()

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        raise NotImplementedError()

    def evaluate(self, run: execution.Run[frs.LiveWorkflow]) -> None:
        raise NotImplementedError()
