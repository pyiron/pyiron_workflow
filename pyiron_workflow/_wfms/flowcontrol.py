from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import dag, execution
from pyiron_workflow._wfms.datatypes import FlowControl, Graph, NodeMap


class ForEach(FlowControl):

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.WorkflowNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)

        bn = self.recipe.body_node
        self._prospective_nodes = NodeMap(
            self, dag.recipe2static(bn.label, bn.node, owner=self)
        )

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        return self.recipe.input_edges

    @property
    def prospective_edges(self) -> frs.Edges:
        return {}

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        return self.recipe.output_edges

    @property
    def prospective_nodes(self) -> NodeMap:
        return self._prospective_nodes

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
        node_map = NodeMap(self)
        for step in run.steps:
            node = dag.recipe2static(step.label, step.run.result.recipe, owner=self)
            node.current_run = step.run
            node_map[step.label] = node
        return node_map


class If(FlowControl):
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


class Try(FlowControl):
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
