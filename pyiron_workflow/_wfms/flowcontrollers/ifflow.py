from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import (
    FlowControl,
    Graph,
    NodeMap,
    ProspectiveOutputEdges,
)


class If(FlowControl[frs.LiveIf]):
    _recipe: frs.IfNode

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.IfNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)

        nodes: list = []
        for case in recipe.cases:
            nodes.append(
                constructors.recipe2static(
                    case.condition.label, case.condition.node, owner=self
                )
            )
            nodes.append(
                constructors.recipe2static(case.body.label, case.body.node, owner=self)
            )
        if recipe.else_case is not None:
            nodes.append(
                constructors.recipe2static(
                    recipe.else_case.label, recipe.else_case.node, owner=self
                )
            )
        self._prospective_nodes = NodeMap(self, *nodes)

    @classmethod
    def _result_type(cls) -> type[frs.LiveIf]:
        return frs.LiveIf

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        return self._recipe.input_edges

    @property
    def prospective_edges(self) -> frs.Edges:
        return {}

    @property
    def prospective_output_edges(self) -> ProspectiveOutputEdges:
        return self._recipe.prospective_output_edges

    @property
    def prospective_nodes(self) -> NodeMap:
        return self._prospective_nodes

    def evaluate(
        self, run: execution.Run[frs.LiveIf], config: execution.RunConfig
    ) -> None:
        recipe = self._recipe
        result = run.result

        for case in recipe.cases:
            self._stage_node_input_edges(case.condition.label, result, recipe)
            cond_node = constructors.recipe2static(
                case.condition.label, case.condition.node, owner=self
            )
            dag.evaluate_node(cond_node, run, config)

            if self._condition_value(case, result):
                body_label = case.body.label
                self._stage_node_input_edges(body_label, result, recipe)
                self._stage_body_output_edges(body_label, result, recipe)
                body_node = constructors.recipe2static(
                    body_label, case.body.node, owner=self
                )
                dag.evaluate_node(body_node, run, config)
                # Exit early if we've found a truthy conditional branch
                dag.populate_outputs(result)
                return

        if recipe.else_case is not None:
            else_label = recipe.else_case.label
            self._stage_node_input_edges(else_label, result, recipe)
            self._stage_body_output_edges(else_label, result, recipe)
            else_node = constructors.recipe2static(
                else_label, recipe.else_case.node, owner=self
            )
            dag.evaluate_node(else_node, run, config)
        # else: no case fired and no else_case — output ports stay at NOT_DATA.
        dag.populate_outputs(result)

    def _build_retrospective_nodes(self, run: execution.Run[frs.LiveIf]) -> NodeMap:
        nodes = []
        for step in run.steps:
            node = constructors.recipe2static(
                step.label, step.run.result.recipe, owner=self
            )
            node.current_run = step.run
            nodes.append(node)
        return NodeMap(self, *nodes)

    @staticmethod
    def _stage_node_input_edges(
        node_label: frs.Label,
        result: frs.LiveIf,
        recipe: frs.IfNode,
    ) -> None:
        """
        Copy `recipe.input_edges` entries targeting `node_label` onto `result`.

        The runtime DAG of an :class:`If` is grown one stage at a time. Each stage
        depends on the parent input ports being visible to the next-evaluated
        condition or body, which :func:`dag.gather_target_inputs` resolves via
        `result.input_edges`.
        """
        for target, source in recipe.input_edges.items():
            if target.node == node_label:
                result.input_edges[target] = source

    @staticmethod
    def _stage_body_output_edges(
        body_label: frs.Label,
        result: frs.LiveIf,
        recipe: frs.IfNode,
    ) -> None:
        """
        Project `recipe.prospective_output_edges` onto a single body's sources.

        `prospective_output_edges` lists, per output target, every body (or
        `else_case`) that could supply that output. The unique source whose
        `node` equals `body_label` is the one actualized for this run.
        Output targets without a matching source for this body are skipped —
        their ports stay at `NOT_DATA`.
        """
        for target, sources in recipe.prospective_output_edges.items():
            for source in sources:
                if source.node == body_label:
                    result.output_edges[target] = source
                    break

    @staticmethod
    def _condition_value(case: frs.ConditionalCase, result: frs.LiveIf) -> bool:
        output_label = case.condition_output
        if output_label is None:
            output_label = next(iter(case.condition.node.outputs))
        live_condition = result.nodes[case.condition.label]
        return bool(live_condition.output_ports[output_label].value)
