from __future__ import annotations

import flowrep as fr

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    Node,
    NodeMap,
    StaticGraph,
)


class If(StaticGraph[fr.schemas.IfRecipe, fr.schemas.IfData]):
    _recipe: fr.schemas.IfRecipe

    @classmethod
    def _result_type(cls) -> type[fr.schemas.IfData]:
        return fr.schemas.IfData

    def _build_nodes(self, recipe: fr.schemas.IfRecipe) -> NodeMap:
        nodes: dict[fr.schemas.Label, Node] = {}
        for case in recipe.cases:
            nodes[case.condition.label] = constructors.recipe2node(
                case.condition.recipe, case.condition.label
            )
            nodes[case.body.label] = constructors.recipe2node(
                case.body.recipe, case.body.label
            )
        if recipe.else_case is not None:
            nodes[recipe.else_case.label] = constructors.recipe2node(
                recipe.else_case.recipe, recipe.else_case.label
            )
        return NodeMap(self, nodes)

    def _build_edges(self, recipe: fr.schemas.IfRecipe) -> EdgeList:
        return []

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        recipe = self._recipe
        result = run.result

        for case in recipe.cases:
            condition_label = case.condition.label
            self._stage_node(condition_label, result, case.condition.recipe)
            self._stage_node_input_edges(condition_label, result, recipe)
            condition_node = self.nodes[condition_label]
            dag.evaluate_node(condition_node, condition_label, run, config)

            if self._condition_value(case, result):
                body_label = case.body.label
                self._stage_node(body_label, result, case.body.recipe)
                self._stage_node_input_edges(body_label, result, recipe)
                self._stage_body_output_edges(body_label, result, recipe)
                body_node = self.nodes[body_label]
                dag.evaluate_node(body_node, body_label, run, config)
                # Exit early if we've found a truthy conditional branch
                dag.populate_outputs(result)
                return run

        if recipe.else_case is not None:
            else_label = recipe.else_case.label
            self._stage_node(else_label, result, recipe.else_case.recipe)
            self._stage_node_input_edges(else_label, result, recipe)
            self._stage_body_output_edges(else_label, result, recipe)
            else_node = self.nodes[else_label]
            dag.evaluate_node(else_node, else_label, run, config)
        # else: no case fired and no else_case — output ports stay at NOT_DATA.
        dag.populate_outputs(result)
        return run

    @staticmethod
    def _stage_node(
        node_label: fr.schemas.Label,
        result: fr.schemas.IfData,
        node_recipe: (
            fr.schemas.AtomicRecipe
            | fr.schemas.ForEachRecipe
            | fr.schemas.IfRecipe
            | fr.schemas.TryRecipe
            | fr.schemas.WhileRecipe
            | fr.schemas.WorkflowRecipe
        ),
    ) -> None:
        result.nodes[node_label] = fr.tools.recipe2data(node_recipe)

    @staticmethod
    def _stage_node_input_edges(
        node_label: fr.schemas.Label,
        result: fr.schemas.IfData,
        recipe: fr.schemas.IfRecipe,
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
        body_label: fr.schemas.Label,
        result: fr.schemas.IfData,
        recipe: fr.schemas.IfRecipe,
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
    def _condition_value(
        case: fr.schemas.ConditionalCase, result: fr.schemas.IfData
    ) -> bool:
        output_label = case.condition_output
        if output_label is None:
            output_label = next(iter(case.condition.recipe.outputs))
        live_condition = result.nodes[case.condition.label]
        return bool(live_condition.output_ports[output_label].value)
