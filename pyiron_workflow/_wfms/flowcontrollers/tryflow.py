from __future__ import annotations

from flowrep.api import schemas as frs
from flowrep.api import tools as frt
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    Node,
    NodeMap,
    StaticGraph,
)


class UnmatchedExceptionError(TypeError): ...


class Try(StaticGraph[frs.TryRecipe, frs.TryData]):
    _recipe: frs.TryRecipe

    @classmethod
    def _result_type(cls) -> type[frs.TryData]:
        return frs.TryData

    def _build_nodes(self, recipe: frs.TryRecipe) -> NodeMap:
        nodes: dict[frs.Label, Node] = {
            recipe.try_node.label: constructors.recipe2node(
                recipe.try_node.node, recipe.try_node.label
            )
        }
        for case in recipe.exception_cases:
            nodes[case.body.label] = constructors.recipe2node(
                case.body.node, case.body.label
            )
        return NodeMap(self, nodes)

    def _build_edges(self, recipe: frs.TryRecipe) -> EdgeList:
        return []

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        result = run.result
        recipe = result.recipe

        try_label = recipe.try_node.label
        self._stage_node(try_label, result, recipe.try_node.node)
        self._stage_node_input_edges(try_label, result, recipe)
        try_node = self.nodes[try_label]

        try:
            dag.evaluate_node(try_node, try_label, run, config)
        except BaseException as exc:
            for case in recipe.exception_cases:
                exc_types = self._resolve_exception_types(case)
                if not isinstance(exc, exc_types):
                    continue

                body_label = case.body.label
                self._stage_node(body_label, result, case.body.node)
                self._stage_node_input_edges(body_label, result, recipe)
                body_node = self.nodes[body_label]
                try:
                    dag.evaluate_node(body_node, body_label, run, config)
                except BaseException:
                    # Handler itself raised!
                    raise
                self._stage_body_output_edges(body_label, result, recipe)
                dag.populate_outputs(result)
                return run
            raise UnmatchedExceptionError(
                f"Expected an exception among "
                f"{[e.qualname for case in recipe.exception_cases for e in case.exceptions]} "
                f"while evaluating {run.lexical_path!r}, but got a {type(exc).__name__!r} "
                "instead."
            ) from exc

        self._stage_body_output_edges(try_label, result, recipe)
        dag.populate_outputs(result)
        return run

    @staticmethod
    def _resolve_exception_types(
        case: frs.ExceptionCase,
    ) -> tuple[type[BaseException], ...]:
        return tuple(
            retrieve.import_from_string(info.fully_qualified_name)
            for info in case.exceptions
        )

    @staticmethod
    def _stage_node(
        node_label: frs.Label,
        result: frs.IfData,
        node_recipe: (
            frs.AtomicRecipe
            | frs.ForEachRecipe
            | frs.IfRecipe
            | frs.TryRecipe
            | frs.WhileRecipe
            | frs.WorkflowRecipe
        ),
    ) -> None:
        result.nodes[node_label] = frt.recipe2data(node_recipe)

    @staticmethod
    def _stage_node_input_edges(
        node_label: frs.Label,
        result: frs.TryData,
        recipe: frs.TryRecipe,
    ) -> None:
        for target, source in recipe.input_edges.items():
            if target.node == node_label:
                result.input_edges[target] = source

    @staticmethod
    def _stage_body_output_edges(
        body_label: frs.Label,
        result: frs.TryData,
        recipe: frs.TryRecipe,
    ) -> None:
        for target, sources in recipe.prospective_output_edges.items():
            for source in sources:
                if source.node == body_label:
                    result.output_edges[target] = source
                    break
