from __future__ import annotations

from flowrep.api import schemas as frs
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import FlowControl, Graph, NodeMap


class Try(FlowControl[frs.LiveTry]):
    _recipe: frs.TryNode

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.TryNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)

        nodes: list = [
            constructors.recipe2static(
                recipe.try_node.label, recipe.try_node.node, owner=self
            )
        ]
        for case in recipe.exception_cases:
            nodes.append(
                constructors.recipe2static(case.body.label, case.body.node, owner=self)
            )
        self._prospective_nodes = NodeMap(self, *nodes)

    @classmethod
    def _result_type(cls) -> type[frs.LiveTry]:
        return frs.LiveTry

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        return self._recipe.input_edges

    @property
    def prospective_edges(self) -> frs.Edges:
        return {}

    @property
    def prospective_output_edges(self) -> frs.ProspectiveOutputEdges:
        return self._recipe.prospective_output_edges

    @property
    def prospective_nodes(self) -> NodeMap:
        return self._prospective_nodes

    def evaluate(
        self, run: execution.Run[frs.LiveTry], config: execution.RunConfig
    ) -> None:
        result = run.result
        recipe = result.recipe

        try_label = recipe.try_node.label
        self._stage_node_input_edges(try_label, result, recipe)
        try_node = constructors.recipe2static(
            try_label, recipe.try_node.node, owner=self
        )

        try:
            dag.evaluate_node(try_node, run, config)
        except BaseException as exc:
            # evaluate_node skips step-bookkeeping when execution.run raises.
            # Record the failed try-body manually so the run is introspectable.
            if try_node.current_run is not None:
                run.steps.append(execution.Step(try_label, try_node.current_run))
                result.nodes[try_label] = try_node.current_run.result

            for case in recipe.exception_cases:
                exc_types = self._resolve_exception_types(case)
                if not isinstance(exc, exc_types):
                    continue

                body_label = case.body.label
                self._stage_node_input_edges(body_label, result, recipe)
                body_node = constructors.recipe2static(
                    body_label, case.body.node, owner=self
                )
                try:
                    dag.evaluate_node(body_node, run, config)
                except BaseException:
                    # Handler itself raised; record the step before propagating.
                    if body_node.current_run is not None:
                        run.steps.append(
                            execution.Step(body_label, body_node.current_run)
                        )
                        result.nodes[body_label] = body_node.current_run.result
                    raise
                self._stage_body_output_edges(body_label, result, recipe)
                dag.populate_outputs(result)
                return

            raise

        self._stage_body_output_edges(try_label, result, recipe)
        dag.populate_outputs(result)

    def _build_retrospective_nodes(self, run: execution.Run[frs.LiveTry]) -> NodeMap:
        nodes = []
        for step in run.steps:
            node = constructors.recipe2static(
                step.label, step.run.result.recipe, owner=self
            )
            node.current_run = step.run
            nodes.append(node)
        return NodeMap(self, *nodes)

    @staticmethod
    def _resolve_exception_types(
        case: frs.ExceptionCase,
    ) -> tuple[type[BaseException], ...]:
        return tuple(
            retrieve.import_from_string(info.fully_qualified_name)
            for info in case.exceptions
        )

    @staticmethod
    def _stage_node_input_edges(
        node_label: frs.Label,
        result: frs.LiveTry,
        recipe: frs.TryNode,
    ) -> None:
        for target, source in recipe.input_edges.items():
            if target.node == node_label:
                result.input_edges[target] = source

    @staticmethod
    def _stage_body_output_edges(
        body_label: frs.Label,
        result: frs.LiveTry,
        recipe: frs.TryNode,
    ) -> None:
        for target, sources in recipe.prospective_output_edges.items():
            for source in sources:
                if source.node == body_label:
                    result.output_edges[target] = source
                    break
