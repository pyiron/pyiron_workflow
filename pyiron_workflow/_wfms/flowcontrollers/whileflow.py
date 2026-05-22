from __future__ import annotations

from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import (
    NodeMap,
    StaticGraph,
)


class While(StaticGraph[frs.WhileRecipe, frs.WhileData]):
    _recipe: frs.WhileRecipe

    @classmethod
    def _result_type(cls) -> type[frs.WhileData]:
        return frs.WhileData

    def _build_nodes(self, recipe: frs.WhileRecipe) -> NodeMap:
        return NodeMap(
            self,
            {
                recipe.case.condition.label: constructors.recipe2node(
                    recipe.case.condition.label, recipe.case.condition.node, owner=self
                ),
                recipe.case.body.label: constructors.recipe2node(
                    recipe.case.body.label, recipe.case.body.node, owner=self
                ),
            },
        )

    def _build_edges(self, recipe: frs.WhileRecipe) -> frs.Edges:
        """
        Cyclic 'iteration' edges exist from the prospective body back to the
        prospective condition and prospective body inputs. The resulting edges list
        does *not* give a DAG — they describe the loop structure at the recipe level
        for tools (GUIs, visualisations) that want to express the while-loop's
        iteration semantics.
        """
        input_edges = [
            (source, target) for target, source in recipe.input_edges.items()
        ]

        looped_edges = [
            (source, target) for target, source in recipe.body_body_edges.items()
        ] + [(source, target) for target, source in recipe.body_condition_edges.items()]

        body_return_edges = [
            (source, target) for target, source in recipe.output_edges.items()
        ]

        fallback_edges = [
            (frs.InputSource(port=output_label), frs.OutputTarget(port=output_label))
            for output_label in self.outputs
        ]

        return input_edges + looped_edges + body_return_edges + fallback_edges

    @staticmethod
    def _indexed_label(prefix: frs.Label, index: int) -> frs.Label:
        return f"{prefix}_{index}"

    def evaluate(
        self, run: execution.Run[frs.WhileData], config: execution.RunConfig
    ) -> execution.Run[frs.WhileData]:
        recipe = self._recipe
        result = run.result
        cond_prefix = recipe.case.condition.label
        body_prefix = recipe.case.body.label
        # The two pwf children — these are the durable authoring objects
        # that carry executors, fleche policies, etc.
        condition_node = self.nodes[cond_prefix]
        body_node = self.nodes[body_prefix]

        last_body_label: frs.Label | None = None
        iteration = 0
        while True:
            cond_label = self._indexed_label(cond_prefix, iteration)
            self._stage_node(cond_label, result, recipe.case.condition.node)
            self._stage_child_edges(
                cond_label, cond_prefix, recipe, result, last_body_label
            )
            dag.evaluate_node(condition_node, cond_label, run, config)

            if not self._condition_value(cond_label, recipe.case, result):
                break

            body_label = self._indexed_label(body_prefix, iteration)
            self._stage_node(body_label, result, recipe.case.body.node)
            self._stage_child_edges(
                body_label, body_prefix, recipe, result, last_body_label
            )
            dag.evaluate_node(body_node, body_label, run, config)

            last_body_label = body_label
            iteration += 1

        self._stage_final_output_edges(result, recipe, last_body_label)
        dag.populate_outputs(result)
        return run

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
    def _stage_child_edges(
        indexed_label: frs.Label,
        base_label: frs.Label,
        recipe: frs.WhileRecipe,
        result: frs.WhileData,
        last_body_label: frs.Label | None,
    ) -> None:
        output_port_to_body_port = {
            ot.port: src.port for ot, src in recipe.output_edges.items()
        }
        for target, source in recipe.input_edges.items():
            if target.node != base_label:
                continue
            new_target = frs.TargetHandle(node=indexed_label, port=target.port)
            if source.port in output_port_to_body_port and last_body_label is not None:
                body_port = output_port_to_body_port[source.port]
                result.edges[new_target] = frs.SourceHandle(
                    node=last_body_label, port=body_port
                )
            else:
                result.input_edges[new_target] = source

    @staticmethod
    def _stage_final_output_edges(
        result: frs.WhileData,
        recipe: frs.WhileRecipe,
        last_body_label: frs.Label | None,
    ) -> None:
        for target, source in recipe.output_edges.items():
            if last_body_label is None:
                result.output_edges[target] = frs.InputSource(port=target.port)
            else:
                result.output_edges[target] = frs.SourceHandle(
                    node=last_body_label, port=source.port
                )

    @staticmethod
    def _condition_value(
        cond_label: frs.Label, case: frs.ConditionalCase, result: frs.WhileData
    ) -> bool:
        output_label = case.condition_output or next(iter(case.condition.node.outputs))
        return bool(result.nodes[cond_label].output_ports[output_label].value)
