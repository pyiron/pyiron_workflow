from __future__ import annotations

import flowrep as fr

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import (
    NodeMap,
    StaticGraph,
)


class While(StaticGraph[fr.schemas.WhileRecipe, fr.schemas.WhileData]):
    _recipe: fr.schemas.WhileRecipe

    @classmethod
    def _result_type(cls) -> type[fr.schemas.WhileData]:
        return fr.schemas.WhileData

    def _build_nodes(self, recipe: fr.schemas.WhileRecipe) -> NodeMap:
        return NodeMap(
            self,
            {
                recipe.case.condition.label: constructors.recipe2node(
                    recipe.case.condition.node, recipe.case.condition.label
                ),
                recipe.case.body.label: constructors.recipe2node(
                    recipe.case.body.node, recipe.case.body.label
                ),
            },
        )

    def _build_edges(self, recipe: fr.schemas.WhileRecipe) -> fr.schemas.Edges:
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
            (
                fr.schemas.InputSource(port=output_label),
                fr.schemas.OutputTarget(port=output_label),
            )
            for output_label in self.outputs
        ]

        return input_edges + looped_edges + body_return_edges + fallback_edges

    @staticmethod
    def _indexed_label(prefix: fr.schemas.Label, index: int) -> fr.schemas.Label:
        return f"{prefix}_{index}"

    def evaluate(
        self, run: execution.Run[fr.schemas.WhileData], config: execution.RunConfig
    ) -> execution.Run[fr.schemas.WhileData]:
        recipe = self._recipe
        result = run.result
        cond_prefix = recipe.case.condition.label
        body_prefix = recipe.case.body.label
        # The two pwf children — these are the durable authoring objects
        # that carry executors, fleche policies, etc.
        condition_node = self.nodes[cond_prefix]
        body_node = self.nodes[body_prefix]

        last_body_label: fr.schemas.Label | None = None
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
    def _stage_child_edges(
        indexed_label: fr.schemas.Label,
        base_label: fr.schemas.Label,
        recipe: fr.schemas.WhileRecipe,
        result: fr.schemas.WhileData,
        last_body_label: fr.schemas.Label | None,
    ) -> None:
        output_port_to_body_port = {
            ot.port: src.port for ot, src in recipe.output_edges.items()
        }
        for target, source in recipe.input_edges.items():
            if target.node != base_label:
                continue
            new_target = fr.schemas.TargetHandle(node=indexed_label, port=target.port)
            if source.port in output_port_to_body_port and last_body_label is not None:
                body_port = output_port_to_body_port[source.port]
                result.edges[new_target] = fr.schemas.SourceHandle(
                    node=last_body_label, port=body_port
                )
            else:
                result.input_edges[new_target] = source

    @staticmethod
    def _stage_final_output_edges(
        result: fr.schemas.WhileData,
        recipe: fr.schemas.WhileRecipe,
        last_body_label: fr.schemas.Label | None,
    ) -> None:
        for target, source in recipe.output_edges.items():
            if last_body_label is None:
                result.output_edges[target] = fr.schemas.InputSource(port=target.port)
            else:
                result.output_edges[target] = fr.schemas.SourceHandle(
                    node=last_body_label, port=source.port
                )

    @staticmethod
    def _condition_value(
        cond_label: fr.schemas.Label,
        case: fr.schemas.ConditionalCase,
        result: fr.schemas.WhileData,
    ) -> bool:
        output_label = case.condition_output or next(iter(case.condition.node.outputs))
        return bool(result.nodes[cond_label].output_ports[output_label].value)
