from __future__ import annotations

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import constructors, dag, execution
from pyiron_workflow._wfms.datatypes import FlowControl, Graph, NodeMap


class While(FlowControl[frs.LiveWhile]):
    _recipe: frs.WhileNode

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.WhileNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)
        self._prospective_nodes = NodeMap(
            self,
            constructors.recipe2static(
                recipe.case.condition.label, recipe.case.condition.node, owner=self
            ),
            constructors.recipe2static(
                recipe.case.body.label, recipe.case.body.node, owner=self
            ),
        )

    @classmethod
    def _result_type(cls) -> type[frs.LiveWhile]:
        return frs.LiveWhile

    @staticmethod
    def _indexed_label(prefix: frs.Label, index: int) -> frs.Label:
        return f"{prefix}_{index}"

    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        return self._recipe.input_edges

    @property
    def prospective_edges(self) -> frs.Edges:
        """
        Cyclic 'iteration' edges from the prospective body back to the prospective
        condition and prospective body inputs. These are *not* DAG edges and are never
        executed — they describe the loop structure at the recipe level for tools
        (GUIs, visualisations) that want to render the while-loop's iteration semantics.
        """
        return {**self._recipe.body_body_edges, **self._recipe.body_condition_edges}

    @property
    def prospective_output_edges(self) -> frs.OutputEdges:
        return self._recipe.output_edges

    @property
    def prospective_nodes(self) -> NodeMap:
        return self._prospective_nodes

    def evaluate(
        self, run: execution.Run[frs.LiveWhile], config: execution.RunConfig
    ) -> None:
        recipe = self._recipe
        result = run.result
        cond_prefix = recipe.case.condition.label
        body_prefix = recipe.case.body.label
        cond_recipe = recipe.case.condition.node
        body_recipe = recipe.case.body.node

        last_body_label: frs.Label | None = None
        iteration = 0

        while True:
            cond_label = self._indexed_label(cond_prefix, iteration)
            self._stage_child_edges(
                cond_label, cond_prefix, recipe, result, last_body_label
            )
            cond_node = constructors.recipe2static(cond_label, cond_recipe, owner=self)
            dag.evaluate_node(cond_node, run, config)

            if not self._condition_value(cond_label, recipe.case, result):
                break

            body_label = self._indexed_label(body_prefix, iteration)
            self._stage_child_edges(
                body_label, body_prefix, recipe, result, last_body_label
            )
            body_node = constructors.recipe2static(body_label, body_recipe, owner=self)
            dag.evaluate_node(body_node, run, config)

            last_body_label = body_label
            iteration += 1

        self._stage_final_output_edges(result, recipe, last_body_label)
        dag.populate_outputs(result)

    def _build_retrospective_nodes(self, run: execution.Run[frs.LiveWhile]) -> NodeMap:
        nodes = []
        for step in run.steps:
            node = constructors.recipe2static(
                step.label, step.run.result.recipe, owner=self
            )
            node.current_run = step.run
            nodes.append(node)
        return NodeMap(self, *nodes)

    @staticmethod
    def _stage_child_edges(
        indexed_label: frs.Label,
        base_label: frs.Label,
        recipe: frs.WhileNode,
        result: frs.LiveWhile,
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
        result: frs.LiveWhile,
        recipe: frs.WhileNode,
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
        cond_label: frs.Label, case: frs.ConditionalCase, result: frs.LiveWhile
    ) -> bool:
        output_label = case.condition_output or next(iter(case.condition.node.outputs))
        return bool(result.nodes[cond_label].output_ports[output_label].value)
