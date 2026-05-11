from __future__ import annotations

import math
from collections.abc import MutableMapping
from typing import Any

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import atomic, constructors, dag, execution, transformers
from pyiron_workflow._wfms.datatypes import (
    FlowControl,
    Graph,
    NodeMap,
    StaticNode,
)


class ForEach(FlowControl):

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.ForEachNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)

        bn = self.recipe.body_node
        self._prospective_nodes = NodeMap(
            self, constructors.recipe2static(bn.label, bn.node, owner=self)
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
        result = run.result
        nodes: NodeMap
        (
            nodes,
            result.input_edges,
            result.edges,
            result.output_edges,
        ) = self._build_runtime_dag(run)
        dag.evaluate_dag_by_layer(nodes, run, config)

    def _build_retrospective_input_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.InputEdges:
        if self.current_run is None:
            return {}
        return self.current_run.result.input_edges

    def _build_retrospective_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.Edges:
        if self.current_run is None:
            return {}
        return self.current_run.result.edges

    def _build_retrospective_output_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.OutputEdges:
        if self.current_run is None:
            return {}
        return self.current_run.result.output_edges

    def _build_retrospective_nodes(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> NodeMap:
        nodes = []
        for step in run.steps:
            node = constructors.recipe2static(
                step.label, step.run.result.recipe, owner=self
            )
            node.current_run = step.run
            nodes.append(node)
        return NodeMap(self, *nodes)

    def _build_runtime_dag(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> tuple[NodeMap, frs.InputEdges, frs.Edges, frs.OutputEdges]:

        recipe = self._validate_recipe(run.result.recipe)
        body = recipe.body_node
        inputs = run.result.input_ports

        # Map body port names -> parent port names
        nested_label_map = self._body_to_parent_label_map(
            recipe.input_edges, body.label, recipe.nested_ports
        )
        zipped_label_map = self._body_to_parent_label_map(
            recipe.input_edges, body.label, recipe.zipped_ports
        )

        # Map parent port names -> input data length
        nested_length_map = self._input_length_map(
            nested_label_map, inputs, recipe.nested_ports
        )
        zipped_length_map = self._validate_zipped_lengths(
            self._input_length_map(zipped_label_map, inputs, recipe.zipped_ports)
        )
        iterated_length_map = {**nested_length_map, **zipped_length_map}

        scatter_nodes: tuple[atomic.Atomic, ...] = tuple(
            transformers.Transform1toN(length).node(
                self._scatter_label(label), owner=self
            )
            for label, length in iterated_length_map.items()
        )

        total_steps = self._calculate_total_steps(nested_length_map, zipped_length_map)
        body_nodes: tuple[StaticNode[Any], ...] = tuple(
            constructors.recipe2static(
                self._body_label(body.label, i), body.node, owner=self
            )
            for i in range(total_steps)
        )

        aggregate_transformer_map: dict[frs.Label, transformers.TransformNto1] = {
            label: transformers.TransformNto1(total_steps) for label in recipe.outputs
        }
        aggregator_nodes = tuple(
            t.node(self._aggregate_label(label), owner=self)
            for label, t in aggregate_transformer_map.items()
        )

        nodes = NodeMap(self, *(scatter_nodes + body_nodes + aggregator_nodes))

        broadcast_labels = list(
            set(inputs) - set(nested_length_map).union(zipped_length_map)
        )

        # Mixed-radix decomposition: nested ports are outer dims, zipped is innermost.
        nested_strides = self._nested_strides(total_steps, nested_length_map)
        zipped_multiplier = (
            next(iter(zipped_length_map.values())) if zipped_length_map else 1
        )

        input_edges = {
            # parent to nested
            frs.TargetHandle(
                node=self._scatter_label(parent_port),
                port=transformers.Transform1toN.input_label,
            ): frs.InputSource(port=parent_port)
            for child_port, parent_port in nested_label_map.items()
        }
        input_edges.update(
            # parent to zipped
            {
                frs.TargetHandle(
                    node=self._scatter_label(parent_port),
                    port=transformers.Transform1toN.input_label,
                ): frs.InputSource(port=parent_port)
                for child_port, parent_port in zipped_label_map.items()
            }
        )
        input_edges.update(
            # broadcast input to bodies
            {
                frs.TargetHandle(
                    node=self._body_label(body.label, i),
                    port=label,
                ): frs.InputSource(port=label)
                for label in broadcast_labels
                for i in range(total_steps)
            }
        )

        edges = {
            # nested scatters to bodies: each nested port advances at its own stride
            frs.TargetHandle(
                node=self._body_label(body.label, i),
                port=child_port,
            ): frs.SourceHandle(
                node=self._scatter_label(parent_port),
                port=transformers.Transform1toN.output_label(
                    (i // nested_strides[parent_port]) % nested_length_map[parent_port]
                ),
            )
            for child_port, parent_port in nested_label_map.items()
            for i in range(total_steps)
        }
        edges.update(
            # zipped scatters to bodies: all zipped ports share the innermost index
            {
                frs.TargetHandle(
                    node=self._body_label(body.label, i),
                    port=child_port,
                ): frs.SourceHandle(
                    node=self._scatter_label(parent_port),
                    port=transformers.Transform1toN.output_label(i % zipped_multiplier),
                )
                for child_port, parent_port in zipped_label_map.items()
                for i in range(total_steps)
            }
        )
        edges.update(
            {
                # bodies to aggregators (genuinely 1:1)
                frs.TargetHandle(
                    node=self._aggregate_label(parent_port),
                    port=transformers.TransformNto1.input_label(i),
                ): frs.SourceHandle(
                    node=self._body_label(body.label, i),
                    port=child_port,
                )
                for parent_port, child_port in self._captured_output_label_map(
                    recipe.output_edges, body.label
                ).items()
                for i in range(total_steps)
            }
        )
        transfer_label_map = self._transfer_label_map(recipe.output_edges)
        edges.update(
            # nested scatters passed through to aggregators
            {
                frs.TargetHandle(
                    node=self._aggregate_label(aggregate_label),
                    port=transformers.TransformNto1.input_label(i),
                ): frs.SourceHandle(
                    node=self._scatter_label(scatter_label),
                    port=transformers.Transform1toN.output_label(
                        (i // nested_strides[scatter_label])
                        % nested_length_map[scatter_label]
                    ),
                )
                for aggregate_label, scatter_label in transfer_label_map.items()
                if scatter_label in nested_length_map
                for i in range(total_steps)
            }
        )
        edges.update(
            # zipped scatters passed through to aggregators
            {
                frs.TargetHandle(
                    node=self._aggregate_label(aggregate_label),
                    port=transformers.TransformNto1.input_label(i),
                ): frs.SourceHandle(
                    node=self._scatter_label(scatter_label),
                    port=transformers.Transform1toN.output_label(i % zipped_multiplier),
                )
                for aggregate_label, scatter_label in transfer_label_map.items()
                if scatter_label in zipped_length_map
                for i in range(total_steps)
            }
        )

        output_edges = {
            # aggregators to parent
            frs.OutputTarget(
                port=label,
            ): frs.SourceHandle(
                node=self._aggregate_label(label),
                port=transformers.TransformNto1.output_label,
            )
            for label in recipe.outputs
        }

        return (
            nodes,
            input_edges,
            edges,
            output_edges,
        )

    @staticmethod
    def _validate_recipe(recipe: frs.NodeType) -> frs.ForEachNode:
        if not isinstance(recipe, frs.ForEachNode):
            raise TypeError(
                f"Expected a {frs.ForEachNode.__name__!r} recipe, but got "
                f"{recipe!r}"
            )
        return recipe

    @staticmethod
    def _body_to_parent_label_map(
        input_edges: frs.InputEdges, body_label: frs.Label, references: frs.Labels
    ) -> dict[frs.Label, frs.Label]:
        return {
            target.port: source.port
            for (target, source) in input_edges.items()
            if (target.node == body_label and target.port in references)
        }

    @staticmethod
    def _input_length_map(
        label_map: dict[frs.Label, frs.Label],
        inputs: MutableMapping[frs.Label, frs.InputPort],
        iterated_body_ports: frs.Labels,
    ) -> dict[frs.Label, int]:
        length_map: dict[frs.Label, int] = {}
        for body_port_label in iterated_body_ports:
            parent_port_label = label_map[body_port_label]
            length_map[parent_port_label] = len(inputs[parent_port_label].value)
        return length_map

    @staticmethod
    def _validate_zipped_lengths(
        length_map: dict[frs.Label, int],
    ) -> dict[frs.Label, int]:
        if len(length_map) > 0:
            expected_length = next(iter(length_map.values()))
            if not all(z == expected_length for z in length_map.values()):
                raise ValueError(
                    f"Expected all zipped ports to have the same length, but got "
                    f"{length_map}."
                )
        return length_map

    @staticmethod
    def _scatter_label(suffix: frs.Label) -> frs.Label:
        return f"scatter_{suffix}"

    @staticmethod
    def _body_label(prefix: frs.Label, n: int) -> frs.Label:
        return f"{prefix}_{n}"

    @staticmethod
    def _aggregate_label(suffix: frs.Label) -> frs.Label:
        return f"aggregate_{suffix}"

    @staticmethod
    def _calculate_total_steps(
        nested_length_map: dict[frs.Label, int],
        zipped_length_map: dict[frs.Label, int],
    ) -> int:
        nested_multiplier = (
            1 if len(nested_length_map) == 0 else math.prod(nested_length_map.values())
        )
        zipped_multiplier = (
            1 if len(zipped_length_map) == 0 else next(iter(zipped_length_map.values()))
        )
        return nested_multiplier * zipped_multiplier

    @staticmethod
    def _nested_strides(
        total_steps: int, nested_length_map: dict[frs.Label, int]
    ) -> dict[frs.Label, int]:
        """
        Per-port strides for mixed-radix decomposition of the body index.

        Nested ports are outer dimensions in `nested_length_map` insertion order;
        zipped ports occupy the innermost dimension (stride 1) and are not
        represented here.
        """
        strides: dict[frs.Label, int] = {}
        running = total_steps
        for parent_label, length in nested_length_map.items():
            running //= length
            strides[parent_label] = running
        return strides

    @staticmethod
    def _captured_output_label_map(
        output_edges: frs.OutputEdges, body_label: frs.Label
    ) -> dict[frs.Label, frs.Label]:
        return {
            target.port: source.port
            for (target, source) in output_edges.items()
            if source.node == body_label
        }

    @staticmethod
    def _transfer_label_map(
        output_edges: frs.OutputEdges,
    ) -> dict[frs.Label, frs.Label]:
        return {
            aggregate.port: scatter.port
            for (aggregate, scatter) in output_edges.items()
            if (isinstance(scatter, frs.InputSource))
        }
