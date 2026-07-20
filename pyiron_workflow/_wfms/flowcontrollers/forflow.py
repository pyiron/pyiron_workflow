from __future__ import annotations

import math
from collections.abc import MutableMapping

import flowrep as fr

from pyiron_workflow._wfms import (
    constructors,
    dag,
    execution,
    transformers,
)
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    EdgeTuple,
    Node,
    NodeMap,
    StaticGraph,
)


class ForEach(StaticGraph[fr.schemas.ForEachRecipe, fr.schemas.ForEachData]):
    _recipe: fr.schemas.ForEachRecipe

    @classmethod
    def _result_type(cls) -> type[fr.schemas.ForEachData]:
        return fr.schemas.ForEachData

    def _build_nodes(self, recipe: fr.schemas.ForEachRecipe) -> NodeMap:
        bn = self.recipe.body_node
        return NodeMap(
            self,
            {bn.label: constructors.recipe2node(bn.recipe, bn.label)},
        )

    def _build_edges(self, recipe: fr.schemas.ForEachRecipe) -> EdgeList:
        return EdgeList(
            EdgeTuple(source, target) for target, source in recipe.input_edges.items()
        ) + EdgeList(
            EdgeTuple(source, target) for target, source in recipe.output_edges.items()
        )  # No peer-edges for the for-each loop recipes

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        result = run.result
        nodes = self._build_runtime_dag(run)
        dag.evaluate_dag_by_layer(nodes, run, config)
        dag.populate_outputs(result)
        return run

    def _build_runtime_dag(self, run: execution.Run[fr.schemas.ForEachData]) -> NodeMap:
        runtime_map: dict[fr.schemas.Label, Node] = {}

        result = run.result
        recipe = result.recipe

        body_label = recipe.body_node.label
        inputs = result.input_ports
        body_node = self.nodes[body_label]

        # Map body port names -> parent port names
        nested_label_map = self._body_to_parent_label_map(
            recipe.input_edges, body_label, recipe.nested_ports
        )
        zipped_label_map = self._body_to_parent_label_map(
            recipe.input_edges, body_label, recipe.zipped_ports
        )

        # Map parent port names -> input data length
        nested_length_map = self._input_length_map(
            nested_label_map, inputs, recipe.nested_ports
        )
        zipped_length_map = self._validate_zipped_lengths(
            self._input_length_map(zipped_label_map, inputs, recipe.zipped_ports)
        )
        iterated_length_map = {**nested_length_map, **zipped_length_map}

        # Scatter nodes
        for label, length in iterated_length_map.items():
            result_scatter_label = self._scatter_label(label)
            scatter_node = transformers.Transform1toN(length).node(result_scatter_label)
            runtime_map[result_scatter_label] = scatter_node
            result.nodes[result_scatter_label] = (
                scatter_node.generate_flowrep_live_node()
            )

        total_steps = self._calculate_total_steps(nested_length_map, zipped_length_map)
        # Body nodes
        for i in range(total_steps):
            result_body_label = self._body_label(body_label, i)
            runtime_map[result_body_label] = body_node
            result.nodes[result_body_label] = body_node.generate_flowrep_live_node()

        # Aggregator nodes
        for label in recipe.outputs:
            result_aggregator_label = self._aggregate_label(label)
            aggregator_node = transformers.TransformNto1(total_steps).node(label)
            runtime_map[result_aggregator_label] = aggregator_node
            result.nodes[result_aggregator_label] = (
                aggregator_node.generate_flowrep_live_node()
            )

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
            fr.schemas.TargetHandle(
                node=self._scatter_label(parent_port),
                port=transformers.Transform1toN.input_label,
            ): fr.schemas.InputSource(port=parent_port)
            for child_port, parent_port in nested_label_map.items()
        }
        input_edges.update(
            # parent to zipped
            {
                fr.schemas.TargetHandle(
                    node=self._scatter_label(parent_port),
                    port=transformers.Transform1toN.input_label,
                ): fr.schemas.InputSource(port=parent_port)
                for child_port, parent_port in zipped_label_map.items()
            }
        )
        input_edges.update(
            # broadcast input to bodies
            {
                fr.schemas.TargetHandle(
                    node=self._body_label(body_label, i),
                    port=label,
                ): fr.schemas.InputSource(port=label)
                for label in broadcast_labels
                for i in range(total_steps)
            }
        )
        result.input_edges = input_edges

        edges = {
            # nested scatters to bodies: each nested port advances at its own stride
            fr.schemas.TargetHandle(
                node=self._body_label(body_label, i),
                port=child_port,
            ): fr.schemas.SourceHandle(
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
                fr.schemas.TargetHandle(
                    node=self._body_label(body_label, i),
                    port=child_port,
                ): fr.schemas.SourceHandle(
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
                fr.schemas.TargetHandle(
                    node=self._aggregate_label(parent_port),
                    port=transformers.TransformNto1.input_label(i),
                ): fr.schemas.SourceHandle(
                    node=self._body_label(body_label, i),
                    port=child_port,
                )
                for parent_port, child_port in self._captured_output_label_map(
                    recipe.output_edges, body_label
                ).items()
                for i in range(total_steps)
            }
        )
        transfer_label_map = self._transfer_label_map(recipe.output_edges)
        edges.update(
            # nested scatters passed through to aggregators
            {
                fr.schemas.TargetHandle(
                    node=self._aggregate_label(aggregate_label),
                    port=transformers.TransformNto1.input_label(i),
                ): fr.schemas.SourceHandle(
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
                fr.schemas.TargetHandle(
                    node=self._aggregate_label(aggregate_label),
                    port=transformers.TransformNto1.input_label(i),
                ): fr.schemas.SourceHandle(
                    node=self._scatter_label(scatter_label),
                    port=transformers.Transform1toN.output_label(i % zipped_multiplier),
                )
                for aggregate_label, scatter_label in transfer_label_map.items()
                if scatter_label in zipped_length_map
                for i in range(total_steps)
            }
        )
        result.edges = edges

        output_edges = {
            # aggregators to parent
            fr.schemas.OutputTarget(
                port=label,
            ): fr.schemas.SourceHandle(
                node=self._aggregate_label(label),
                port=transformers.TransformNto1.output_label,
            )
            for label in recipe.outputs
        }
        result.output_edges = output_edges

        return NodeMap(self, runtime_map)

    @staticmethod
    def _body_to_parent_label_map(
        input_edges: fr.schemas.InputEdges,
        body_label: fr.schemas.Label,
        references: fr.schemas.Labels,
    ) -> dict[fr.schemas.Label, fr.schemas.Label]:
        return {
            target.port: source.port
            for (target, source) in input_edges.items()
            if (target.node == body_label and target.port in references)
        }

    @staticmethod
    def _input_length_map(
        label_map: dict[fr.schemas.Label, fr.schemas.Label],
        inputs: MutableMapping[fr.schemas.Label, fr.schemas.InputDataPort],
        iterated_body_ports: fr.schemas.Labels,
    ) -> dict[fr.schemas.Label, int]:
        length_map: dict[fr.schemas.Label, int] = {}
        for body_port_label in iterated_body_ports:
            parent_port_label = label_map[body_port_label]
            length_map[parent_port_label] = len(inputs[parent_port_label].value)
        return length_map

    @staticmethod
    def _validate_zipped_lengths(
        length_map: dict[fr.schemas.Label, int],
    ) -> dict[fr.schemas.Label, int]:
        if len(length_map) > 0:
            expected_length = next(iter(length_map.values()))
            if not all(z == expected_length for z in length_map.values()):
                raise ValueError(
                    f"Expected all zipped ports to have the same length, but got "
                    f"{length_map}."
                )
        return length_map

    @staticmethod
    def _scatter_label(suffix: fr.schemas.Label) -> fr.schemas.Label:
        return f"scatter_{suffix}"

    @staticmethod
    def _body_label(prefix: fr.schemas.Label, n: int) -> fr.schemas.Label:
        return f"{prefix}_{n}"

    @staticmethod
    def _aggregate_label(suffix: fr.schemas.Label) -> fr.schemas.Label:
        return f"aggregate_{suffix}"

    @staticmethod
    def _calculate_total_steps(
        nested_length_map: dict[fr.schemas.Label, int],
        zipped_length_map: dict[fr.schemas.Label, int],
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
        total_steps: int, nested_length_map: dict[fr.schemas.Label, int]
    ) -> dict[fr.schemas.Label, int]:
        """
        Per-port strides for mixed-radix decomposition of the body index.

        Nested ports are outer dimensions in `nested_length_map` insertion order;
        zipped ports occupy the innermost dimension (stride 1) and are not
        represented here.
        """
        strides: dict[fr.schemas.Label, int] = {}
        running = total_steps
        for parent_label, length in nested_length_map.items():
            running //= length
            strides[parent_label] = running
        return strides

    @staticmethod
    def _captured_output_label_map(
        output_edges: fr.schemas.OutputEdges, body_label: fr.schemas.Label
    ) -> dict[fr.schemas.Label, fr.schemas.Label]:
        return {
            target.port: source.port
            for (target, source) in output_edges.items()
            if source.node == body_label
        }

    @staticmethod
    def _transfer_label_map(
        output_edges: fr.schemas.OutputEdges,
    ) -> dict[fr.schemas.Label, fr.schemas.Label]:
        return {
            aggregate.port: scatter.port
            for (aggregate, scatter) in output_edges.items()
            if (isinstance(scatter, fr.schemas.InputSource))
        }
