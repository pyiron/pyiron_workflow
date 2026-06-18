from __future__ import annotations

from concurrent import futures
from typing import TYPE_CHECKING, Any

import flowrep as fr
import semantikon
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import constructors, execution, lexical, validation

if TYPE_CHECKING:
    import rdflib
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    ImmutableDag,
    Node,
    NodeMap,
)


class Macro(ImmutableDag):
    _recipe: fr.schemas.WorkflowRecipe

    @classmethod
    def _result_type(cls) -> type[fr.schemas.DagData]:
        return fr.schemas.DagData

    def _build_nodes(self, recipe: fr.schemas.WorkflowRecipe) -> NodeMap:
        return NodeMap(
            self,
            {
                node_label: constructors.recipe2node(node_recipe, node_label)
                for node_label, node_recipe in recipe.nodes.items()
            },
        )

    def _build_edges(self, recipe: fr.schemas.WorkflowRecipe) -> EdgeList:
        return constructors.edges2edgelist(
            recipe.input_edges, recipe.edges, recipe.output_edges
        )

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        evaluate_dag_by_layer(self.nodes, run, config)
        populate_outputs(run.result)
        return run

    def validate(
        self,
        do_types: bool = True,
        do_ontology: bool = True,
        extra_knowledge: rdflib.Graph | None = None,
    ) -> validation.CombinedValidationReport:
        """Validate this node's types and (optionally) ontology.

        Thin wrapper around :func:`validation.validate_plan`.
        """
        return validation.validate_plan(
            self,
            do_types=do_types,
            do_ontology=do_ontology,
            extra_knowledge=extra_knowledge,
        )

    @property
    def function_metadata(self) -> semantikon.FunctionMetadata | None:
        if reference := self.recipe.reference:
            fqn = reference.info.fully_qualified_name
            function = retrieve.import_from_string(fqn)
            return getattr(function, "_semantikon_metadata", None)
        return None


def evaluate_dag_by_layer(
    nodes: NodeMap,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
) -> None:
    result = run.result
    layers = topo_sort_nodes(nodes, result.edges)

    if config.dag_layers_multithreaded:
        _multithreaded_layers(layers, nodes, run, config)
    else:
        for layer in layers:
            for label in layer:
                evaluate_node(nodes[label], label, run, config)


def _multithreaded_layers(
    layers: list[list[fr.schemas.Label]],
    nodes: NodeMap,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
):
    with futures.ThreadPoolExecutor(
        max_workers=config.dag_layers_max_threads
    ) as executor:
        for layer in layers:
            pending = {
                executor.submit(evaluate_node, nodes[label], label, run, config): label
                for label in layer
            }
            errors: dict[str, Exception] = {}
            for future in futures.as_completed(pending):
                exc = future.exception()
                if exc is None:
                    continue
                if not isinstance(exc, Exception):
                    raise exc  # don't defer KeyboardInterrupt / SystemExit
                if config.dag_layers_fail_fast:
                    raise exc
                errors[pending[future]] = exc
            if errors:
                if len(errors) == 1:
                    raise errors.popitem()[1]
                else:
                    raise ExceptionGroup(
                        f"{len(errors)} node(s) failed in layer", list(errors.values())
                    )


def topo_sort_nodes(
    nodes: NodeMap, edges: fr.schemas.Edges
) -> list[list[fr.schemas.Label]]:
    """
    Kahn's algorithm over sibling edges, grouped into independent layers.

    Each layer contains nodes whose dependencies all live in earlier layers, so
    members of a layer may be executed concurrently. Deterministic tie-breaking
    by label within each layer.
    """
    in_degree: dict[fr.schemas.Label, int] = dict.fromkeys(nodes, 0)
    successors: dict[fr.schemas.Label, list[fr.schemas.Label]] = {
        label: [] for label in nodes
    }

    for target, source in edges.items():
        if target.node not in in_degree or source.node not in successors:
            continue  # Skip edges that cross batch boundaries (e.g. While iterations)
        in_degree[target.node] += 1
        successors[source.node].append(target.node)

    current_layer = sorted(label for label in nodes if in_degree[label] == 0)
    layers: list[list[fr.schemas.Label]] = []
    processed = 0
    while current_layer:
        layers.append(current_layer)
        processed += len(current_layer)
        next_layer: list[str] = []
        for label in current_layer:
            for succ in successors.get(label, []):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    next_layer.append(succ)
        current_layer = sorted(next_layer)

    if processed != len(nodes):  # pragma: no cover
        raise ValueError(
            "Cycle detected in workflow edges. This should have been caught by the "
            "underlying recipe validation. Please raise a GitHub issue reporting "
            "how you got here!"
        )
    return layers


def evaluate_node(
    node: Node[Any, execution.ResultType],
    label_in_run: fr.schemas.Label,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
):
    result = run.result
    input_data = gather_target_inputs(label_in_run, result)
    if any(val is fr.schemas.NOT_DATA for val in input_data.values()):
        # Possible development: raise a warning or optionally an exception here
        return
    sub_run = execution.Run[execution.ResultType](
        lexical_path=lexical.lexical_path(run.lexical_path, label_in_run),
        result=node.generate_flowrep_live_node(),
        status=execution.RunStatus.PENDING,
        progress_dir=config.progress_dir,
    )
    run.steps.append(sub_run)
    result.nodes[label_in_run] = sub_run.result
    execution.run(node, config, sub_run, **input_data)


def gather_target_inputs(
    node_label: fr.schemas.Label,
    runtime_data: fr.schemas.CompositeData,
) -> dict[str, Any]:
    """
    Resolve input values for a target node from graph input ports and sibling
    output ports according to the graph recipe edges.

    Ports not covered by any edge are omitted — the child's own defaults (if any)
    will be used downstream.
    """
    inputs: dict[str, Any] = {}

    try:
        input_names = runtime_data.nodes[node_label].recipe.inputs
    except Exception as e:
        raise e
    for port in input_names:
        th = fr.schemas.TargetHandle(node=node_label, port=port)

        if th in runtime_data.input_edges:
            owner_source = runtime_data.input_edges[th]
            owner_input_port = runtime_data.input_ports[owner_source.port]
            inputs[port] = owner_input_port.get_data()
        elif th in runtime_data.edges:
            sibling_source = runtime_data.edges[th]
            sibling_data = runtime_data.nodes[sibling_source.node]
            sibling_output_port = sibling_data.output_ports[sibling_source.port]
            inputs[port] = sibling_output_port.value
        # else: port has a default on the child, _call_atomic will handle it

    return inputs


def populate_outputs(result: fr.schemas.CompositeData) -> None:
    for target, source in result.output_edges.items():
        if isinstance(source, fr.schemas.InputSource):
            val = result.input_ports[source.port].get_data()
        elif isinstance(source, fr.schemas.SourceHandle):
            child = result.nodes[source.node]
            val = child.output_ports[source.port].value
        else:  # pragma: no cover
            # Just future-proofing any new source types so we fail cleanly
            raise NotImplementedError(f"Unsupported source type {type(source)}")
        result.output_ports[target.port].value = val
