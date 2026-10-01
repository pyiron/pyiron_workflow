from __future__ import annotations

from concurrent import futures
from typing import TYPE_CHECKING, Any

import flowrep as fr
import semantikon
from pyiron_snippets import retrieve

from pyiron_workflow import constructors, datatypes, execution, lexical, validation

if TYPE_CHECKING:
    from collections.abc import Iterable

    import rdflib


class Macro(datatypes.ImmutableDag):
    _recipe: fr.schemas.WorkflowRecipe

    @classmethod
    def _result_type(cls) -> type[fr.schemas.DagData]:
        return fr.schemas.DagData

    def _build_nodes(self, recipe: fr.schemas.WorkflowRecipe) -> datatypes.NodeMap:
        return datatypes.NodeMap(
            self,
            {
                node_label: constructors.recipe2node(node_recipe, node_label)
                for node_label, node_recipe in recipe.nodes.items()
            },
        )

    def _build_edges(self, recipe: fr.schemas.WorkflowRecipe) -> datatypes.EdgeList:
        return constructors.edges2edgelist(
            recipe.input_edges, recipe.edges, recipe.output_edges
        )

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        evaluate_dag(self.nodes, run, config)
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


def evaluate_dag(
    nodes: datatypes.NodeMap,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
) -> None:
    if config.dag_multithreaded:
        _evaluate_greedily(nodes, run, config)
    else:
        for layer in topo_sort_nodes(nodes, run.result.edges):
            for label in layer:
                evaluate_node(nodes[label], label, run, config)


def _evaluate_greedily(
    nodes: datatypes.NodeMap,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
) -> None:
    """
    Kahn's algorithm driven by thread completion: each node is submitted as soon
    as its last sibling dependency finishes, so chains are not held back by
    unrelated slow siblings. All bookkeeping happens on the calling thread.

    A node that raises never releases its successors. Unless failing fast, every
    branch independent of the failure still runs to completion before the
    collected errors are raised.
    """
    in_degree, successors = _dependency_graph(nodes, run.result.edges)
    errors: list[Exception] = []
    with futures.ThreadPoolExecutor(max_workers=config.dag_max_threads) as executor:
        roots = [label for label, degree in in_degree.items() if degree == 0]
        pending = _submit(executor, roots, nodes, run, config)
        while pending:
            done, _ = futures.wait(pending, return_when=futures.FIRST_COMPLETED)
            ready: list[fr.schemas.Label] = []
            for future in done:
                label = pending.pop(future)
                exc = future.exception()
                if exc is None:
                    ready.extend(_release(label, in_degree, successors))
                elif not isinstance(exc, Exception) or config.dag_fail_fast:
                    raise exc  # don't defer KeyboardInterrupt / SystemExit
                else:
                    errors.append(exc)
            pending.update(_submit(executor, ready, nodes, run, config))
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"{len(errors)} node(s) failed", errors)


def _submit(
    executor: futures.Executor,
    labels: Iterable[fr.schemas.Label],
    nodes: datatypes.NodeMap,
    run: execution.Run[fr.schemas.CompositeData],
    config: execution.RunConfig,
) -> dict[futures.Future[None], fr.schemas.Label]:
    return {
        executor.submit(evaluate_node, nodes[label], label, run, config): label
        for label in sorted(labels)
    }


def _dependency_graph(
    nodes: Iterable[fr.schemas.Label], edges: fr.schemas.Edges
) -> tuple[dict[fr.schemas.Label, int], dict[fr.schemas.Label, list[fr.schemas.Label]]]:
    """In-degree and successor lists over sibling edges.

    A target fed several ports by the same source counts each edge, and appears
    that many times among the source's successors, so the counts stay balanced.
    """
    in_degree: dict[fr.schemas.Label, int] = dict.fromkeys(nodes, 0)
    successors: dict[fr.schemas.Label, list[fr.schemas.Label]] = {
        label: [] for label in in_degree
    }
    for target, source in edges.items():
        if target.node not in in_degree or source.node not in successors:
            continue  # Skip edges that cross batch boundaries (e.g. While iterations)
        in_degree[target.node] += 1
        successors[source.node].append(target.node)
    return in_degree, successors


def _release(
    label: fr.schemas.Label,
    in_degree: dict[fr.schemas.Label, int],
    successors: dict[fr.schemas.Label, list[fr.schemas.Label]],
) -> list[fr.schemas.Label]:
    """Mark `label` finished; return the successors that just became ready."""
    released = []
    for successor in successors[label]:
        in_degree[successor] -= 1
        if in_degree[successor] == 0:
            released.append(successor)
    return released


def topo_sort_nodes(
    nodes: datatypes.NodeMap, edges: fr.schemas.Edges
) -> list[list[fr.schemas.Label]]:
    """
    Kahn's algorithm over sibling edges, grouped into independent layers.

    Each layer contains nodes whose dependencies all live in earlier layers.
    Deterministic tie-breaking by label within each layer.
    """
    in_degree, successors = _dependency_graph(nodes, edges)
    current_layer = sorted(label for label, degree in in_degree.items() if degree == 0)
    layers: list[list[fr.schemas.Label]] = []
    processed = 0
    while current_layer:
        layers.append(current_layer)
        processed += len(current_layer)
        current_layer = sorted(
            successor
            for label in current_layer
            for successor in _release(label, in_degree, successors)
        )

    if processed != len(in_degree):  # pragma: no cover
        raise ValueError(
            "Cycle detected in workflow edges. This should have been caught by the "
            "underlying recipe validation. Please raise a GitHub issue reporting "
            "how you got here!"
        )
    return layers


def evaluate_node(
    node: datatypes.Node[Any, execution.ResultType],
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
        run_dir=config.run_dir,
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
