from __future__ import annotations

import collections
import datetime
from collections.abc import MutableMapping
from typing import Any, TypeAlias

import semantikon
from flowrep.api import schemas as frs
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import constructors, execution, lexical
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    Graph,
    InputPort,
    Node,
    NodeMap,
    OutputPort,
    PortMap,
    PortType,
    StaticGraph,
)


class MutablePortMap(
    PortMap[PortType, "Workflow"], MutableMapping[frs.Label, PortType]
):
    def __setitem__(self, key: frs.Label, value: PortType):
        owner = self._pwf_lexical_map__owner
        if value.owner is not owner:
            raise ValueError(
                f"Port {key!r} already has owner {value.owner.lexical_path!r} and cannot "
                f"be assigned to a port map with owner {owner.lexical_path!r}"
            )
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: frs.Label):
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: frs.Label, value: PortType):
        self.__setitem__(key, value)


class MutableNodeMap(NodeMap, MutableMapping[frs.Label, Node]):
    def __setitem__(self, key: frs.Label, value: Node):
        if value.owner is not None and value.owner is not self._pwf_lexical_map__owner:
            raise ValueError(
                f"Node {key!r} already has owner {value.owner.lexical_path!r} and "
                f"cannot be assigned to a node map (owner "
                f"{self._pwf_lexical_map__owner.lexical_path!r})."
            )
        value.owner = self._pwf_lexical_map__owner
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: frs.Label):
        value = self._pwf_lexical_map__data[key]
        value.owner = None
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: frs.Label, value: Node):
        self.__setitem__(key, value)


GraphAction: TypeAlias = tuple  # TODO, but probably needs an enum for action type
GraphDiff: TypeAlias = list[GraphAction]


class Workflow(Node[frs.WorkflowNode, frs.LiveWorkflow], Graph):
    """This is the key mutable one"""

    undo_stack: collections.deque[GraphDiff]
    redo_stack: collections.deque[GraphDiff]

    def __init__(
        self,
        label: frs.Label,
        *,
        owner: Graph | None = None,
        undo_limit: int = 10,
    ):
        # Add a super call later if needed
        self._label = label
        self._owner = owner
        self._detached_root = None
        self.executor = None
        self.current_run = None
        self._nodes = MutableNodeMap(self)
        self._edges: EdgeList = []
        self._inputs = MutablePortMap[InputPort](self)
        self._outputs = MutablePortMap[OutputPort](self)
        self.undo_stack = collections.deque(maxlen=undo_limit)
        self.redo_stack = collections.deque(maxlen=undo_limit)

    @property
    def inputs(self) -> MutablePortMap[InputPort]:
        return self._inputs

    @property
    def outputs(self) -> MutablePortMap[OutputPort]:
        return self._outputs

    @property
    def recipe(self) -> frs.WorkflowNode:
        inp, peer, out = self._decompose_edges()
        return frs.WorkflowNode(
            inputs=list(self.inputs.keys()),
            outputs=list(self.outputs.keys()),
            nodes={label: node.recipe for label, node in self.nodes.items()},
            input_edges=inp,
            edges=peer,
            output_edges=out,
        )

    def generate_flowrep_live_node(self) -> frs.LiveWorkflow:
        return frs.LiveWorkflow.from_recipe(self.recipe)

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        raise NotImplementedError()

    @property
    def nodes(self) -> MutableNodeMap:
        return self._nodes

    @property
    def edges(self) -> EdgeList:
        return self._edges

    def _decompose_edges(self) -> tuple[frs.InputEdges, frs.Edges, frs.OutputEdges]:
        inp: frs.InputEdges = {}
        peer: frs.Edges = {}
        out: frs.OutputEdges = {}
        for source, target in self.edges:
            if isinstance(source, frs.InputSource) and isinstance(
                target, frs.TargetHandle
            ):
                inp[target] = source
            elif isinstance(source, frs.SourceHandle) and isinstance(
                target, frs.TargetHandle
            ):
                peer[target] = source
            elif isinstance(source, frs.SourceHandle | frs.InputSource) and isinstance(
                target, frs.OutputTarget
            ):
                out[target] = source
            else:
                raise TypeError(
                    f"{self.lexical_path!r} has an edge that does not fit into known "
                    f"input/peer/output buckets: {source!r} -> {target!r}"
                )
        return inp, peer, out

    @property
    def undo_limit(self) -> int | None:
        return self.undo_stack.maxlen

    @undo_limit.setter
    def undo_limit(self, value: int) -> None:
        self.undo_stack = collections.deque(self.undo_stack, maxlen=value)
        self.redo_stack = collections.deque(self.redo_stack, maxlen=value)

    def to_locked_macro(self) -> Macro:
        raise NotImplementedError()

    def create_input(
        self,
        label: frs.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def rename_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def create_output(
        self, label: frs.Label, type_hint: type | None = None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def add_port_hint(
        self, port: InputPort | OutputPort, hint: type | None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_port_hint(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_hint(port, None)

    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_port_metadata(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_metadata(port, None)

    def rename_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def add_node(self, *nodes: Node) -> GraphDiff:
        raise NotImplementedError()

    def remove_node(self, *nodes: Node) -> GraphDiff:
        raise NotImplementedError()

    def rename_node(self, node: Node, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def connect(self, *edges) -> GraphDiff:
        raise NotImplementedError()

    def disconnect(self, *edges) -> GraphDiff:
        raise NotImplementedError()

    def group(self, *nodes) -> GraphDiff:
        raise NotImplementedError()

    def ungroup(self, *nodes) -> GraphDiff:
        raise NotImplementedError()

    def _undo_diff(self, diff: GraphDiff) -> GraphDiff:
        # For user-facing undo, and for rolling back failed change attempts
        raise NotImplementedError()

    def undo(self, steps: int = 1) -> list[GraphDiff]:
        # Iteratively pop from the undo stack and send to the private _undo_diff
        raise NotImplementedError()

    def _redo_diff(self, diff: GraphDiff) -> GraphDiff:
        raise NotImplementedError()

    def redo(self, steps: int = 1) -> list[GraphDiff]:
        raise NotImplementedError()


class Macro(StaticGraph[frs.WorkflowNode, frs.LiveWorkflow], Graph):
    _recipe: frs.WorkflowNode

    @classmethod
    def _result_type(cls) -> type[frs.LiveWorkflow]:
        return frs.LiveWorkflow

    def _build_nodes(self, recipe: frs.WorkflowNode) -> NodeMap:
        return NodeMap(
            self,
            *(
                constructors.recipe2static(node_label, node_recipe, owner=self)
                for node_label, node_recipe in recipe.nodes.items()
            ),
        )

    def _build_edges(self, recipe: frs.WorkflowNode) -> EdgeList:
        return (
            [(source, target) for target, source in recipe.input_edges.items()]
            + [(source, target) for target, source in recipe.edges.items()]
            + [(source, target) for target, source in recipe.output_edges.items()]
        )

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        evaluate_dag_by_layer(self.nodes, run, config)
        populate_outputs(run.result)
        return run

    @property
    def function_metadata(self) -> semantikon.FunctionMetadata | None:
        if reference := self.recipe.reference:
            fqn = reference.info.fully_qualified_name
            function = retrieve.import_from_string(fqn)
            return getattr(function, "_semantikon_metadata", None)
        return None

    def to_unlocked_workflow(self) -> Workflow:
        raise NotImplementedError()


def evaluate_dag_by_layer(
    nodes: NodeMap, run: execution.Run[frs.Composite], config: execution.RunConfig
) -> None:
    result = run.result
    layers = topo_sort_nodes(nodes, result.edges)

    for layer in layers:
        # TODO: Optionally multithread inside a given layer
        for label in layer:
            # TODO: Try evaluation and collect any exceptions to optionally fail late
            evaluate_node(nodes[label], label, run, config)


def topo_sort_nodes(nodes: NodeMap, edges: frs.Edges) -> list[list[frs.Label]]:
    """
    Kahn's algorithm over sibling edges, grouped into independent layers.

    Each layer contains nodes whose dependencies all live in earlier layers, so
    members of a layer may be executed concurrently. Deterministic tie-breaking
    by label within each layer.
    """
    in_degree: dict[frs.Label, int] = dict.fromkeys(nodes, 0)
    successors: dict[frs.Label, list[frs.Label]] = {label: [] for label in nodes}

    for target, source in edges.items():
        if target.node not in in_degree or source.node not in successors:
            continue  # Skip edges that cross batch boundaries (e.g. While iterations)
        in_degree[target.node] += 1
        successors[source.node].append(target.node)

    current_layer = sorted(label for label in nodes if in_degree[label] == 0)
    layers: list[list[frs.Label]] = []
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
    node: Node[Any, Any],
    label_in_run: frs.Label,
    run: execution.Run[frs.Composite],
    config: execution.RunConfig,
):
    result = run.result
    input_data = gather_target_inputs(label_in_run, result)
    if frs.NOT_DATA in input_data.values():
        # Possible development: raise a warning or optionally an exception here
        return
    t_start = datetime.datetime.now()
    try:
        sub_run = execution.run(
            node, config, run.lexical_path, label_in_run, **input_data
        )
    except Exception as e:
        sub_run = execution.Run(
            lexical_path=lexical.lexical_path(run.lexical_path, label_in_run),
            result=node.generate_flowrep_live_node(),
            status=execution.RunStatus.FAILED,
            exception=e,
            started_at=run.started_at,
            finished_at=t_start,
            progress_dir=config.progress_dir,
        )
        raise e
    finally:
        run.steps.append(sub_run)
        result.nodes[label_in_run] = sub_run.result


def gather_target_inputs(
    node_label: frs.Label,
    runtime_data: frs.Composite,
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
        th = frs.TargetHandle(node=node_label, port=port)

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


def populate_outputs(result: frs.Composite) -> None:
    for target, source in result.output_edges.items():
        if isinstance(source, frs.InputSource):
            val = result.input_ports[source.port].get_data()
        elif isinstance(source, frs.SourceHandle):
            child = result.nodes[source.node]
            val = child.output_ports[source.port].value
        else:  # pragma: no cover
            # Just future-proofing any new source types so we fail cleanly
            raise NotImplementedError(f"Unsupported source type {type(source)}")
        result.output_ports[target.port].value = val
