from __future__ import annotations

import collections
import dataclasses
import functools
import itertools
from collections.abc import Iterable, MutableMapping
from typing import Protocol, TypeAlias

import semantikon
from flowrep.api import schemas as frs

from pyiron_workflow._wfms import dag, execution
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    EdgeTuple,
    Graph,
    InputPort,
    Node,
    NodeMap,
    OutputPort,
    PortMap,
    PortType,
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


class GraphAction(Protocol):
    def inverse(self) -> GraphAction: ...


@dataclasses.dataclass(frozen=True)
class AddNode:
    node: Node

    def inverse(self) -> RemoveNode:
        return RemoveNode(self.node)


@dataclasses.dataclass(frozen=True)
class RemoveNode:
    node: Node

    def inverse(self) -> AddNode:
        return AddNode(self.node)


@dataclasses.dataclass(frozen=True)
class AddEdge:
    edge: EdgeTuple

    def inverse(self) -> RemoveEdge:
        return RemoveEdge(self.edge)


@dataclasses.dataclass(frozen=True)
class RemoveEdge:
    edge: EdgeTuple

    def inverse(self) -> AddEdge:
        return AddEdge(self.edge)


@dataclasses.dataclass(frozen=True)
class RenameNode:
    node: Node
    old_label: frs.Label
    new_label: frs.Label

    def inverse(self) -> RenameNode:
        return RenameNode(self.node, self.new_label, self.old_label)


# ...


GraphDiff: TypeAlias = list[GraphAction]


class Workflow(Node[frs.WorkflowNode, frs.LiveWorkflow], Graph):
    """This is the key mutable one"""

    _inputs: MutablePortMap[InputPort]
    _outputs: MutablePortMap[OutputPort]
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

    @staticmethod
    def _flatten(diffs: Iterable[GraphDiff]) -> GraphDiff:
        return list(itertools.chain.from_iterable(diffs))

    @staticmethod
    def _undoable(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            diff = method(self, *args, **kwargs)
            self.undo_stack.append(diff)
            self.redo_stack.clear()  # branching invalidates redo history
            return diff

        return wrapper

    def get_node(self, node: Node | frs.Label) -> Node:
        if isinstance(node, str):
            return self.nodes[node]
        else:
            owned = self.nodes.get(node.label, None)
            if owned is None or node is not owned:
                raise KeyError(
                    f"Cannot get {node!r} named {node.label!r} -- no such node is owned."
                )
            return node

    def create_input(
        self,
        label: frs.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def remove_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def rename_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def create_output(
        self, label: frs.Label, type_hint: type | None = None
    ) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def remove_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def add_port_hint(
        self, port: InputPort | OutputPort, hint: type | None
    ) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def remove_port_hint(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_hint(port, None)

    @_undoable
    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
    ) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def remove_port_metadata(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_metadata(port, None)

    @_undoable
    def rename_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def add_node(self, *nodes: Node) -> GraphDiff:
        return self._flatten(self._add_node(node) for node in nodes)

    def _add_node(self, node: Node) -> GraphDiff:
        self.nodes[node.label] = node
        return [AddNode(node)]

    @_undoable
    def remove_node(self, *nodes: Node | frs.Label) -> GraphDiff:
        return self._flatten(self._remove_node(self.get_node(node)) for node in nodes)

    def _remove_node(self, node: Node) -> GraphDiff:
        disconnect_diff = self.disconnect(node)
        del self.nodes[node.label]
        remove_diff = [RemoveNode(node)]
        return disconnect_diff + remove_diff

    @_undoable
    def rename_node(self, node: Node, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    @_undoable
    def add_edge(self, *edges: EdgeTuple) -> GraphDiff:
        return self._flatten(self._add_edge(edge) for edge in edges)

    def _add_edge(self, edge: EdgeTuple) -> GraphDiff:
        # TODO: Maybe verify it's not already there? Not costless though
        self.edges.append(edge)
        return [AddEdge(edge)]

    @_undoable
    def remove_edge(self, *edges: EdgeTuple) -> GraphDiff:
        return self._flatten(self._add_edge(edge) for edge in edges)

    def _remove_edge(self, edge: EdgeTuple):
        # TODO: Fail more cleanly
        self.edges.remove(edge)
        return [RemoveEdge(edge)]

    @_undoable
    def disconnect(self, *nodes: Node | frs.Label) -> GraphDiff:
        return self._flatten(self._disconnect(self.get_node(node)) for node in nodes)

    def _disconnect(self, node: Node) -> GraphDiff:
        participating_edges: EdgeList = [
            edge
            for edge in self.edges
            if node.label in (edge.source.node, edge.target.node)
        ]
        return self.remove_edge(*participating_edges)

    @_undoable
    def group(self, *nodes: Node) -> GraphDiff:
        raise NotImplementedError()

    @_undoable  # Lossy on underlying macro function reference, if any
    def ungroup(
        self, graph: dag.Macro | Workflow, block_if_reference: bool = False
    ) -> GraphDiff:
        if (
            block_if_reference
            and isinstance(graph, dag.Macro)
            and graph.recipe.reference is not None
        ):
            raise ValueError(
                f"Cannot ungroup {graph.lexical_path!r} a -- it is a "
                f"{graph.__class__.__name__} with an underlying python reference "
                f"({graph.recipe.reference!r}). Override by setting "
                "`block_if_reference=False`"
            )
        raise NotImplementedError()

    def _undo_diff(self, diff: GraphDiff) -> GraphDiff:
        inverse_diff = [action.inverse() for action in reversed(diff)]
        for action in inverse_diff:
            self._dispatch(action)
        return inverse_diff

    def undo(self, steps: int = 1) -> list[GraphDiff]:
        # Iteratively pop from the undo stack and send to the private _undo_diff
        # Move undone activity onto the redo stack
        raise NotImplementedError()

    def _redo_diff(self, diff: GraphDiff) -> GraphDiff:
        for action in reversed(diff):
            self._dispatch(action)
        return diff

    def redo(self, steps: int = 1) -> list[GraphDiff]:
        # Iteratively pop from the redo stack and send to the private _redo_diff
        # Move redone activity back onto the undo stack
        raise NotImplementedError()

    def _dispatch(self, action: GraphAction) -> None:
        match action:
            case AddNode(node=node):
                self._add_node(node)  # node: Node, fully narrowed
            case RemoveNode(node=node):
                self._remove_node(node)
            case AddEdge(edge=edge):
                self._add_edge(edge)
            # ...
        raise NotImplementedError()
