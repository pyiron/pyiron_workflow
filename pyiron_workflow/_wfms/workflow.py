from __future__ import annotations

import collections
import dataclasses
import functools
from collections.abc import Callable, MutableMapping
from typing import Any, Protocol, TypeAlias

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


class MutableNodeMap(NodeMap, MutableMapping[frs.Label, Node]):
    _pwf_lexical_map__owner: Workflow

    def __setitem__(self, key: frs.Label, value: Node):
        if value.owner is not None and value.owner is not self._pwf_lexical_map__owner:
            raise ValueError(
                f"Node {key!r} already has owner {value.owner.lexical_path!r} and "
                f"cannot be assigned to a node map (owner "
                f"{self._pwf_lexical_map__owner.lexical_path!r})."
            )
        if key != value.label:
            raise ValueError(
                f"Node {key!r} already has label {value.label!r} and cannot be assigned "
                f"to a node map with label {value.label!r}."
            )
        value.owner = self._pwf_lexical_map__owner
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: frs.Label):
        value = self._pwf_lexical_map__data[key]
        value.owner = None
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: frs.Label, value: Node):
        """Syntactic sugar for adding fresh nodes to the graph"""
        value.label = key  # Rely on Node.label setter protection for ownership
        self._pwf_lexical_map__owner.add_node(value)


class GraphAction(Protocol):
    def inverse(self) -> GraphAction: ...


@dataclasses.dataclass(frozen=True)
class AddInput:
    port: InputPort

    def inverse(self) -> RemoveInput:
        return RemoveInput(self.port)


@dataclasses.dataclass(frozen=True)
class RemoveInput:
    port: InputPort

    def inverse(self) -> AddInput:
        return AddInput(self.port)


@dataclasses.dataclass(frozen=True)
class AddOutput:
    port: OutputPort

    def inverse(self) -> RemoveOutput:
        return RemoveOutput(self.port)


@dataclasses.dataclass(frozen=True)
class RemoveOutput:
    port: OutputPort

    def inverse(self) -> AddOutput:
        return AddOutput(self.port)


@dataclasses.dataclass(frozen=True)
class ReplacePort:
    old_port: InputPort | OutputPort
    new_port: InputPort | OutputPort

    def inverse(self) -> ReplacePort:
        return ReplacePort(self.new_port, self.old_port)


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


GraphDiff: TypeAlias = list[GraphAction]


class Workflow(Node[frs.WorkflowNode, frs.LiveWorkflow], Graph):
    """This is the key mutable one"""

    _inputs: MutablePortMap[InputPort]
    _outputs: MutablePortMap[OutputPort]
    _nodes: MutableNodeMap
    _edges: EdgeList
    _diff_accumulator: GraphDiff | None
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
        self._inputs = MutablePortMap[InputPort](self)
        self._outputs = MutablePortMap[OutputPort](self)
        self._nodes = MutableNodeMap(self)
        self._edges: EdgeList = []
        self._diff_accumulator: GraphDiff | None = None
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
    def _records(method: Callable[..., Any]) -> Callable[..., Any]:
        """
        Marks a private leaf mutation. Returns the action and appends it to
        the active diff accumulator (if any).
        """

        @functools.wraps(method)
        def wrapper(self: Workflow, *args: Any, **kwargs: Any) -> Any:
            action = method(self, *args, **kwargs)
            if self._diff_accumulator is not None:
                self._diff_accumulator.append(action)
            return action

        return wrapper

    @staticmethod
    def _undoable(method: Callable[..., Any]) -> Callable[..., Any]:
        """
        Marks a public top-level operation. On success, pushes the accumulated
        diff onto undo_stack and clears redo_stack. On exception, dispatches the
        inverse of every accumulated action (in reverse order) before re-raising.
        """

        @functools.wraps(method)
        def wrapper(self: Workflow, *args: Any, **kwargs: Any) -> Any:
            if self._diff_accumulator is not None:
                return method(self, *args, **kwargs)  # nested: parent commits
            accumulator: GraphDiff = []
            self._diff_accumulator = accumulator
            try:
                method(self, *args, **kwargs)
            except Exception:
                for action in reversed(accumulator):
                    self._dispatch(action.inverse())
                raise
            finally:
                self._diff_accumulator = None
            self.undo_stack.append(accumulator)
            self.redo_stack.clear()
            return accumulator

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

    def get_input(self, port: InputPort | frs.Label) -> InputPort:
        if isinstance(port, str):
            return self.inputs[port]
        else:
            owned = self.inputs.get(port.label, None)
            if owned is None or port is not owned:
                raise KeyError(
                    f"Cannot get {port!r} named {port.label!r} -- no such input port is owned."
                )
            return port

    def get_output(self, port: OutputPort | frs.Label) -> OutputPort:
        if isinstance(port, str):
            return self.outputs[port]
        else:
            owned = self.outputs.get(port.label, None)
            if owned is None or port is not owned:
                raise KeyError(
                    f"Cannot get {port!r} named {port.label!r} -- no such output port is owned."
                )
            return port

    # --- Leaf private mutations (each returns one GraphAction) ---

    @_records
    def _add_node(self, node: Node) -> AddNode:
        self.nodes[node.label] = node
        return AddNode(node)

    @_records
    def _remove_node_shallow(self, node: Node) -> RemoveNode:
        del self.nodes[node.label]
        return RemoveNode(node)

    @_records
    def _add_edge(self, edge: EdgeTuple) -> AddEdge:
        self.edges.append(edge)
        return AddEdge(edge)

    @_records
    def _remove_edge(self, edge: EdgeTuple) -> RemoveEdge:
        self.edges.remove(edge)
        return RemoveEdge(edge)

    @_records
    def _add_input(self, port: InputPort) -> AddInput:
        self.inputs[port.label] = port
        return AddInput(port)

    @_records
    def _remove_input_shallow(self, port: InputPort) -> RemoveInput:
        del self.inputs[port.label]
        return RemoveInput(port)

    @_records
    def _add_output(self, port: OutputPort) -> AddOutput:
        self.outputs[port.label] = port
        return AddOutput(port)

    @_records
    def _remove_output_shallow(self, port: OutputPort) -> RemoveOutput:
        del self.outputs[port.label]
        return RemoveOutput(port)

    @_records
    def _replace_port(
        self, old: InputPort | OutputPort, new: InputPort | OutputPort
    ) -> ReplacePort:
        if old.label in self.inputs and self.inputs[old.label] is old:
            target_map: MutablePortMap[InputPort] | MutablePortMap[OutputPort] = (
                self.inputs
            )
        elif old.label in self.outputs and self.outputs[old.label] is old:
            target_map = self.outputs
        else:
            raise KeyError(
                f"Port {old.label!r} is not owned by this workflow's inputs or outputs"
            )
        del target_map[old.label]
        target_map[new.label] = new  # type: ignore[assignment]
        return ReplacePort(old, new)

    @_records
    def _rename_node_label(self, node: Node, new_label: frs.Label) -> RenameNode:
        old_label = node.label
        # Bypass MutableNodeMap.__setitem__ (which rejects relabelling owned nodes)
        del self.nodes[old_label]
        node._label = new_label  # type: ignore[misc]
        self.nodes[new_label] = node
        return RenameNode(node, old_label, new_label)

    # --- Composite private helpers (orchestrate leaves, not themselves decorated) ---

    def _edges_touching_node(self, label: frs.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if (isinstance(e.source, frs.SourceHandle) and e.source.node == label)
            or (isinstance(e.target, frs.TargetHandle) and e.target.node == label)
        ]

    def _edges_using_input(self, label: frs.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if isinstance(e.source, frs.InputSource) and e.source.port == label
        ]

    def _edges_using_output(self, label: frs.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if isinstance(e.target, frs.OutputTarget) and e.target.port == label
        ]

    def _disconnect(self, node: Node) -> None:
        for edge in self._edges_touching_node(node.label):
            self._remove_edge(edge)

    # --- Public mutation methods ---

    @_undoable
    def create_input(
        self,
        label: frs.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> None:
        self._add_input(
            InputPort(
                label=label,
                owner=self,
                type_hint=type_hint,
                type_metadata=type_metadata,
            )
        )

    @_undoable
    def remove_input(self, port: InputPort | frs.Label) -> None:
        resolved = self.get_input(port)
        for edge in self._edges_using_input(resolved.label):
            self._remove_edge(edge)
        self._remove_input_shallow(resolved)

    @_undoable
    def rename_input(self, port: InputPort | frs.Label, new_label: frs.Label) -> None:
        resolved = self.get_input(port)
        old_label = resolved.label
        new_port = dataclasses.replace(resolved, label=new_label)
        for edge in self._edges_using_input(old_label):
            rewritten = EdgeTuple(frs.InputSource(port=new_label), edge.target)
            self._remove_edge(edge)
            self._add_edge(rewritten)
        self._replace_port(resolved, new_port)

    @_undoable
    def create_output(
        self,
        label: frs.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> None:
        self._add_output(
            OutputPort(
                label=label,
                owner=self,
                type_hint=type_hint,
                type_metadata=type_metadata,
            )
        )

    @_undoable
    def remove_output(self, port: OutputPort | frs.Label) -> None:
        resolved = self.get_output(port)
        for edge in self._edges_using_output(resolved.label):
            self._remove_edge(edge)
        self._remove_output_shallow(resolved)

    @_undoable
    def rename_output(self, port: OutputPort | frs.Label, new_label: frs.Label) -> None:
        resolved = self.get_output(port)
        old_label = resolved.label
        new_port = dataclasses.replace(resolved, label=new_label)
        for edge in self._edges_using_output(old_label):
            rewritten = EdgeTuple(edge.source, frs.OutputTarget(port=new_label))
            self._remove_edge(edge)
            self._add_edge(rewritten)
        self._replace_port(resolved, new_port)

    @_undoable
    def add_port_hint(self, port: InputPort | OutputPort, hint: type | None) -> None:
        new_port = dataclasses.replace(port, type_hint=hint)
        self._replace_port(port, new_port)

    def remove_port_hint(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_hint(port, None)  # type: ignore[return-value]

    @_undoable
    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
    ) -> None:
        new_port = dataclasses.replace(port, type_metadata=metadata)
        self._replace_port(port, new_port)

    def remove_port_metadata(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_metadata(port, None)  # type: ignore[return-value]

    @_undoable
    def add_node(self, *nodes: Node) -> None:
        for n in nodes:
            self._add_node(n)

    @_undoable
    def remove_node(self, *nodes: Node | frs.Label) -> None:
        for n in nodes:
            resolved = self.get_node(n)
            self._disconnect(resolved)
            self._remove_node_shallow(resolved)

    @_undoable
    def rename_node(self, node: Node | frs.Label, new_label: frs.Label) -> None:
        resolved = self.get_node(node)
        old_label = resolved.label
        for edge in self._edges_touching_node(old_label):
            self._remove_edge(edge)
            self._add_edge(
                self._rewrite_edge_for_node_rename(edge, old_label, new_label)
            )
        self._rename_node_label(resolved, new_label)

    @staticmethod
    def _rewrite_edge_for_node_rename(
        edge: EdgeTuple, old_label: frs.Label, new_label: frs.Label
    ) -> EdgeTuple:
        source = edge.source
        target = edge.target
        if isinstance(source, frs.SourceHandle) and source.node == old_label:
            source = frs.SourceHandle(node=new_label, port=source.port)
        if isinstance(target, frs.TargetHandle) and target.node == old_label:
            target = frs.TargetHandle(node=new_label, port=target.port)
        return EdgeTuple(source, target)

    @_undoable
    def add_edge(self, *edges: EdgeTuple) -> None:
        for e in edges:
            self._add_edge(e)

    @_undoable
    def remove_edge(self, *edges: EdgeTuple) -> None:
        for e in edges:
            self._remove_edge(e)

    @_undoable
    def disconnect(self, *nodes: Node | frs.Label) -> None:
        for n in nodes:
            self._disconnect(self.get_node(n))

    @_undoable
    def group(self, *nodes: Node) -> None:
        raise NotImplementedError()

    @_undoable  # Lossy on underlying macro function reference, if any
    def ungroup(
        self, graph: dag.Macro | Workflow, block_if_reference: bool = False
    ) -> None:
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

    # --- Undo / redo ---

    def _undo_diff(self, diff: GraphDiff) -> GraphDiff:
        inverse = [action.inverse() for action in reversed(diff)]
        for action in inverse:
            self._dispatch(action)
        return inverse

    def undo(self, steps: int = 1) -> list[GraphDiff]:
        undone = []
        for _ in range(steps):
            if not self.undo_stack:
                break
            diff = self.undo_stack.pop()
            inverse = self._undo_diff(diff)
            self.redo_stack.append(diff)
            undone.append(inverse)
        return undone

    def _redo_diff(self, diff: GraphDiff) -> GraphDiff:
        for action in diff:
            self._dispatch(action)
        return diff

    def redo(self, steps: int = 1) -> list[GraphDiff]:
        redone = []
        for _ in range(steps):
            if not self.redo_stack:
                break
            diff = self.redo_stack.pop()
            self._redo_diff(diff)
            self.undo_stack.append(diff)
            redone.append(diff)
        return redone

    def _dispatch(self, action: GraphAction) -> None:
        match action:
            case AddNode(node=node):
                self._add_node(node)
            case RemoveNode(node=node):
                self._remove_node_shallow(node)
            case AddEdge(edge=edge):
                self._add_edge(edge)
            case RemoveEdge(edge=edge):
                self._remove_edge(edge)
            case AddInput(port=port):
                self._add_input(port)
            case RemoveInput(port=port):
                self._remove_input_shallow(port)
            case AddOutput(port=port):
                self._add_output(port)
            case RemoveOutput(port=port):
                self._remove_output_shallow(port)
            case ReplacePort(old_port=o, new_port=n):
                self._replace_port(o, n)
            case RenameNode(node=n, new_label=label):
                self._rename_node_label(n, label)
            case _:
                raise TypeError(f"Unknown {GraphAction.__name__}: {action!r}")
