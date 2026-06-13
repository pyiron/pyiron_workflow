from __future__ import annotations

import collections
import dataclasses
import functools
import types
from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, Any

import flowrep as fr
import semantikon
from semantikon.metadata import Missing

from pyiron_workflow._wfms import actions, constructors, dag, execution, validation
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    EdgeTuple,
    Graph,
    InputPort,
    MutableDag,
    Node,
    NodeMap,
    OutputPort,
    Port,
    PortMap,
    PortType,
    coerce_to_port,
    source_port_to_handle,
    target_port_to_handle,
)

if TYPE_CHECKING:
    import rdflib


def _duplicate_node_error(owner: Graph, key: fr.schemas.Label) -> ValueError:
    return ValueError(
        f"{owner.lexical_path!r} already has a node {key!r}; remove or rename "
        f"it before assigning a new one."
    )


def is_nodelike(value: object) -> bool:
    """Whether `value` is convertible to a node."""
    return isinstance(value, Node | constructors.RecipeOptions | types.FunctionType)


class MutablePortMap(
    PortMap[PortType, "Workflow"], MutableMapping[fr.schemas.Label, PortType]
):
    def __setitem__(self, key: fr.schemas.Label, value: PortType):
        owner = self._pwf_lexical_map__owner
        if value.owner is not owner:
            raise ValueError(
                f"Port {key!r} already has owner {value.owner.lexical_path!r} and cannot "
                f"be assigned to a port map with owner {owner.lexical_path!r}"
            )
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: fr.schemas.Label):
        del self._pwf_lexical_map__data[key]


class MutableNodeMap(NodeMap, MutableMapping[fr.schemas.Label, Node]):
    _pwf_lexical_map__owner: Workflow

    def __setitem__(self, key: fr.schemas.Label, value: Node):
        if key in self._pwf_lexical_map__data:
            raise _duplicate_node_error(self._pwf_lexical_map__owner, key)
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
        value._owner = self._pwf_lexical_map__owner
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: fr.schemas.Label):
        value = self._pwf_lexical_map__data[key]
        value._owner = None
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: fr.schemas.Label, value: object) -> None:
        """
        Syntactic sugar for adding a fresh node to the graph.

        Assigning a node-like value to a public attribute name converts it to
        a `Node`, labels it with the attribute name, and adds it to the owning
        workflow. Internal slot assignments (the `_pwf_lexical_map__*` names,
        e.g. when unpickling) bypass the sugar and set the attribute directly.
        """
        if key.startswith("_pwf_lexical_map"):
            object.__setattr__(self, key, value)
            return
        if key in self._pwf_lexical_map__data:
            raise _duplicate_node_error(self._pwf_lexical_map__owner, key)
        self._pwf_lexical_map__owner.add_node(constructors.node(value, key))


class Workflow(MutableDag):
    """
    This is the key mutable one.

    Workflow inputs must not hold default values, as they have no underlying python
    reference, and so this execution-impacting data would truly live as part of the
    WfMS Workflow node state. However, their ports are free to hold type and metadata
    annotations; these impact validation (directly a WfMS concern), but have no impact
    on the actual execution of the python dataflow, and so are both useful and safe at
    this level.
    """

    _inputs: MutablePortMap[InputPort]
    _outputs: MutablePortMap[OutputPort]
    _nodes: MutableNodeMap
    _edges: EdgeList
    _diff_accumulator: actions.GraphDiff | None
    undo_stack: collections.deque[actions.GraphDiff]
    redo_stack: collections.deque[actions.GraphDiff]

    @classmethod
    def from_recipe(
        cls, label: fr.schemas.Label, recipe: fr.schemas.WorkflowRecipe
    ) -> Workflow:
        wf = cls(label)
        flowrep_data = fr.tools.recipe2data(recipe)

        for input_label in recipe.inputs:
            annotation = flowrep_data.input_ports[input_label].annotation
            if annotation is not None:
                hint = semantikon.annotation_to_type_hint(annotation)
                metadata = semantikon.annotation_to_type_metadata(annotation)
            else:
                hint, metadata = None, None
            wf._add_input(
                InputPort(
                    label=input_label, owner=wf, type_hint=hint, type_metadata=metadata
                )
            )
        for output_label in recipe.outputs:
            annotation = flowrep_data.output_ports[output_label].annotation
            if annotation is not None:
                hint = semantikon.annotation_to_type_hint(annotation)
                metadata = semantikon.annotation_to_type_metadata(annotation)
            else:
                hint, metadata = None, None
            wf._add_output(
                OutputPort(
                    label=output_label, owner=wf, type_hint=hint, type_metadata=metadata
                )
            )
        for child_label, child_recipe in recipe.nodes.items():
            wf._add_node(constructors.node(child_recipe, child_label))
        for edge in constructors.edges2edgelist(
            recipe.input_edges, recipe.edges, recipe.output_edges
        ):
            wf._add_edge(edge)

        return wf

    def __init__(
        self,
        label: fr.schemas.Label,
        undo_limit: int = 10,
        /,
        *positional_connections: Port | Node,
        **keyword_connections: Port | Node,
    ):
        # Add a super call later if needed
        self._label = label
        self._owner = None
        self._detached_root = None
        self._pending_connections = {}
        self.executor = None
        self.last_run = None
        self._inputs = MutablePortMap[InputPort](self)
        self._outputs = MutablePortMap[OutputPort](self)
        self._nodes = MutableNodeMap(self)
        self._edges: EdgeList = []
        self._diff_accumulator: actions.GraphDiff | None = None
        self.undo_stack = collections.deque(maxlen=undo_limit)
        self.redo_stack = collections.deque(maxlen=undo_limit)
        self.connect_input(*positional_connections, **keyword_connections)

    def __setattr__(self, name: str, value: object) -> None:
        """Syntactic sugar for adding a fresh node to the graph.

        Assigning a node-like value to a public attribute name converts it to
        a `Node`, labels it with the attribute name, and adds it via
        `add_node` (so the assignment is undoable). Private (`_`-prefixed)
        names and non-node-like values are assigned normally. A name that
        collides with a real attribute, or a label already taken by a node,
        is rejected.
        """
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if not is_nodelike(value):
            object.__setattr__(self, name, value)
            return
        if hasattr(type(self), name) or name in self.__dict__:
            raise AttributeError(
                f"Cannot assign a node to {name!r} on {self.lexical_path!r}: it "
                f"collides with an existing attribute. Use a different label, or "
                f"add the node via `nodes[{name!r}] = ...`."
            )
        if name in self.nodes:
            raise _duplicate_node_error(self, name)
        self.add_node(constructors.node(value, name))

    @property
    def inputs(self) -> MutablePortMap[InputPort]:
        return self._inputs

    @property
    def outputs(self) -> MutablePortMap[OutputPort]:
        return self._outputs

    @property
    def recipe(self) -> fr.schemas.WorkflowRecipe:
        inp, peer, out = constructors.edgelist2edges(
            self.edges, f"{self.lexical_path!r}"
        )
        return fr.schemas.WorkflowRecipe(
            inputs=list(self.inputs.keys()),
            outputs=list(self.outputs.keys()),
            nodes={label: node.recipe for label, node in self.nodes.items()},
            input_edges=inp,
            edges=peer,
            output_edges=out,
        )

    def generate_flowrep_live_node(self) -> fr.schemas.DagData:
        data = fr.schemas.DagData.from_recipe(self.recipe)
        for label, input_port in self.inputs.items():
            self._update_data_port_metadata(input_port, data.input_ports[label])
        for label, output_port in self.outputs.items():
            self._update_data_port_metadata(output_port, data.output_ports[label])
        return data

    @staticmethod
    def _update_data_port_metadata(
        pwf_port: InputPort | OutputPort,
        flowrep_port: fr.schemas.InputDataPort | fr.schemas.OutputDataPort,
    ) -> None:
        if pwf_port.type_hint is not None:
            if pwf_port.type_metadata is not None:
                annotation = semantikon.u(
                    pwf_port.type_hint,
                    **{
                        k: v
                        for k, v in dataclasses.asdict(pwf_port.type_metadata).items()
                        if not isinstance(v, Missing)
                    },
                )
            else:
                annotation = pwf_port.type_hint
            flowrep_port.annotation = annotation

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        dag.evaluate_dag_by_layer(self.nodes, run, config)
        dag.populate_outputs(run.result)
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
    def nodes(self) -> MutableNodeMap:
        return self._nodes

    @property
    def edges(self) -> EdgeList:
        return self._edges

    @property
    def undo_limit(self) -> int | None:
        return self.undo_stack.maxlen

    @undo_limit.setter
    def undo_limit(self, value: int | None) -> None:
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
            accumulator: actions.GraphDiff = []
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

    # --- Leaf private mutations (each returns one actions.GraphAction) ---

    @_records
    def _add_node(self, node: Node) -> actions.AddNode:
        self.nodes[node.label] = node
        return actions.AddNode(node)

    @_records
    def _remove_node_shallow(self, node: Node) -> actions.RemoveNode:
        del self.nodes[node.label]
        return actions.RemoveNode(node)

    @_records
    def _add_edge(self, edge: EdgeTuple) -> actions.AddEdge:
        self.edges.append(edge)
        return actions.AddEdge(edge)

    @_records
    def _remove_edge(self, edge: EdgeTuple) -> actions.RemoveEdge:
        self.edges.remove(edge)
        return actions.RemoveEdge(edge)

    @_records
    def _add_input(self, port: InputPort) -> actions.AddInput:
        self.inputs[port.label] = port
        return actions.AddInput(port)

    @_records
    def _remove_input_shallow(self, port: InputPort) -> actions.RemoveInput:
        del self.inputs[port.label]
        return actions.RemoveInput(port)

    @_records
    def _add_output(self, port: OutputPort) -> actions.AddOutput:
        self.outputs[port.label] = port
        return actions.AddOutput(port)

    @_records
    def _remove_output_shallow(self, port: OutputPort) -> actions.RemoveOutput:
        del self.outputs[port.label]
        return actions.RemoveOutput(port)

    @_records
    def _replace_port(
        self, old: InputPort | OutputPort, new: InputPort | OutputPort
    ) -> actions.ReplacePort:
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
        return actions.ReplacePort(old, new)

    @_records
    def _rename_node_label(
        self, node: Node, new_label: fr.schemas.Label
    ) -> actions.RenameNode:
        old_label = node.label
        # Bypass MutableNodeMap.__setitem__ (which rejects relabelling owned nodes)
        del self.nodes[old_label]
        node._label = new_label  # type: ignore[misc]
        self.nodes[new_label] = node
        return actions.RenameNode(node, old_label, new_label)

    @_records
    def _move_node(
        self,
        node: Node,
        from_graph: Workflow,
        to_graph: Workflow,
        new_label: fr.schemas.Label | None = None,
    ) -> actions.MoveNode:
        """Move `node` from `from_graph` to `to_graph`, optionally renaming it.

        Bare slot manipulation is used because the public node-map setters
        validate ownership transitions; here we explicitly know the move is
        intentional and atomic from the perspective of the diff accumulator.
        """
        old_label = node.label
        target_label = new_label if new_label is not None else old_label
        del from_graph._nodes._pwf_lexical_map__data[old_label]
        node._label = target_label  # type: ignore[misc]
        node._owner = to_graph
        to_graph._nodes._pwf_lexical_map__data[target_label] = node
        return actions.MoveNode(
            node=node,
            from_graph=from_graph,
            to_graph=to_graph,
            old_label=old_label,
            new_label=target_label,
        )

    # --- Composite private helpers (orchestrate leaves, not themselves decorated) ---

    def _edges_touching_node(self, label: fr.schemas.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if (
                isinstance(e.source, fr.schemas.SourceHandle) and e.source.node == label
            )
            or (
                isinstance(e.target, fr.schemas.TargetHandle) and e.target.node == label
            )
        ]

    def _edges_using_input(self, label: fr.schemas.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if isinstance(e.source, fr.schemas.InputSource) and e.source.port == label
        ]

    def _edges_using_output(self, label: fr.schemas.Label) -> EdgeList:
        return [
            e
            for e in self.edges
            if isinstance(e.target, fr.schemas.OutputTarget) and e.target.port == label
        ]

    def _disconnect(self, node: Node) -> None:
        for edge in self._edges_touching_node(node.label):
            self._remove_edge(edge)

    # --- Public mutation methods ---

    @_undoable
    def create_input(
        self,
        label: fr.schemas.Label,
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
    def remove_input(self, port: InputPort | fr.schemas.Label) -> None:
        resolved = self.get_input(port)
        for edge in self._edges_using_input(resolved.label):
            self._remove_edge(edge)
        self._remove_input_shallow(resolved)

    @_undoable
    def rename_input(
        self, port: InputPort | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None:
        resolved = self.get_input(port)
        old_label = resolved.label
        new_port = dataclasses.replace(resolved, label=new_label)
        for edge in self._edges_using_input(old_label):
            rewritten = EdgeTuple(fr.schemas.InputSource(port=new_label), edge.target)
            self._remove_edge(edge)
            self._add_edge(rewritten)
        self._replace_port(resolved, new_port)

    @_undoable
    def create_output(
        self,
        label: fr.schemas.Label,
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
    def remove_output(self, port: OutputPort | fr.schemas.Label) -> None:
        resolved = self.get_output(port)
        for edge in self._edges_using_output(resolved.label):
            self._remove_edge(edge)
        self._remove_output_shallow(resolved)

    @_undoable
    def rename_output(
        self, port: OutputPort | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None:
        resolved = self.get_output(port)
        old_label = resolved.label
        new_port = dataclasses.replace(resolved, label=new_label)
        for edge in self._edges_using_output(old_label):
            rewritten = EdgeTuple(edge.source, fr.schemas.OutputTarget(port=new_label))
            self._remove_edge(edge)
            self._add_edge(rewritten)
        self._replace_port(resolved, new_port)

    @_undoable
    def add_port_hint(self, port: InputPort | OutputPort, hint: type | None) -> None:
        new_port = dataclasses.replace(port, type_hint=hint)
        self._replace_port(port, new_port)

    def remove_port_hint(self, port: InputPort | OutputPort) -> None:
        return self.add_port_hint(port, None)

    @_undoable
    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
    ) -> None:
        new_port = dataclasses.replace(port, type_metadata=metadata)
        self._replace_port(port, new_port)

    def remove_port_metadata(self, port: InputPort | OutputPort) -> None:
        return self.add_port_metadata(port, None)

    @_undoable
    def add_node(self, *nodes: Node) -> None:
        for n in nodes:
            self._add_node(n)
            for edge in n.use_pending_edges():
                self._add_edge(edge)

    @_undoable
    def remove_node(self, *nodes: Node | fr.schemas.Label) -> None:
        for n in nodes:
            resolved = self.get_node(n)
            self._disconnect(resolved)
            self._remove_node_shallow(resolved)

    @_undoable
    def rename_node(
        self, node: Node | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None:
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
        edge: EdgeTuple, old_label: fr.schemas.Label, new_label: fr.schemas.Label
    ) -> EdgeTuple:
        source = edge.source
        target = edge.target
        if isinstance(source, fr.schemas.SourceHandle) and source.node == old_label:
            source = fr.schemas.SourceHandle(node=new_label, port=source.port)
        if isinstance(target, fr.schemas.TargetHandle) and target.node == old_label:
            target = fr.schemas.TargetHandle(node=new_label, port=target.port)
        return EdgeTuple(source, target)

    @_undoable
    def add_edge(
        self, *edges: EdgeTuple, type_validate: bool = True, strict: bool = False
    ) -> None:
        if type_validate:
            for e in edges:
                self._add_edge(validation.validate_edge(e, self, strict=strict))
        else:
            for e in edges:
                self._add_edge(e)

    @_undoable
    def remove_edge(self, *edges: EdgeTuple) -> None:
        for e in edges:
            self._remove_edge(e)

    @_undoable
    def connect(self, source: Node | Port, target: Port):
        source_port = coerce_to_port(source)
        self._add_edge(
            EdgeTuple(
                source_port_to_handle(source_port, context=self),
                target_port_to_handle(target, context=self),
            )
        )

    @_undoable
    def disconnect(self, *nodes: Node | fr.schemas.Label) -> None:
        for n in nodes:
            self._disconnect(self.get_node(n))

    @_undoable
    def lock_subgraph(
        self, workflow_child: Workflow | dag.Macro | fr.schemas.Label
    ) -> None:
        instance = self.get_node(workflow_child)
        if isinstance(instance, dag.Macro):
            return  # Already locked -- macros are static
        elif not isinstance(instance, Workflow):
            raise TypeError(
                f"Cannot lock {instance.lexical_path!r} -- it is a {type(instance)} but "
                f"we need a {Workflow.__name__}."
            )
        macro_equivalent = constructors.workflow2macro(instance)
        touching = self._edges_touching_node(instance.label)
        self._disconnect(instance)
        self._remove_node_shallow(instance)
        self._add_node(macro_equivalent)
        for edge in touching:
            self._add_edge(edge)

    @_undoable
    def unlock_subgraph(self, macro: Workflow | dag.Macro | fr.schemas.Label) -> None:
        instance = self.get_node(macro)
        if isinstance(instance, Workflow):
            return  # Already unlocked -- workflows are mutable
        elif not isinstance(instance, dag.Macro):
            raise TypeError(
                f"Cannot unlock {instance.lexical_path!r} -- it is not a "
                f"{dag.Macro.__name__}."
            )
        wf_equivalent = constructors.macro2workflow(instance)
        touching = self._edges_touching_node(instance.label)
        self._disconnect(instance)
        self._remove_node_shallow(instance)
        self._add_node(wf_equivalent)
        for edge in touching:
            self._add_edge(edge)

    @_undoable
    def group(self, label: fr.schemas.Label, *nodes: Node | fr.schemas.Label) -> None:
        if not nodes:
            raise ValueError(
                f"Cannot group an empty set of nodes on {self.lexical_path!r}."
            )
        if label in self.nodes:
            raise _duplicate_node_error(self, label)

        resolved = [self.get_node(n) for n in nodes]
        if duplicate := {x.label for x in resolved if resolved.count(x) > 1}:
            raise ValueError(
                f"{self.lexical_path!r} encountered a problem creating group "
                f"{label!r}: Duplicate nodes found in group request: {duplicate}"
            )
        grouped_labels = {x.label for x in resolved}

        subgraph = Workflow(label)
        new_edges: EdgeList = []
        seen_inputs: set[tuple[fr.schemas.Label, fr.schemas.Label]] = set()
        seen_outputs: set[tuple[fr.schemas.Label, fr.schemas.Label]] = set()

        for edge in self.edges:
            src_in_group = (
                isinstance(edge.source, fr.schemas.SourceHandle)
                and edge.source.node in grouped_labels
            )
            tgt_in_group = (
                isinstance(edge.target, fr.schemas.TargetHandle)
                and edge.target.node in grouped_labels
            )
            if src_in_group and tgt_in_group:
                subgraph._add_edge(edge)
            elif tgt_in_group:
                new_label = self._ensure_boundary_input(
                    subgraph, self._target_handle(edge), seen_inputs
                )
                new_edges.append(
                    EdgeTuple(
                        edge.source, fr.schemas.TargetHandle(node=label, port=new_label)
                    )
                )
            elif src_in_group:
                new_label = self._ensure_boundary_output(
                    subgraph, self._source_handle(edge), seen_outputs
                )
                new_edges.append(
                    EdgeTuple(
                        fr.schemas.SourceHandle(node=label, port=new_label), edge.target
                    )
                )

        for node in resolved:
            self._disconnect(node)
            self._move_node(node, self, subgraph)

        self._add_node(subgraph)
        for edge in new_edges:
            self._add_edge(edge)

    @staticmethod
    def _target_handle(edge: EdgeTuple) -> fr.schemas.TargetHandle:
        """Narrow `edge.target` to `TargetHandle`. Caller must have verified."""
        assert isinstance(edge.target, fr.schemas.TargetHandle)
        return edge.target

    @staticmethod
    def _source_handle(edge: EdgeTuple) -> fr.schemas.SourceHandle:
        """Narrow `edge.source` to `SourceHandle`. Caller must have verified."""
        assert isinstance(edge.source, fr.schemas.SourceHandle)
        return edge.source

    @staticmethod
    def _ensure_boundary_port(
        subgraph: Workflow,
        handle: fr.schemas.SourceHandle | fr.schemas.TargetHandle,
        seen: set[tuple[fr.schemas.Label, fr.schemas.Label]],
        port_cls: type[InputPort] | type[OutputPort],
        add_port: Callable[[InputPort | OutputPort], Any],
        make_edge: Callable[[fr.schemas.Label], EdgeTuple],
    ) -> fr.schemas.Label:
        """Create a boundary port on `subgraph` plus its inner edge for
        `handle`, if `(handle.node, handle.port)` hasn't been seen yet.
        Returns the boundary port label either way."""
        new_label = f"{handle.node}__{handle.port}"
        key = (handle.node, handle.port)
        if key not in seen:
            seen.add(key)
            add_port(
                port_cls(
                    label=new_label,
                    owner=subgraph,
                    type_hint=None,
                    type_metadata=None,
                )
            )
            subgraph._add_edge(make_edge(new_label))
        return new_label

    @staticmethod
    def _ensure_boundary_input(
        subgraph: Workflow,
        target: fr.schemas.TargetHandle,
        seen: set[tuple[fr.schemas.Label, fr.schemas.Label]],
    ) -> fr.schemas.Label:
        return Workflow._ensure_boundary_port(
            subgraph,
            target,
            seen,
            InputPort,
            subgraph._add_input,
            lambda new_label: EdgeTuple(fr.schemas.InputSource(port=new_label), target),
        )

    @staticmethod
    def _ensure_boundary_output(
        subgraph: Workflow,
        source: fr.schemas.SourceHandle,
        seen: set[tuple[fr.schemas.Label, fr.schemas.Label]],
    ) -> fr.schemas.Label:
        return Workflow._ensure_boundary_port(
            subgraph,
            source,
            seen,
            OutputPort,
            subgraph._add_output,
            lambda new_label: EdgeTuple(
                source, fr.schemas.OutputTarget(port=new_label)
            ),
        )

    @_undoable  # Lossy on underlying macro function reference, if any
    def ungroup(
        self,
        graph: Workflow | dag.Macro | fr.schemas.Label,
        block_if_reference: bool = False,
        label_map: dict[fr.schemas.Label, fr.schemas.Label] | None = None,
    ) -> None:
        instance = self.get_node(graph)
        if isinstance(instance, dag.Macro):
            if block_if_reference and instance.recipe.reference is not None:
                raise ValueError(
                    f"Cannot ungroup {instance.lexical_path!r} -- it is a "
                    f"{instance.__class__.__name__} with an underlying python "
                    f"reference ({instance.recipe.reference!r}). Override by "
                    "setting `block_if_reference=False`."
                )
            self.unlock_subgraph(instance)
            instance = self.nodes[instance.label]
        if not isinstance(instance, Workflow):
            raise TypeError(
                f"Cannot ungroup {instance.lexical_path!r} -- it is not a "
                f"{Workflow.__name__} or {dag.Macro.__name__}."
            )

        label_map = dict(label_map or {})
        for k in label_map:
            if k not in instance.nodes:
                raise ValueError(
                    f"label_map key {k!r} is not a child of "
                    f"{instance.lexical_path!r}."
                )

        renames: dict[fr.schemas.Label, fr.schemas.Label] = {
            child_label: label_map.get(child_label, f"{instance.label}_{child_label}")
            for child_label in instance.nodes
        }
        new_labels = list(renames.values())
        if duplicates := {x for x in new_labels if new_labels.count(x) > 1}:
            raise ValueError(
                f"Ungrouping {instance.lexical_path!r} would create duplicate "
                f"labels among lifted children: {sorted(duplicates)!r}."
            )
        for new_label in new_labels:
            if new_label in self.nodes and new_label != instance.label:
                raise ValueError(
                    f"Ungrouping {instance.lexical_path!r} would create a name "
                    f"collision on {self.lexical_path!r}: {new_label!r} is "
                    "already present."
                )

        # Index outer edges by the subgraph port they touch -- inner
        # InputSource/OutputTarget endpoints fan in/out to several outer peers,
        # and passthrough inner edges must compose with every (feeder, consumer)
        # pair.
        touching = self._edges_touching_node(instance.label)
        incoming_by_port: dict[fr.schemas.Label, list[EdgeTuple]] = {}
        outgoing_by_port: dict[fr.schemas.Label, list[EdgeTuple]] = {}
        for edge in touching:
            if (
                isinstance(edge.target, fr.schemas.TargetHandle)
                and edge.target.node == instance.label
            ):
                incoming_by_port.setdefault(edge.target.port, []).append(edge)
            else:
                assert (
                    isinstance(edge.source, fr.schemas.SourceHandle)
                    and edge.source.node == instance.label
                )
                outgoing_by_port.setdefault(edge.source.port, []).append(edge)

        # Single pass over inner edges: dispatch by (source kind, target kind) and
        # build the list of edges to lift into the parent.
        rewritten_outer: EdgeList = []
        for inner in instance.edges:
            src, tgt = inner.source, inner.target
            if isinstance(src, fr.schemas.SourceHandle) and isinstance(
                tgt, fr.schemas.TargetHandle
            ):
                # Peer edge: rename both endpoints
                rewritten_outer.append(
                    EdgeTuple(
                        fr.schemas.SourceHandle(node=renames[src.node], port=src.port),
                        fr.schemas.TargetHandle(node=renames[tgt.node], port=tgt.port),
                    )
                )
            elif isinstance(src, fr.schemas.InputSource) and isinstance(
                tgt, fr.schemas.TargetHandle
            ):
                # Subgraph input -> child: each outer feeder now drives the lifted child
                for outer in incoming_by_port.get(src.port, []):
                    rewritten_outer.append(
                        EdgeTuple(
                            outer.source,
                            fr.schemas.TargetHandle(
                                node=renames[tgt.node], port=tgt.port
                            ),
                        )
                    )
            elif isinstance(src, fr.schemas.SourceHandle) and isinstance(
                tgt, fr.schemas.OutputTarget
            ):
                # Child -> subgraph output: each outer consumer reads from the lifted child
                for outer in outgoing_by_port.get(tgt.port, []):
                    rewritten_outer.append(
                        EdgeTuple(
                            fr.schemas.SourceHandle(
                                node=renames[src.node], port=src.port
                            ),
                            outer.target,
                        )
                    )
            elif isinstance(src, fr.schemas.InputSource) and isinstance(
                tgt, fr.schemas.OutputTarget
            ):
                # Passthrough: compose every (feeder, consumer) pair
                for outer_in in incoming_by_port.get(src.port, []):
                    for outer_out in outgoing_by_port.get(tgt.port, []):
                        rewritten_outer.append(
                            EdgeTuple(outer_in.source, outer_out.target)
                        )

        for edge in touching:
            self._remove_edge(edge)
        for child_label, child in list(instance.nodes.items()):
            self._move_node(child, instance, self, new_label=renames[child_label])
        self._remove_node_shallow(instance)
        for edge in rewritten_outer:
            self._add_edge(edge)

    # --- Undo / redo ---

    def _undo_diff(self, diff: actions.GraphDiff) -> actions.GraphDiff:
        inverse = [action.inverse() for action in reversed(diff)]
        for action in inverse:
            self._dispatch(action)
        return inverse

    def undo(self, steps: int = 1) -> list[actions.GraphDiff]:
        undone = []
        for _ in range(steps):
            if not self.undo_stack:
                break
            diff = self.undo_stack.pop()
            inverse = self._undo_diff(diff)
            self.redo_stack.append(diff)
            undone.append(inverse)
        return undone

    def _redo_diff(self, diff: actions.GraphDiff) -> actions.GraphDiff:
        for action in diff:
            self._dispatch(action)
        return diff

    def redo(self, steps: int = 1) -> list[actions.GraphDiff]:
        redone = []
        for _ in range(steps):
            if not self.redo_stack:
                break
            diff = self.redo_stack.pop()
            self._redo_diff(diff)
            self.undo_stack.append(diff)
            redone.append(diff)
        return redone

    def _dispatch(self, action: actions.GraphAction) -> None:
        match action:
            case actions.AddNode(node=node):
                self._add_node(node)
            case actions.RemoveNode(node=node):
                self._remove_node_shallow(node)
            case actions.AddEdge(edge=edge):
                self._add_edge(edge)
            case actions.RemoveEdge(edge=edge):
                self._remove_edge(edge)
            case actions.AddInput(port=port):
                self._add_input(port)
            case actions.RemoveInput(port=port):
                self._remove_input_shallow(port)
            case actions.AddOutput(port=port):
                self._add_output(port)
            case actions.RemoveOutput(port=port):
                self._remove_output_shallow(port)
            case actions.ReplacePort(old_port=o, new_port=n):
                self._replace_port(o, n)
            case actions.RenameNode(node=n, new_label=label):
                self._rename_node_label(n, label)
            case actions.MoveNode(
                node=n,
                from_graph=fg,
                to_graph=tg,
                new_label=nl,
            ):
                self._move_node(n, fg, tg, new_label=nl)
            case _:
                raise TypeError(f"Unknown {actions.GraphAction.__name__}: {action!r}")
