from __future__ import annotations

import abc
import dataclasses
from collections.abc import Mapping
from concurrent import futures
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    NamedTuple,
    Self,
    TypeAlias,
    TypeVar,
)

import flowrep as fr
import semantikon

from pyiron_workflow._wfms import execution, injection, lexical

if TYPE_CHECKING:
    from pyiron_workflow._wfms import actions


@dataclasses.dataclass(frozen=True)
class Port(
    injection.OperatorInjectionMixin, abc.ABC
):  # Satisfies pyiron_workflow._wfms.lexical.Lexical["Node"]
    label: fr.schemas.Label
    owner: Node
    type_hint: type | None
    type_metadata: semantikon.TypeMetadata | None
    _io_indicator: ClassVar[str]

    @property
    def lexical_path(self) -> lexical.LexicalPath:
        return lexical.lexical_path(
            self.owner.lexical_path, self._io_indicator, self.label
        )

    def __str__(self):
        hint = f", type_hint: {self.type_hint}" if self.type_hint is not None else ""
        meta = (
            f", type_metadata: {self.type_metadata}"
            if self.type_metadata is not None
            else ""
        )
        return f"{self.__class__.__name__}({self.lexical_path}" + hint + meta + ")"


@dataclasses.dataclass(frozen=True)
class InputPort(Port):
    has_default: bool = False
    _io_indicator: ClassVar[str] = "inputs"

    @property
    def _injection(self) -> injection.InjectionContext:
        return injection.InjectionContext(
            port=lambda: self,
            graph=lambda: self.owner,
            label=lambda: self.label,
            lexical_path=lambda: self.lexical_path,
        )


@dataclasses.dataclass(frozen=True)
class OutputPort(Port):
    _io_indicator: ClassVar[str] = "outputs"

    @property
    def _injection(self) -> injection.InjectionContext:
        return injection.InjectionContext(
            port=lambda: self,
            graph=lambda: self.owner.owner,
            label=lambda: f"{self.owner.label}_{self.label}",
            lexical_path=lambda: self.lexical_path,
        )


def coerce_to_port(obj: Port | Node) -> Port:
    if isinstance(obj, Port):
        return obj
    elif isinstance(obj, Node):
        if len(obj.outputs) != 1:
            raise ValueError(
                "Nodes can only be used as proxies for ports in edge "
                "creation sugar if they have a single output port."
                f"{obj.lexical_path!r} cannot be coerced to a port since it has more "
                f"than one output port. "
            )
        return next(iter(obj.outputs.values()))
    else:
        raise TypeError(
            f"Expected a {Port.__name__} or a {Node.__name__} object, got {obj} "
        )


def source_port_to_handle(port: Port, context: MutableDag):
    if port.owner is context and port.label in context.inputs:
        return fr.schemas.InputSource(port=port.label)
    elif port.owner.owner is context and port.label in port.owner.outputs:
        return fr.schemas.SourceHandle(node=port.owner.label, port=port.label)
    else:
        raise ValueError(
            f"Port {port.lexical_path!r} is not owned by the inputs of context mutable "
            f"graph {context.lexical_path!r}, nor an output of that graph's child."
        )


def target_port_to_handle(port: Port, context: MutableDag):
    if port.owner is context and port.label in context.outputs:
        return fr.schemas.OutputTarget(port=port.label)
    elif port.owner.owner is context and port.label in port.owner.inputs:
        return fr.schemas.TargetHandle(node=port.owner.label, port=port.label)
    else:
        raise ValueError(
            f"Port {port.label!r} is not owned by the outputs of context mutable graph "
            f"{context.lexical_path!r}, nor an input of that graph's child."
        )


PortType = TypeVar("PortType", bound=Port)


class PortMap(lexical.LexicalMap[PortType, lexical.OwnerType]): ...


RecipeType = TypeVar(
    "RecipeType",
    fr.schemas.AtomicRecipe,
    fr.schemas.ForEachRecipe,
    fr.schemas.IfRecipe,
    fr.schemas.TryRecipe,
    fr.schemas.WhileRecipe,
    fr.schemas.WorkflowRecipe,
    fr.schemas.ConstantRecipe,
)


class Node(
    lexical.Lexical["Graph"],
    injection.OperatorInjectionMixin,
    Generic[RecipeType, execution.ResultType],
    abc.ABC,
):
    _label: fr.schemas.Label
    _owner: Graph | None
    _detached_root: lexical.LexicalPath | None
    _pending_connections: dict[str, Port]
    executor: futures.Executor | execution.ExecutorInstructions | None
    last_run: execution.Run[execution.ResultType] | None

    @property
    @abc.abstractmethod
    def inputs(self) -> lexical.LexicalMap[InputPort, Any]: ...

    @property
    @abc.abstractmethod
    def outputs(self) -> lexical.LexicalMap[OutputPort, Any]: ...

    @property
    @abc.abstractmethod
    def recipe(self) -> RecipeType: ...

    @abc.abstractmethod
    def generate_flowrep_live_node(self) -> execution.ResultType: ...

    @abc.abstractmethod
    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]: ...

    @property
    def label(self) -> fr.schemas.Label:
        return self._label

    @label.setter
    def label(self, new_label: fr.schemas.Label) -> None:
        if self.owner is not None:
            raise ValueError(
                f"{self.__class__.__name__}({self.lexical_path!r}) already has an "
                f"owner (lexical path = {getattr(self.owner, 'lexical_path', None)!r}). "
                f"It cannot take the new label: {new_label!r}."
                # TODO: Redirect to moving it on the owner once this is available
            )
        self._label = new_label

    @property
    def owner(self) -> Graph | None:
        return self._owner

    @property
    def lexical_root(self) -> lexical.LexicalPath:
        if self.owner is not None:
            return self.owner.lexical_path
        elif detached := self._detached_root:
            return detached
        else:
            return lexical.LexicalPath()

    @property
    def lexical_path(self) -> lexical.LexicalPath:
        if self.lexical_root:
            return lexical.lexical_path(self.lexical_root, self.label)
        return self.label

    def get_input(self, port: InputPort | fr.schemas.Label) -> InputPort:
        """A flexible wrapper to access inputs by object or by label"""
        return lexical.get_item_from_map(port, self.inputs, "input port")

    def get_output(self, port: OutputPort | fr.schemas.Label) -> OutputPort:
        """A flexible wrapper to access outputs by object or by label"""
        return lexical.get_item_from_map(port, self.outputs, "output port")

    def run(
        self, config: execution.RunConfig | None = None, /, **input_data
    ) -> execution.Run[execution.ResultType]:
        current_run = execution.run(self, config, **input_data)
        self.last_run = current_run
        return current_run

    def pull(
        self,
        config: execution.RunConfig | None = None,
        break_out_of_context: bool = False,
        expose_defaults: bool = False,
        /,
        **input_kwargs,
    ) -> execution.Run[execution.ResultType]:
        from pyiron_workflow._wfms import pull as _pull  # noqa: PLC0415 -- cycle guard

        return _pull.pull(
            self, config, break_out_of_context, expose_defaults, **input_kwargs
        )

    def pulled_workflow(
        self, break_out_of_context: bool = False, expose_defaults: bool = False, /
    ):
        from pyiron_workflow._wfms import pull as _pull  # noqa: PLC0415 -- cycle guard

        return _pull.pulled_workflow(self, break_out_of_context, expose_defaults)

    def pulled_inputs(
        self, break_out_of_context: bool = False, expose_defaults: bool = False, /
    ):
        from pyiron_workflow._wfms import pull as _pull  # noqa: PLC0415 -- cycle guard

        return _pull.pulled_inputs(self, break_out_of_context, expose_defaults)

    @abc.abstractmethod
    def copy(
        self, new_label: fr.schemas.Label | None = None, _copy_to: Self | None = None
    ) -> Self:
        """
        Make a new copy of this node based on its recipe, and copy over node-state
        (so far, just the executor.)

        Intentionally loses scope information like the owner and any pending
        connections, and history information like the ``last_run``.
        """

    @staticmethod
    def _copy_data(from_: Node, to_: Node, /) -> None:
        to_.executor = from_.executor

    def __call__(self, *args: Port | Node, **kwargs: Port | Node) -> Self:
        self.connect_input(*args, **kwargs)
        return self

    def connect_input(self, *args: Port | Node, **kwargs: Port | Node) -> None:
        """
        A syntactic shortcut for adding new edges feeding this node on the owning graph.

        If this node does not yet have an owner, caches these edges for later use with
        :meth:`apply_pending_connections`.
        """
        connections = dict(zip(self.inputs.keys(), args, strict=False))
        connections.update(kwargs)
        for k, v in connections.items():
            connections[k] = coerce_to_port(v)
        self._pending_connections.update(connections)
        if isinstance(self._owner, MutableDag):
            self._owner.add_edge(*self.use_pending_edges())
        elif self._owner is not None:
            raise self._mutable_owner_error()

    def use_pending_edges(self) -> EdgeList:
        """
        Converts the internal pending connections to a list of edges and clears the
        pending dictionary -- use 'em or lose 'em.

        Raises instead if the current owner is not a mutable dag.
        """
        if isinstance(self.owner, MutableDag):
            edges: EdgeList = []
            for target_label, source_port in self._pending_connections.items():
                source = source_port_to_handle(source_port, self.owner)
                target = fr.schemas.TargetHandle(node=self.label, port=target_label)
                edges.append(EdgeTuple(source, target))
            self._pending_connections.clear()
            return edges
        else:
            raise self._mutable_owner_error()

    def detach_pending_connections(self) -> dict[str, Port]:
        """
        Return a copy of the pending input connections and clear them, *without*
        attempting to realize them as edges.

        Unlike :meth:`use_pending_edges`, this neither requires a mutable owner nor
        resolves the connections against one. It is used when a node carrying pending
        connections is absorbed into a new graph during operator injection, so the
        connections can be re-homed (lifted) onto the absorbing graph.
        """
        detached = dict(self._pending_connections)
        self._pending_connections.clear()
        return detached

    def _mutable_owner_error(self) -> TypeError:
        tag = ""
        if isinstance(self.owner, ImmutableDag):
            tag = "Try unlocking owner to a mutable graph first."
        return TypeError(
            f"{self.lexical_path!r} does not have a mutable owner, and so its "
            f"inputs cannot be modified." + tag
        )

    @property
    def _injection(self) -> injection.InjectionContext:
        return injection.InjectionContext(
            port=self._injection_port,
            graph=self._injection_context,
            label=lambda: self.label,
            lexical_path=lambda: self.lexical_path,
        )

    def _injection_port(self) -> OutputPort:
        if len(self.outputs) != 1:
            raise ValueError(
                f"{self.lexical_path!r} cannot be injected since it has more than "
                f"one output port."
            )
        return next(iter(self.outputs.values()))

    def _injection_context(self) -> Graph | None:
        context_graph = self.owner
        if context_graph is None and self._pending_connections:
            # A freshly-built injection graph is unparented but logically belongs to the
            # context that owns its pending sources. They share one context by construction
            # (cross-context combination is rejected before any graph is built), so any
            # representative resolves it.
            representative = next(iter(self._pending_connections.values()))
            context_graph = representative._injection.graph
        return context_graph

    def __getstate__(self):
        state = dict(super().__getstate__())

        if self.owner is not None:
            state["_detached_root"] = self.owner.lexical_path
        state["_owner"] = None

        if isinstance(self.executor, futures.Executor):
            state["executor"] = None

        return state


class StaticNode(Node[RecipeType, execution.ResultType], abc.ABC):
    _recipe: RecipeType

    @classmethod
    @abc.abstractmethod
    def _result_type(cls) -> type[execution.ResultType]: ...

    def __init__(
        self,
        recipe: RecipeType,
        label: fr.schemas.Label,
        /,
        *positional_connections: Port | Node,
        **keyword_connections: Port | Node,
    ):
        self._label = label  # TODO: also accept None and use function name for default
        self._owner = None
        self._detached_root = None
        self._pending_connections = {}
        self._recipe = recipe
        live_preview = self.generate_flowrep_live_node()
        self._inputs = self._build_inputs(live_preview)
        self._outputs = self._build_outputs(live_preview)

        self.executor = None
        self.last_run = None
        self.connect_input(*positional_connections, **keyword_connections)

    @property
    def inputs(self) -> PortMap[InputPort, Node]:
        return self._inputs

    @property
    def outputs(self) -> PortMap[OutputPort, Node]:
        return self._outputs

    @property
    def recipe(self) -> RecipeType:
        return self._recipe

    def copy(
        self, new_label: fr.schemas.Label | None = None, _copy_to: Self | None = None
    ) -> Self:
        node_copy = _copy_to or self.__class__(self.recipe, new_label or self.label)
        self._copy_data(self, node_copy)
        return node_copy

    def generate_flowrep_live_node(self) -> execution.ResultType:
        return self._result_type().from_recipe(self.recipe)

    def _build_inputs(self, live: execution.ResultType) -> PortMap[InputPort, Node]:
        return PortMap[InputPort, Node](
            self,
            {
                label: InputPort(
                    label=label,
                    owner=self,
                    type_hint=semantikon.annotation_to_type_hint(
                        flowrep_port.annotation
                    ),
                    type_metadata=semantikon.annotation_to_type_metadata(
                        flowrep_port.annotation
                    ),
                    has_default=label in self.recipe.inputs_with_defaults,
                )
                for label, flowrep_port in live.input_ports.items()
            },
        )

    def _build_outputs(self, live: execution.ResultType) -> PortMap[OutputPort, Node]:
        return PortMap[OutputPort, Node](
            self,
            {
                label: OutputPort(
                    label=label,
                    owner=self,
                    type_hint=semantikon.annotation_to_type_hint(
                        flowrep_port.annotation
                    ),
                    type_metadata=semantikon.annotation_to_type_metadata(
                        flowrep_port.annotation
                    ),
                )
                for label, flowrep_port in live.output_ports.items()
            },
        )


class NodeMap(lexical.LexicalMap[Node, "Graph"]):

    def __init__(
        self,
        owner: Graph,
        data: Mapping[fr.schemas.Label, Node] | None = None,
        /,
    ):
        super().__init__(owner, data)
        for value in self._pwf_lexical_map__data.values():
            value._owner = owner

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self._pwf_lexical_map__owner.lexical_path}):\n"
            + "\n".join(
                f"\t{label!r}: {type(node).__name__}" for label, node in self.items()
            )
        )


class EdgeTuple(NamedTuple):
    source: fr.schemas.InputSource | fr.schemas.SourceHandle
    target: fr.schemas.OutputTarget | fr.schemas.TargetHandle


EdgeList: TypeAlias = list[EdgeTuple]


class Graph(lexical.HasLexicalPath, abc.ABC):
    """A node that owns a map of child nodes and the edges between them.

    Concrete graphs (`StaticGraph`, `Workflow`) are also `Node`s and take their
    lexical identity from there; `Graph` only contributes the graph-specific
    surface. It depends on `HasLexicalPath` rather than the full `Lexical`
    protocol so that it shares no base with `Node` -- keeping the
    `StaticGraph` / `Workflow` inheritance free of a `Lexical` diamond.

    `__getattr__` makes child nodes reachable as attributes: it runs only after
    normal attribute lookup fails, so genuine attributes (properties, methods,
    instance state) always shadow a node of the same name. Names starting with
    `_` are never resolved this way -- it keeps the fallback recursion-safe and
    avoids exposing private-looking state. Such nodes remain reachable via
    `graph.nodes[label]`.
    """

    @property
    @abc.abstractmethod
    def nodes(self) -> NodeMap: ...

    @property
    @abc.abstractmethod
    def edges(self) -> EdgeList: ...

    def get_node(self, node: Node | fr.schemas.Label) -> Node:
        """A flexible wrapper to access nodes by object or by label"""
        return lexical.get_item_from_map(node, self.nodes, "node")

    def __getattr__(self, item: str) -> Node:
        if item.startswith("_"):
            raise AttributeError(item)
        try:
            return self.nodes[item]
        except KeyError:
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {item!r}"
            ) from None

    def __setstate__(self, state: dict[str, object]) -> None:
        # `Node.__getstate__` nulls every node's `_owner` so a node shipped to
        # an executor does not drag its antecedent graph along. A graph's
        # children travel inside `nodes` but arrive detached, so re-attach
        # them (and drop the now-stale detached root) here.
        self.__dict__.update(state)
        for child in self.nodes.values():
            child._owner = self
            child._detached_root = None


class StaticGraph(StaticNode[RecipeType, execution.ResultType], Graph, abc.ABC):

    _nodes: NodeMap
    _edges: EdgeList

    def __init__(
        self,
        recipe: RecipeType,
        label: fr.schemas.Label,
        /,
        *positional_connections: Port | Node,
        **keyword_connections: Port | Node,
    ):
        super().__init__(recipe, label, *positional_connections, **keyword_connections)
        self._nodes = self._build_nodes(recipe)
        self._edges = self._build_edges(recipe)

    def copy(
        self, new_label: fr.schemas.Label | None = None, _copy_to: Self | None = None
    ) -> Self:
        node_copy = super().copy(new_label=new_label, _copy_to=_copy_to)
        for label, child in self.nodes.items():
            child.copy(_copy_to=node_copy.nodes[label])
        return node_copy

    @abc.abstractmethod
    def _build_nodes(self, recipe: RecipeType) -> NodeMap: ...

    @abc.abstractmethod
    def _build_edges(self, recipe: RecipeType) -> EdgeList: ...

    @property
    def nodes(self) -> NodeMap:
        return self._nodes

    @property
    def edges(self) -> EdgeList:
        return self._edges


class ImmutableDag(
    StaticGraph[fr.schemas.WorkflowRecipe, fr.schemas.DagData], abc.ABC
): ...


class MutableDag(Node[fr.schemas.WorkflowRecipe, fr.schemas.DagData], Graph, abc.ABC):
    undo_limit: int | None

    @abc.abstractmethod
    def create_input(
        self,
        label: fr.schemas.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> None: ...

    @abc.abstractmethod
    def remove_input(self, port: InputPort | fr.schemas.Label) -> None: ...

    @abc.abstractmethod
    def rename_input(
        self, port: InputPort | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def create_output(
        self,
        label: fr.schemas.Label,
        type_hint: type | None = None,
        type_metadata: semantikon.TypeMetadata | None = None,
    ) -> None: ...

    @abc.abstractmethod
    def remove_output(self, port: OutputPort | fr.schemas.Label) -> None: ...

    @abc.abstractmethod
    def rename_output(
        self, port: OutputPort | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def add_port_hint(
        self, port: InputPort | OutputPort, hint: type | None
    ) -> None: ...

    @abc.abstractmethod
    def remove_port_hint(self, port: InputPort | OutputPort) -> None: ...

    @abc.abstractmethod
    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
    ) -> None: ...

    @abc.abstractmethod
    def remove_port_metadata(self, port: InputPort | OutputPort) -> None: ...

    @abc.abstractmethod
    def add_node(self, *nodes: Node) -> None: ...

    @abc.abstractmethod
    def remove_node(self, *nodes: Node | fr.schemas.Label) -> None: ...

    @abc.abstractmethod
    def rename_node(
        self, node: Node | fr.schemas.Label, new_label: fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def add_edge(
        self, *edges: EdgeTuple, type_validate: bool = True, strict: bool = False
    ) -> None: ...

    @abc.abstractmethod
    def remove_edge(self, *edges: EdgeTuple) -> None: ...

    @abc.abstractmethod
    def connect(self, source: Node | Port, target: Port): ...

    @abc.abstractmethod
    def disconnect(self, *nodes: Node | fr.schemas.Label) -> None: ...

    @abc.abstractmethod
    def lock_subgraph(
        self, workflow_child: MutableDag | ImmutableDag | fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def unlock_subgraph(
        self, macro: MutableDag | ImmutableDag | fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def group(
        self, label: fr.schemas.Label, *nodes: Node | fr.schemas.Label
    ) -> None: ...

    @abc.abstractmethod
    def ungroup(
        self,
        graph: MutableDag | ImmutableDag | fr.schemas.Label,
        block_if_reference: bool = False,
        label_map: dict[fr.schemas.Label, fr.schemas.Label] | None = None,
    ) -> None: ...

    @abc.abstractmethod
    def undo(self, steps: int = 1) -> list[actions.GraphDiff]: ...

    @abc.abstractmethod
    def redo(self, steps: int = 1) -> list[actions.GraphDiff]: ...


class InjectionContextHelper(injection.InjectionContext, abc.ABC):

    @property
    @abc.abstractmethod
    def port(self) -> Port: ...

    @property
    @abc.abstractmethod
    def graph(self) -> MutableDag | None: ...

    @property
    @abc.abstractmethod
    def label(self) -> fr.schemas.Label: ...
