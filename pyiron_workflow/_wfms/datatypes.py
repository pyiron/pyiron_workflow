from __future__ import annotations

import abc
import dataclasses
import pathlib
from collections.abc import Mapping
from concurrent import futures
from typing import ClassVar, Generic, NamedTuple, TypeAlias, TypeVar

import semantikon
from flowrep.api import schemas as frs

from pyiron_workflow._wfms import annotation, execution, lexical


@dataclasses.dataclass(frozen=True)
class Port(abc.ABC):  # Satisfies pyiron_workflow._wfms.lexical.Lexical["Node"]
    label: frs.Label
    owner: Node
    type_hint: type | None
    type_metadata: semantikon.TypeMetadata | None
    _io_indicator: ClassVar[str]

    @property
    def lexical_path(self) -> lexical.LexicalPath:
        return lexical.lexical_path(
            self.owner.lexical_path, self._io_indicator, self.label
        )


@dataclasses.dataclass(frozen=True)
class InputPort(Port):
    has_default: bool = False
    _io_indicator: ClassVar[str] = "inputs"


@dataclasses.dataclass(frozen=True)
class OutputPort(Port):
    _io_indicator: ClassVar[str] = "outputs"


PortType = TypeVar("PortType", bound=Port)


class PortMap(lexical.LexicalMap[PortType, lexical.OwnerType]): ...


RecipeType = TypeVar(
    "RecipeType",
    frs.AtomicRecipe,
    frs.ForEachRecipe,
    frs.IfRecipe,
    frs.TryRecipe,
    frs.WhileRecipe,
    frs.WorkflowRecipe,
)


class Node(
    lexical.Lexical["Graph"], Generic[RecipeType, execution.ResultType], abc.ABC
):
    _label: frs.Label
    _owner: Graph | None
    _detached_root: lexical.LexicalPath | None
    executor: futures.Executor | execution.ExecutorInstructions | None
    current_run: execution.Run[execution.ResultType] | None

    @property
    @abc.abstractmethod
    def inputs(self) -> Mapping[frs.Label, InputPort]: ...

    @property
    @abc.abstractmethod
    def outputs(self) -> Mapping[frs.Label, OutputPort]: ...

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
    def label(self) -> frs.Label:
        return self._label

    @label.setter
    def label(self, new_label: frs.Label) -> None:
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

    @owner.setter
    def owner(self, new_owner: Graph | None):
        if (
            self.owner is not None
            and new_owner is not None
            and self.owner is not new_owner
        ):
            raise ValueError(
                f"{self.label!r} ({self.__class__.__name__}) already has an owner: "
                f"{self.owner.lexical_path!r}. It cannot take a new owner: "
                f"{new_owner.lexical_path!r}."
            )
        self._owner = new_owner

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

    def run(self, **input_data) -> execution.Run[execution.ResultType]:
        config = execution.RunConfig(
            prime_mover=self.lexical_path,
            progress_dir=pathlib.Path.cwd(),
            progress_hooks=[],
        )
        current_run = execution.run(self, config, **input_data)
        self.current_run = current_run
        return current_run

    def __getstate__(self):
        state = dict(super().__getstate__())

        if self.owner is not None:
            state["_detached_root"] = self.owner.lexical_path
        state["_owner"] = None

        if isinstance(self.executor, futures.Executor):
            state["executor"] = None

        return state


class StaticNode(Node[RecipeType, execution.ResultType], abc.ABC):
    _owner: Graph | None
    _recipe: RecipeType

    @classmethod
    @abc.abstractmethod
    def _result_type(cls) -> type[execution.ResultType]: ...

    def __init__(
        self,
        label: frs.Label,
        recipe: RecipeType,
        *,
        owner: Graph | None = None,
    ):
        self._label = label  # TODO: also accept None and use function name for default
        self._owner = owner
        self._detached_root = None
        self._recipe = recipe
        live_preview = self.generate_flowrep_live_node()
        self._inputs = self._build_inputs(live_preview)
        self._outputs = self._build_outputs(live_preview)

        self.executor = None
        self.current_run = None

    @property
    def inputs(self) -> PortMap[InputPort, Node]:
        return self._inputs

    @property
    def outputs(self) -> PortMap[OutputPort, Node]:
        return self._outputs

    @property
    def recipe(self) -> RecipeType:
        return self._recipe

    def generate_flowrep_live_node(self) -> execution.ResultType:
        return self._result_type().from_recipe(self.recipe)

    def _build_inputs(self, live: execution.ResultType) -> PortMap[InputPort, Node]:
        return PortMap[InputPort, Node](
            self,
            {
                label: InputPort(
                    label=label,
                    owner=self,
                    type_hint=annotation.annotation_to_type_hint(
                        flowrep_port.annotation
                    ),
                    type_metadata=annotation.annotation_to_type_metadata(
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
                    type_hint=annotation.annotation_to_type_hint(
                        flowrep_port.annotation
                    ),
                    type_metadata=annotation.annotation_to_type_metadata(
                        flowrep_port.annotation
                    ),
                )
                for label, flowrep_port in live.output_ports.items()
            },
        )


class NodeMap(lexical.LexicalMap[Node, "Graph"]):

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self._pwf_lexical_map__owner.lexical_path}):\n"
            + "\n".join(
                f"\t{label!r}: {type(node).__name__}" for label, node in self.items()
            )
        )


class EdgeTuple(NamedTuple):
    source: frs.InputSource | frs.SourceHandle
    target: frs.OutputTarget | frs.TargetHandle


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
        label: frs.Label,
        recipe: RecipeType,
        *,
        owner: Graph | None = None,
    ):
        super().__init__(label, recipe, owner=owner)
        self._nodes = self._build_nodes(recipe)
        self._edges = self._build_edges(recipe)

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
