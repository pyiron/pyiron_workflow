from __future__ import annotations

import abc
import collections
import dataclasses
import pathlib
from concurrent import futures
from typing import Any, ClassVar, Generic, Protocol, TypeAlias, TypeVar

from flowrep.api import schemas as frs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import annotation, execution, lexical

RecipeType: TypeAlias = (
    frs.AtomicNode
    | frs.ForEachNode
    | frs.IfNode
    | frs.TryNode
    | frs.WhileNode
    | frs.WorkflowNode
)


@dataclasses.dataclass(frozen=True)
class Port(abc.ABC):  # Satisfies pyiron_workflow._wfms.lexical.Lexical["Node"]
    label: frs.Label
    owner: Node
    type_hint: type | None
    type_metadata: sds.TypeMetadata | None
    _io_indicator: ClassVar[str]

    @property
    def lexical_path(self) -> str:
        return f"{self.owner.lexical_path}.{self._io_indicator}.{self.label}"


@dataclasses.dataclass(frozen=True)
class InputPort(Port):
    has_default: bool = False
    _io_indicator: ClassVar[str] = "inputs"


@dataclasses.dataclass(frozen=True)
class OutputPort(Port):
    _io_indicator: ClassVar[str] = "outputs"


PortType = TypeVar("PortType", bound=Port)


class PortMap(lexical.LexicalMap[PortType, lexical.OwnerType_co]): ...


ExecutorInstructions = tuple[type[futures.Executor], tuple[Any, ...], dict[str, Any]]


class Node(lexical.Lexical["Graph"], Generic[execution.ResultType], abc.ABC):
    _label: frs.Label
    _owner: Graph | None
    executor: futures.Executor | ExecutorInstructions | None
    current_run: execution.Run[execution.ResultType] | None
    run_history: collections.deque[execution.Run[execution.ResultType]]

    @property
    @abc.abstractmethod
    def inputs(self) -> PortMap[InputPort, Node]: ...

    @property
    @abc.abstractmethod
    def outputs(self) -> PortMap[OutputPort, Node]: ...

    @property
    @abc.abstractmethod
    def recipe(self) -> RecipeType: ...

    @abc.abstractmethod
    def generate_flowrep_live_node(self) -> execution.ResultType: ...

    @abc.abstractmethod
    def evaluate(
        self, run: execution.Run[execution.ResultType], config: execution.RunConfig
    ) -> None: ...

    @property
    def label(self) -> frs.Label:
        return self._label

    @label.setter
    def label(self, new_label: frs.Label) -> None:
        # TODO: Validation and ownership location concerns
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
    def lexical_path(self) -> str:
        root = "" if self.owner is None else f"{self.owner.lexical_path}."
        return root + self.label

    @property
    def history_limit(self) -> int | None:
        return self.run_history.maxlen

    @history_limit.setter
    def history_limit(self, value: int) -> None:
        self.run_history = collections.deque(self.run_history, maxlen=value)

    def run(self, **input_data) -> execution.Run[execution.ResultType]:
        config = execution.RunConfig(
            prime_mover=self.lexical_path,
            progress_dir=pathlib.Path.cwd(),
            progress_hooks=[],
        )
        return execution.run(self, config, **input_data)

    def dump(self, file: pathlib.Path):
        raise NotImplementedError()

    def __getstate__(self):
        state = super().__getstate__()

        owner = state.pop("_owner", None)
        if owner is not None:
            state["_last_detatched_path"] = owner.lexical_path

        if isinstance(self.executor, futures.Executor):
            state["executor"] = None

        return state


class StaticNode(Node[execution.ResultType], abc.ABC):

    @classmethod
    @abc.abstractmethod
    def _result_type(cls) -> type[execution.ResultType]: ...

    def __init__(
        self,
        label: frs.Label,
        recipe: RecipeType,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        self._label = label  # TODO: also accept None and use function name for default
        self._owner = owner
        self._recipe = recipe
        live_preview = self.generate_flowrep_live_node()
        self._inputs = self._build_inputs(live_preview)
        self._outputs = self._build_outputs(live_preview)

        self.executor = None
        self.current_run = None
        self.run_history = collections.deque(maxlen=history_limit)

    @property
    def inputs(self) -> PortMap[InputPort, Node]:
        return self._inputs

    @property
    def outputs(self) -> PortMap[OutputPort, Node]:
        return self._outputs

    @property
    def recipe(self) -> execution.ResultType:
        return self._recipe

    def generate_flowrep_live_node(self) -> frs.LiveAtomic:
        return self._result_type().from_recipe(self.recipe)

    def _build_inputs(self, live: frs.LiveNode) -> PortMap[InputPort, Node]:
        return PortMap[InputPort, Node](
            self,
            *(
                InputPort(
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
            ),
        )

    def _build_outputs(self, live: frs.LiveNode) -> PortMap[OutputPort, Node]:
        return PortMap[OutputPort, Node](
            self,
            *(
                OutputPort(
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
            ),
        )


class NodeMap(lexical.LexicalMap[Node, "Graph"]): ...


class Graph(lexical.Lexical["Graph"], Protocol):
    @property
    def input_edges(self) -> frs.InputEdges: ...

    @property
    def edges(self) -> frs.Edges: ...

    @property
    def output_edges(self) -> frs.OutputEdges: ...

    @property
    def nodes(self) -> NodeMap: ...


class FlowControl(StaticNode[frs.LiveWorkflow], Graph, abc.ABC):
    """
    Flow controls all have a prospective recipe which resolves into a retrospective DAG.

    In light of this, the :class:`Graph` properties all exist in reference only to the
    current run, while new propsective properties are informed by the recipe itself.
    This differs from macro graphs, where the topology of the recipe and run result are
    identical, and we only need to populate the recipe topology with data values.
    """

    @property
    @abc.abstractmethod
    def prospective_input_edges(self) -> frs.InputEdges: ...

    @property
    @abc.abstractmethod
    def prospective_edges(self) -> frs.Edges: ...

    @property
    @abc.abstractmethod
    def prospective_output_edges(self) -> frs.ProspectiveOutputEdges: ...

    @property
    @abc.abstractmethod
    def prospective_nodes(self) -> NodeMap: ...

    @abc.abstractmethod
    def _build_retrospective_input_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.InputEdges: ...

    @abc.abstractmethod
    def _build_retrospective_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.Edges: ...

    @abc.abstractmethod
    def _build_retrospective_output_edges(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> frs.OutputEdges: ...

    @abc.abstractmethod
    def _build_retrospective_nodes(
        self, run: execution.Run[frs.LiveWorkflow]
    ) -> NodeMap: ...

    @classmethod
    def _result_type(cls) -> type[frs.LiveWorkflow]:
        return frs.FlowControl

    @property
    def input_edges(self) -> frs.InputEdges:
        if self.current_run is None:
            return {}
        return self._build_retrospective_input_edges(self.current_run)

    @property
    def edges(self) -> frs.Edges:
        if self.current_run is None:
            return {}
        return self._build_retrospective_edges(self.current_run)

    @property
    def output_edges(self) -> frs.OutputEdges:
        if self.current_run is None:
            return {}
        return self._build_retrospective_output_edges(self.current_run)

    @property
    def nodes(self) -> NodeMap:
        if self.current_run is None:
            return NodeMap(self)
        return self._build_retrospective_nodes(self.current_run)
