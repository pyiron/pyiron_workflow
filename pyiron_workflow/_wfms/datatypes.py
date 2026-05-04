from __future__ import annotations

import abc
import collections
import dataclasses
import pathlib
from concurrent import futures
from typing import Any, ClassVar, Generic, Protocol, TypeAlias, TypeVar

from flowrep.api import schemas as frs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import execution, lexical

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
    def evaluate(self, run: execution.Run[execution.ResultType]) -> None: ...

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


class FlowControl(Node[frs.LiveWorkflow], Graph, abc.ABC):
    @property
    @abc.abstractmethod
    def prospective_input_edges(self) -> frs.InputEdges: ...

    @property
    @abc.abstractmethod
    def prospective_edges(self) -> frs.Edges: ...

    @property
    @abc.abstractmethod
    def prospective_output_edges(self) -> frs.OutputEdges: ...
