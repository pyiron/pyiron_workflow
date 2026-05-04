from __future__ import annotations

import abc
import collections
import dataclasses
import datetime
import enum
import pathlib
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import Any, ClassVar, Generic, TypeAlias, TypeVar, cast

from flowrep.api import schemas as frs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import lexical

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


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


ResultType = TypeVar("ResultType", bound=frs.LiveNode)


@dataclasses.dataclass
class Run(Generic[ResultType]):
    result: ResultType
    status: RunStatus
    exception: BaseException | None = None
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    progress_dir: pathlib.Path | None = None

    @property
    def outputs(self):
        return self.result.output_ports

    @property
    def duration(self) -> datetime.timedelta | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at


@dataclasses.dataclass(frozen=True)
class RunConfig:
    progress_dir: pathlib.Path
    prime_mover: str
    progress_hooks: Iterable[Callable[[str, RunStatus], None]]


def _run_on_worker(
    node: Node,
    run_config: RunConfig,
    **input_data,
):
    raise NotImplementedError()


class ProgressSink:
    """
    Just a placeholder class -- there should be a way to avoid re-inventing the wheel
    """

    def __init__(self, run_config: RunConfig):
        self._run_config = run_config

    def emit(self, lexical_path: str, status: RunStatus):
        for hook in self._run_config.progress_hooks:
            hook(lexical_path, status)


class Node(lexical.Lexical["Graph"], Generic[ResultType], abc.ABC):
    _label: frs.Label
    _owner: Graph | None
    executor: futures.Executor | ExecutorInstructions | None
    current_run: Run[ResultType] | None
    run_history: collections.deque[Run[ResultType]]

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
    def _empty_live_node(self) -> ResultType: ...

    @abc.abstractmethod
    def _evaluate(
        self, _pwf_node_evaluate__run: Run[ResultType], **input_data_kwargs
    ) -> Run[ResultType]: ...

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

    def run(
        self,
        _pwf_node_run__config: RunConfig | None = None,
        /,
        **input_data,
    ) -> Run[ResultType]:
        config = (
            RunConfig(
                progress_dir=pathlib.Path.cwd(),
                prime_mover=self.lexical_path,
                progress_hooks=[],
            )
            if _pwf_node_run__config is None
            else _pwf_node_run__config
        )

        self.current_run = Run[ResultType](
            result=self._empty_live_node(),
            status=RunStatus.RUNNING,
            started_at=datetime.datetime.now(),
            progress_dir=config.progress_dir,
        )
        sink = self._open_progress_sink(config)
        sink.emit(self.lexical_path, self.current_run.status)

        try:
            self._evaluate(self.current_run, **input_data)
            # Needs careful thinking about how failure will be handled
            self.current_run.status = RunStatus.FINISHED
        except BaseException as e:
            self.current_run.exception = e
            self.current_run.status = RunStatus.FAILED
            if self._is_prime_mover(config.prime_mover):
                self._dump_recovery(config.progress_dir)
            raise e
        finally:
            sink.emit(self.lexical_path, self.current_run.status)
            if self._is_prime_mover(config.prime_mover):
                self.run_history.append(cast(Run[ResultType], self.current_run))

        return cast(Run[ResultType], self.current_run)

    @staticmethod
    def _open_progress_sink(config: RunConfig) -> ProgressSink:
        return ProgressSink(config)

    def _is_prime_mover(self, lexical_path: str) -> bool:
        return lexical_path == self.lexical_path

    def _dump_recovery(self, directory: pathlib.Path):
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


class Graph(Node[ResultType], abc.ABC):
    input_edges: frs.InputEdges
    edges: frs.Edges
    output_edges: frs.OutputEdges

    @property
    @abc.abstractmethod
    def nodes(self) -> NodeMap: ...


class FlowControl(Graph[frs.FlowControl], abc.ABC):
    prospective_input_edges: frs.InputEdges
    prospective_edges: frs.Edges
    prospective_output_edges: frs.OutputEdges
