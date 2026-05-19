from __future__ import annotations

import dataclasses
import datetime
import enum
import pathlib
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeVar

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import lexical

if TYPE_CHECKING:
    from pyiron_workflow._wfms.datatypes import Node


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


ResultType = TypeVar("ResultType", bound=frs.LiveNode[Any])


class Step(NamedTuple):
    label: frs.Label
    run: Run[Any]


def _make_steps():
    return Steps()


@dataclasses.dataclass
class Run(Generic[ResultType]):
    lexical_path: lexical.LexicalPathStr
    result: ResultType
    status: RunStatus
    exception: BaseException | None = None
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    progress_dir: pathlib.Path | None = None
    steps: Steps = dataclasses.field(default_factory=_make_steps)

    @property
    def outputs(self):
        return self.result.output_ports

    @property
    def duration(self) -> datetime.timedelta | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at

    @property
    def label(self) -> str:
        return lexical.get_label(self.lexical_path)


class Steps(list[Run[Any]]):
    @property
    def labels(self):
        return [lexical.get_label(step.label) for step in self]


@dataclasses.dataclass(frozen=True)
class RunConfig:
    prime_mover: str
    progress_dir: pathlib.Path = pathlib.Path.cwd()
    progress_hooks: Iterable[
        Callable[[pathlib.Path, datetime.datetime, str, RunStatus], None]
    ] = dataclasses.field(default_factory=list)
    dump_hook: Callable[[pathlib.Path, Run[Any]], None] | None = None

    def emit_progress(
        self, time: datetime.datetime, lexical_path: str, status: RunStatus
    ):
        for hook in self.progress_hooks:
            hook(self.progress_dir, time, lexical_path, status)

    def is_prime_mover(self, candidate: Node[Any, Any]) -> bool:
        return candidate.lexical_path == self.prime_mover

    def dump(self, filename: str, run: Run[Any]):
        if self.dump_hook is not None:
            self.dump_hook(self.progress_dir / filename, run)

    def dump_failure(self, failed_run: Run[Any]):
        self.dump(self.failure_name(failed_run.lexical_path), failed_run)

    @staticmethod
    def failure_name(lexical_path: lexical.LexicalPathStr) -> str:
        return "failure_" + lexical_path.replace(".", "-")


@dataclasses.dataclass
class ExecutorInstructions:
    constructor: type[futures.Executor]
    args: tuple[Any, ...] = dataclasses.field(default_factory=tuple)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    def instantiate(self) -> futures.Executor:
        return self.constructor(*self.args, **self.kwargs)


def run(
    node: Node[Any, ResultType],
    config: RunConfig,
    root: lexical.LexicalPathStr = "",
    label: frs.Label | None = None,
    /,
    **input_data,
) -> Run[ResultType]:
    data_label = node.label if label is None else label
    lexical_path = lexical.lexical_path(root, data_label) if root else data_label

    start_time = datetime.datetime.now()
    status = RunStatus.PENDING
    config.emit_progress(start_time, lexical_path, status)
    current_run = Run[ResultType](
        lexical_path=lexical_path,
        result=node.generate_flowrep_live_node(),
        status=RunStatus.PENDING,
        started_at=start_time,
        progress_dir=config.progress_dir,
    )

    try:
        current_run.status = RunStatus.RUNNING
        populate_input_ports(current_run.result, input_data)
        if node.executor is not None:
            if isinstance(node.executor, ExecutorInstructions):
                with node.executor.instantiate() as exe:
                    f = exe.submit(node.evaluate, current_run, config)
            elif isinstance(node.executor, futures.Executor):
                f = node.executor.submit(node.evaluate, current_run, config)
            else:
                raise TypeError(
                    f"Expected executor to be an instance of ExecutorInstructions or "
                    f"futures.Executor, but {node.lexical_path!r} got {node.executor}."
                )
            current_run = f.result()
        else:
            current_run = node.evaluate(current_run, config)
        current_run.status = RunStatus.FINISHED
    except BaseException as e:
        current_run.exception = e
        current_run.status = RunStatus.FAILED
        if config.is_prime_mover(node):
            config.dump_failure(current_run)
        raise e
    finally:
        end_time = datetime.datetime.now()
        current_run.finished_at = end_time
        config.emit_progress(end_time, node.lexical_path, current_run.status)

    return current_run


def populate_input_ports(node: frs.LiveNode, values: dict[str, Any]) -> None:
    for name, val in values.items():
        if name in node.input_ports:
            node.input_ports[name].value = val
        else:
            raise ValueError(
                f"Input port '{name}' not found -- please select among "
                f"{node.recipe.inputs}"
            )
