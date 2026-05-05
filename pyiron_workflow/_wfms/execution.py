from __future__ import annotations

import dataclasses
import datetime
import enum
import pathlib
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from flowrep.api import schemas as frs
from flowrep.wfms import _populate_input_ports

if TYPE_CHECKING:
    from pyiron_workflow._wfms.datatypes import Node


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
    steps: list[Run[Any]] = dataclasses.field(default_factory=list)

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
    prime_mover: str
    progress_dir: pathlib.Path
    progress_hooks: Iterable[
        Callable[[pathlib.Path, datetime.datetime, str, RunStatus], None]
    ]

    def emit_progress(
        self, time: datetime.datetime, lexical_path: str, status: RunStatus
    ):
        for hook in self.progress_hooks:
            hook(self.progress_dir, time, lexical_path, status)

    def is_prime_mover(self, candidate: Node[Any]) -> bool:
        return candidate.lexical_path == self.prime_mover


@dataclasses.dataclass
class ExecutorInstructions:
    constructor: type[futures.Executor]
    args: tuple[Any, ...] = dataclasses.field(default_factory=tuple)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    def instantiate(self) -> futures.Executor:
        return self.constructor(*self.args, **self.kwargs)


def run(
    _pwf_run__node: Node[ResultType],
    _pwf_run__config: RunConfig,
    /,
    **input_data,
) -> Run[ResultType]:
    node = _pwf_run__node
    config = _pwf_run__config

    start_time = datetime.datetime.now()
    status = RunStatus.PENDING
    config.emit_progress(start_time, node.lexical_path, status)
    node.current_run = Run[ResultType](
        result=node.generate_flowrep_live_node(),
        status=RunStatus.PENDING,
        started_at=start_time,
        progress_dir=config.progress_dir,
    )

    try:
        node.current_run.status = RunStatus.RUNNING
        _populate_input_ports(node.current_run.result, input_data)
        if node.executor is not None:
            if isinstance(node.executor, ExecutorInstructions):
                with node.executor.instantiate() as exe:
                    f = exe.submit(node.evaluate, node.current_run, config)
            elif isinstance(node.executor, futures.Executor):
                f = node.executor.submit(node.evaluate, node.current_run, config)
            else:
                raise TypeError(
                    f"Expected executor to be an instance of ExecutorInstructions or "
                    f"futures.Executor, but {node.lexical_path!r} got {node.executor}."
                )
            f.result()
        else:
            node.evaluate(node.current_run, config)
        node.current_run.status = RunStatus.FINISHED
    except BaseException as e:
        node.current_run.exception = e
        node.current_run.status = RunStatus.FAILED
        if config.is_prime_mover(node):
            node.dump(config.progress_dir / "failed_state")
        raise e
    finally:
        end_time = datetime.datetime.now()
        node.current_run.finished_at = end_time
        config.emit_progress(end_time, node.lexical_path, node.current_run.status)
        if config.is_prime_mover(node):
            node.run_history.append(node.current_run)

    return node.current_run
