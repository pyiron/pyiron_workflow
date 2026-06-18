from __future__ import annotations

import dataclasses
import datetime
import enum
import pathlib
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeVar

import flowrep as fr

from pyiron_workflow._wfms import lexical

if TYPE_CHECKING:
    from pyiron_workflow._wfms.datatypes import Node


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


ResultType = TypeVar("ResultType", bound=fr.schemas.NodeData[Any])


class Step(NamedTuple):
    label: fr.schemas.Label
    run: Run[Any]


def _make_steps():
    return Steps()


@dataclasses.dataclass
class Run(Generic[ResultType]):
    lexical_path: lexical.LexicalPath
    result: ResultType
    status: RunStatus
    exception: BaseException | ExceptionGroup | None = None
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
    prime_mover: lexical.LexicalPath
    progress_dir: pathlib.Path = pathlib.Path.cwd()
    progress_hooks: Iterable[
        Callable[[pathlib.Path, datetime.datetime, str, RunStatus], None]
    ] = dataclasses.field(default_factory=list)
    exception_hooks: Iterable[
        Callable[[pathlib.Path, Run[ResultType], BaseException], None]
    ] = dataclasses.field(default_factory=list)
    dag_layers_multithreaded: bool = True
    dag_layers_max_threads: int = 10
    dag_layers_fail_fast: bool = False

    def emit_progress(
        self, time: datetime.datetime, lexical_path: str, status: RunStatus
    ):
        for hook in self.progress_hooks:
            hook(self.progress_dir, time, lexical_path, status)

    def emit_exception(self, failed_run: Run[ResultType], exception: BaseException):
        for hook in self.exception_hooks:
            hook(self.progress_dir, failed_run, exception)

    def is_prime_mover(self, candidate: Node[Any, Any]) -> bool:
        return candidate.lexical_path == self.prime_mover


@dataclasses.dataclass
class ExecutorInstructions:
    constructor: type[futures.Executor]
    args: tuple[Any, ...] = dataclasses.field(default_factory=tuple)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    def instantiate(self) -> futures.Executor:
        return self.constructor(*self.args, **self.kwargs)


def run(
    node: Node,
    config: RunConfig | None = None,
    _current_run: Run[ResultType] | None = None,
    /,
    **input_data,
):
    if config is None:
        config = RunConfig(
            prime_mover=node.lexical_path,
            progress_dir=pathlib.Path.cwd(),
            progress_hooks=[],
        )

    if _current_run is None:
        current_run = Run[ResultType](
            lexical_path=lexical.LexicalPath(node.label),
            result=node.generate_flowrep_live_node(),
            status=RunStatus.PENDING,
            progress_dir=config.progress_dir,
        )
    else:
        current_run = _current_run

    current_run.started_at = datetime.datetime.now()
    current_run.status = RunStatus.RUNNING
    config.emit_progress(
        current_run.started_at, current_run.lexical_path, current_run.status
    )
    try:
        populate_input_ports(current_run.result, input_data)
        if node.executor is None:
            node.evaluate(current_run, config)
        else:
            # Across-process: copy back rather than rebind
            f = _submit(node, current_run, config)
            returned, encountered_exception = f.result()
            _copy_run_fields(returned, into=current_run)
            if encountered_exception:
                raise encountered_exception
        current_run.status = RunStatus.FINISHED
    except BaseException as e:
        current_run.exception = e
        current_run.status = RunStatus.FAILED
        if config.is_prime_mover(node):
            config.emit_exception(current_run, e)
        raise
    finally:
        current_run.finished_at = datetime.datetime.now()
        config.emit_progress(
            current_run.finished_at, current_run.lexical_path, current_run.status
        )
    return current_run


def _submit(
    node: Node, current_run: Run[ResultType], config: RunConfig
) -> futures.Future:
    if isinstance(node.executor, ExecutorInstructions):
        with node.executor.instantiate() as exe:
            f = exe.submit(
                _return_mutated_state_with_any_exception, node, current_run, config
            )
    elif isinstance(node.executor, futures.Executor):
        f = node.executor.submit(
            _return_mutated_state_with_any_exception, node, current_run, config
        )
    else:
        raise TypeError(
            f"Expected executor to be an instance of ExecutorInstructions or "
            f"futures.Executor, but {node.lexical_path!r} got {node.executor}."
        )
    return f


def _return_mutated_state_with_any_exception(
    node: Node[Any, ResultType], current_run: Run[ResultType], config: RunConfig
) -> tuple[Run[ResultType], BaseException | None]:
    """
    If an out-of-process evaluation fails, we have no way of recovering its
    state-so-far from the parent process; so we need to set its failure state and
    trigger exception hooks right there in that remote process.

    For successes or in-process routines (i.e., multithreading), the state management
    in the main `run` routine is sufficient.
    """
    try:
        node.evaluate(current_run, config)
        return current_run, None
    except BaseException as e:
        current_run.exception = e
        current_run.status = RunStatus.FAILED
        current_run.finished_at = datetime.datetime.now()
        return current_run, e


def _copy_run_fields(from_run: Run[ResultType], into: Run[ResultType]) -> None:
    vars(into.result).update(vars(from_run.result))
    into.status = from_run.status
    into.exception = from_run.exception
    into.started_at = from_run.started_at
    into.finished_at = from_run.finished_at
    into.steps = from_run.steps


def populate_input_ports(node: fr.schemas.NodeData, values: dict[str, Any]) -> None:
    for name, val in values.items():
        if name in node.input_ports:
            node.input_ports[name].value = val
        else:
            raise ValueError(
                f"Input port '{name}' not found -- please select among "
                f"{node.recipe.inputs}"
            )
