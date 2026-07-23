from __future__ import annotations

import atexit
import contextlib
import dataclasses
import datetime
import enum
import logging
import multiprocessing
import pathlib
import threading
from collections.abc import Callable, Iterable
from concurrent import futures
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeAlias, TypeVar

import flowrep as fr
from pyiron_snippets import dotdict, import_alarm

from pyiron_workflow._wfms import lexical

with import_alarm.ImportAlarm(
    "Using a fleche-cache requires 'fleche'.", raise_exception=True
) as _import_alarm:
    import fleche
    from fleche.caches import BaseCache, Cache

if TYPE_CHECKING:
    from pyiron_workflow._wfms.datatypes import Node


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


_hook_pool: futures.ThreadPoolExecutor | None = None
_hook_pool_lock = threading.Lock()


def _get_hook_pool(max_threads: int) -> futures.ThreadPoolExecutor:
    """Lazily build the per-process, non-blocking-hook thread pool.

    A ``ThreadPoolExecutor`` is process-local, so each process (parent and any
    executor worker) gets its own singleton; ``atexit`` drains it on exit. The
    pool is sized on first creation; later ``max_threads`` values are ignored
    for the lifetime of that process.
    """
    global _hook_pool  # noqa: PLW0603 -- deliberate per-process pool singleton
    with _hook_pool_lock:
        if _hook_pool is None:
            _hook_pool = futures.ThreadPoolExecutor(max_workers=max_threads)
            atexit.register(_shutdown_hook_pool)
        return _hook_pool


def _shutdown_hook_pool() -> None:
    """Drain and discard the hook pool. Registered with ``atexit``; also the
    reset hook tests call in ``tearDown``."""
    global _hook_pool  # noqa: PLW0603 -- deliberate per-process pool singleton
    with _hook_pool_lock:
        if _hook_pool is not None:
            _hook_pool.shutdown(wait=True)
            _hook_pool = None


def _guarded(
    fn: Callable[[pathlib.Path, datetime.datetime, str, RunStatus], None],
    logger: logging.Logger,
    run_dir: pathlib.Path,
    time: datetime.datetime,
    lexical_path: str,
    status: RunStatus,
) -> None:
    """Run a non-blocking progress hook, logging (not raising) ordinary errors.

    Catches ``Exception`` only, so ``KeyboardInterrupt``/``SystemExit`` are left
    to propagate to the caller (here, the pool worker, which absorbs them into
    its discarded future); a deliberate ``os._exit`` raises nothing so it is
    unaffected.
    """
    try:
        fn(run_dir, time, lexical_path, status)
    except Exception:
        logger.exception(
            "Non-blocking progress hook %r failed for %s @ %s",
            fn,
            lexical_path,
            status,
        )


HookFunction: TypeAlias = Callable[
    [pathlib.Path, datetime.datetime, str, RunStatus], None
]


class ProgressHook(NamedTuple):
    fn: HookFunction
    blocking: bool = False


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
    run_dir: pathlib.Path | None = None
    steps: Steps = dataclasses.field(default_factory=_make_steps)

    @property
    def outputs(self):
        return dotdict.DotDict(
            {k: v.value for k, v in self.result.output_ports.items()}
        )

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
    run_dir: pathlib.Path = pathlib.Path.cwd()
    progress_hooks: Iterable[ProgressHook | HookFunction] = dataclasses.field(
        default_factory=list
    )
    exception_hooks: Iterable[
        Callable[[pathlib.Path, Run[ResultType], BaseException], None]
    ] = dataclasses.field(default_factory=list)
    dag_layers_multithreaded: bool = True
    dag_layers_max_threads: int = 10
    dag_layers_fail_fast: bool = False
    hooks_max_threads: int = 10
    logger_name: str = __name__
    fleche_cache: Cache | None = None
    _prime_mover: lexical.LexicalPath | None = dataclasses.field(
        default=None, kw_only=True
    )

    def __post_init__(self) -> None:
        if self.fleche_cache is not None:
            self._assert_fleche_available()

    @_import_alarm
    def _assert_fleche_available(self) -> None:
        pass

    @property
    def prime_mover(self) -> lexical.LexicalPath:
        if self._prime_mover is None:  # pragma: no cover
            raise ValueError(
                f"No prime mover specified. The only known application of "
                f"{self.__class__.__name__} is inside "
                f"{run.__module__}.{run.__qualname__}, which should manually override "
                f"None-values; this should be unreachable."
            )
        return self._prime_mover

    def emit_progress(
        self, time: datetime.datetime, lexical_path: str, status: RunStatus
    ):
        for hook in self.progress_hooks:
            progress_hook = (
                hook if isinstance(hook, ProgressHook) else ProgressHook(hook)
            )
            if progress_hook.blocking:
                progress_hook.fn(self.run_dir, time, lexical_path, status)
            else:
                _get_hook_pool(self.hooks_max_threads).submit(
                    _guarded,
                    progress_hook.fn,
                    logging.getLogger(self.logger_name),
                    self.run_dir,
                    time,
                    lexical_path,
                    status,
                )

    def emit_exception(self, failed_run: Run[ResultType], exception: BaseException):
        for hook in self.exception_hooks:
            hook(self.run_dir, failed_run, exception)

    def is_prime_mover(self, candidate: Node[Any, Any]) -> bool:
        return candidate.lexical_path == self.prime_mover

    def _fleche_cache_context(self) -> BaseCache | contextlib.AbstractContextManager:
        try:
            return (
                fleche.cache(self.fleche_cache)
                if self.fleche_cache
                else fleche.cache("void")
            )
        except NameError:
            return contextlib.nullcontext()


@dataclasses.dataclass
class ExecutorInstructions:
    constructor: type[futures.Executor]
    args: tuple[Any, ...] = dataclasses.field(default_factory=tuple)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
    start_method: str | None = "spawn"

    def instantiate(self) -> futures.Executor:
        return self.constructor(*self.args, **self._resolved_kwargs())

    def _resolved_kwargs(self) -> dict[str, Any]:
        """:attr:`kwargs`, with a non-``fork`` context forced onto process pools.

        Sibling nodes in a DAG layer are evaluated on separate threads, so pools
        built from these instructions can fork concurrently out of a
        multi-threaded parent. That is unsafe regardless -- the child inherits
        locks held by threads that do not exist in it -- and ``filelock >= 3.30``
        promotes it to an outright ``RuntimeError`` via a process-wide audit
        hook, from nothing more than being imported.

        ``spawn`` is what macOS and Windows already default to, so forcing it
        makes every platform behave alike rather than leaving Linux on the fast,
        unsafe path. CPython 3.14 moves Linux off ``fork`` for the same reason.

        An explicit ``mp_context`` in :attr:`kwargs` always wins, and
        ``start_method=None`` opts out entirely.
        """
        if (
            self.start_method is None
            or "mp_context" in self.kwargs
            or not isinstance(self.constructor, type)
            or not issubclass(self.constructor, futures.ProcessPoolExecutor)
            or self.start_method not in multiprocessing.get_all_start_methods()
        ):
            return self.kwargs
        return {
            **self.kwargs,
            "mp_context": multiprocessing.get_context(self.start_method),
        }


def run(
    node: Node,
    config: RunConfig | None = None,
    _current_run: Run[ResultType] | None = None,
    /,
    **input_data,
):
    if config is None:
        config = RunConfig(_prime_mover=node.lexical_path)
    elif config._prime_mover is None:
        config = dataclasses.replace(config, _prime_mover=node.lexical_path)

    if _current_run is None:
        current_run = Run[ResultType](
            lexical_path=lexical.LexicalPath(node.label),
            result=node.generate_flowrep_live_node(),
            status=RunStatus.PENDING,
            run_dir=config.run_dir,
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
            with config._fleche_cache_context():
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
        with config._fleche_cache_context(), node.executor.instantiate() as exe:
            if config.fleche_cache is not None:
                fleche.wrap_executor(exe)
            f = exe.submit(
                _return_mutated_state_with_any_exception, node, current_run, config
            )
    elif isinstance(node.executor, futures.Executor):
        with config._fleche_cache_context():
            if config.fleche_cache is not None:
                fleche.wrap_executor(node.executor)
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
    node: Node[Any, ResultType], current_run: Run[ResultType], config: RunConfig, /
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
