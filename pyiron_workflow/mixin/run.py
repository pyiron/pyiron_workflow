"""
A module to extract encapsulate for complex run mechanics, such as status, executor
interaction, etc.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import Executor as StdLibExecutor
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from time import sleep
from typing import Any

from pyiron_workflow.mixin.has_interface_mixins import HasLabel, HasRun, UsesState


class ReadinessError(ValueError):
    """
    To be raised when :class:`Runnable` calls run and requests a readiness check, but
    isn't :attr:`ready`.
    """

    readiness_dict: dict[str, bool]  # Detailed information on why it is not ready


class Runnable(UsesState, HasLabel, HasRun, ABC):
    """
    An abstract class for interfacing with executors, etc.

    Child classes must define :meth:`on_run` and :attr:`.Runnable.run_args`, then the
    :meth:`run` will invoke `self.on_run(*run_args[0], **run_args[1])`. The
    :class:`Runnable` class then handles the status of the run, passing the call off
    for remote execution, handling any returned futures object, etc.

    Child classes can optionally override :meth:`process_run_result` to do something
    with the returned value of :meth:`on_run`, but by default the returned value just
    passes cleanly through the function.

    The `run` cycle is broken down into sub-steps:
    - `_before_run`: prior to the `running` status being set to `True`
    - `_run`: after the `running` status has been set to `True`
    - `_finish_run`: what is done to the results of running, and when `running` is
        set to `False`
    - `_run_exception`: What to do if an encountered
    - `_run_finally`: What to do after _every_ run, regardless of whether an exception
        was encountered

    Child classes can extend the behavior of these sub-steps, including introducing
    new keyword arguments.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.running: bool = False
        self.failed: bool = False
        self.executor: (
            StdLibExecutor | tuple[Callable[..., StdLibExecutor], tuple, dict] | None
        ) = None
        # We call it an executor, but it can also be instructions on making one
        self.future: None | Future = None
        self._thread_pool_sleep_time: float = 1e-6

    @abstractmethod
    def on_run(self, *args, **kwargs) -> Any:  # callable[..., Any | tuple]:
        """
        What the :meth:`run` method actually does!
        """

    @property
    @abstractmethod
    def run_args(self) -> tuple[tuple, dict]:
        """
        Any data needed for :meth:`on_run`, will be passed as (*args, **kwargs).
        """

    def process_run_result(self, run_output: Any) -> Any:
        """
        What to _do_ with the results of :meth:`on_run` once you have them.

        By extracting this as a separate method, we allow the runnable to pass the
        actual execution off to another entity and release the python process to do
        other things. In such a case, this function should be registered as a callback
        so that the runnable can process the result of that process.

        Args:
            run_output: The results of a `self.on_run(self.run_args)` call.
        """
        return run_output

    @property
    def ready(self) -> bool:
        return not (self.running or self.failed)

    @property
    def _readiness_dict(self) -> dict[str, bool]:
        return {
            "ready": self.ready,
            "running": self.running,
            "failed": self.failed,
        }

    @property
    def readiness_report(self) -> str:
        """A human-readable summary of the readiness to run."""
        report = f"{self.label} readiness report:\n"
        for k, v in self._readiness_dict.items():
            report += f"{k}: {v}\n"
        return report

    def executor_shutdown(self, wait=True, *, cancel_futures=False):
        """Invoke shutdown on the executor (if present)."""
        with contextlib.suppress(AttributeError):
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def run(
        self,
        check_readiness: bool = True,
        raise_run_exceptions: bool = True,
        before_run_kwargs: dict | None = None,
        run_kwargs: dict | None = None,
        run_exception_kwargs: dict | None = None,
        run_finally_kwargs: dict | None = None,
        finish_run_kwargs: dict | None = None,
    ) -> Any | tuple | Future:
        """
        Checks that the runnable is :attr:`ready` (if requested), then executes the
        functionality of defined in :meth:`on_run` by passing it whatever is returned
        by :meth:`run_args`.

        Can stop early if :meth:`_before_run` called here returns `True` as its first
        argument.

        Handles the status of the runnable, communicating with any remote
        computing resources, and processing the result.

        Args:
            check_readiness (bool): Whether to raise a `ReadinessError` if not
                :attr:`ready`. (Default is True.)
            raise_run_exceptions (bool): Whether to raise exceptions encountered while
                :attr:`running`. (Default is True.)
        """

        def _none_to_dict(inp: dict | None) -> dict:
            return {} if inp is None else inp

        before_run_kwargs = _none_to_dict(before_run_kwargs)
        run_kwargs = _none_to_dict(run_kwargs)
        run_exception_kwargs = _none_to_dict(run_exception_kwargs)
        run_finally_kwargs = _none_to_dict(run_finally_kwargs)
        finish_run_kwargs = _none_to_dict(finish_run_kwargs)

        stop_early, result = self._before_run(
            check_readiness=check_readiness, **before_run_kwargs
        )
        if stop_early:
            return result

        executor = (
            None if self.executor is None else self._parse_executor(self.executor)
        )

        self.running = True
        return self._run(
            executor=executor,
            raise_run_exceptions=raise_run_exceptions,
            run_exception_kwargs=run_exception_kwargs,
            run_finally_kwargs=run_finally_kwargs,
            finish_run_kwargs=finish_run_kwargs,
            **run_kwargs,
        )

    def _before_run(
        self, /, check_readiness: bool, *args, **kwargs
    ) -> tuple[bool, Any]:
        """
        Things to do _before_ running.

        Args:
            check_readiness (bool): Whether to raise a `ReadinessError` if not
                :attr:`ready`.
            **kwargs: Keyword arguments used by child classes in overriding this
                function.

        Returns:
            (bool): Whether to exit the parent run call early.
            (Any): What to return on an early-exit.

        Raises:
            (ReadinessError): If :param:`check_readiness` but not :attr:`ready`.
        """
        if check_readiness and not self.ready:
            readiness_error = ReadinessError(self._readiness_error_message)
            readiness_error.readiness_dict = self._readiness_dict
            raise readiness_error
        return False, None

    def _run(
        self,
        /,
        executor: StdLibExecutor | None,
        raise_run_exceptions: bool,
        run_exception_kwargs: dict,
        run_finally_kwargs: dict,
        finish_run_kwargs: dict,
        *args,
        **kwargs,
    ) -> Any | tuple | Future:
        """
        What happens while the status is :attr:`running`, namely invoking
        :meth:`self.on_run` using :attr:`self.run_args`, either locally or on an
        executor.

        Args:
            executor (concurrent.futures.Executor|None): Optionally, executor on which
                to run.
            raise_run_exceptions (bool): Whether to raise encountered exceptions.

        Returns:
            (Any | Future): The result of :meth:`on_run`, or a futures object from
                the executor.
        """
        on_run_args, on_run_kwargs = self.run_args
        if "self" in on_run_kwargs:
            raise ValueError(
                f"{self.label} got 'self' as a run kwarg, but self is already the "
                f"first positional argument passed to :meth:`on_run`."
            )

        if executor is None:
            try:
                run_output = self.on_run(*on_run_args, **on_run_kwargs)
            except (Exception, KeyboardInterrupt) as e:
                self._run_exception(**run_exception_kwargs)
                self._run_finally(**run_finally_kwargs)
                if raise_run_exceptions:
                    raise e
                else:
                    run_output = None
            return self._finish_run(
                run_output,
                raise_run_exceptions=raise_run_exceptions,
                run_exception_kwargs=run_exception_kwargs,
                run_finally_kwargs=run_finally_kwargs,
                **finish_run_kwargs,
            )
        else:
            if isinstance(executor, ThreadPoolExecutor):
                self.future = executor.submit(
                    self._thread_pool_run, *on_run_args, **on_run_kwargs
                )
            else:
                self.future = executor.submit(
                    self.on_run, *on_run_args, **on_run_kwargs
                )
            self.future.add_done_callback(
                partial(
                    self._finish_run,
                    raise_run_exceptions=raise_run_exceptions,
                    run_exception_kwargs=run_exception_kwargs,
                    run_finally_kwargs=run_finally_kwargs,
                    **finish_run_kwargs,
                )
            )
            return self.future

    def _run_exception(self, /, *args, **kwargs):
        """
        What to do if an exception is encountered inside :meth:`_run` or
        :meth:`_finish_run.
        """
        self.running = False
        self.failed = True

    def _run_finally(self, /, *args, **kwargs):
        """
        What to do after :meth:`_finish_run` (whether an exception is encountered or
        not), or in :meth:`_run` after an exception is encountered.
        """

    def _finish_run(
        self,
        run_output: tuple | Future,
        /,
        raise_run_exceptions: bool,
        run_exception_kwargs: dict,
        run_finally_kwargs: dict,
        **kwargs,
    ) -> Any | tuple | None:
        """
        Switch the status, then process and return the run result.
        """
        self.running = False
        try:
            if isinstance(run_output, Future):
                run_output = run_output.result()
            return self.process_run_result(run_output)
        except Exception as e:
            self._run_exception(**run_exception_kwargs)
            if raise_run_exceptions:
                raise e
            return None
        finally:
            self._run_finally(**run_finally_kwargs)

    def _thread_pool_run(self, *args, **kwargs):
        """
        A poor attempt at avoiding (probably) thread races
        """
        result = self.on_run(*args, **kwargs)
        sleep(self._thread_pool_sleep_time)
        return result

    @property
    def _readiness_error_message(self) -> str:
        return (
            f"{self.label} received a run command but is not ready. The runnable "
            f"should be neither running nor failed.\n" + self.readiness_report
        )

    @staticmethod
    def _parse_executor(
        executor: StdLibExecutor | tuple[Callable[..., StdLibExecutor], tuple, dict],
    ) -> StdLibExecutor:
        """
        If you've already got an executor, you're done. But if you get callable and
        some args and kwargs, turn them into an executor!

        This is because executors can't be serialized, but you might want to use an
        executor on the far side of serialization. The most straightforward example is
        to simply pass an executor class and its args and kwargs, but in a more
        sophisticated case perhaps you want some function that accesses the _same_
        executor on multiple invocations such that multiple nodes are sharing the same
        executor. The functionality here isn't intended to hold your hand for this, but
        should be flexible enough that you _can_ do it if you want to.
        """
        if isinstance(executor, StdLibExecutor):
            return executor
        elif (
            isinstance(executor, tuple)
            and callable(executor[0])
            and isinstance(executor[1], tuple)
            and isinstance(executor[2], dict)
        ):
            executor = executor[0](*executor[1], **executor[2])
            if not isinstance(executor, StdLibExecutor):
                raise TypeError(
                    f"Executor parsing got a callable and expected it to return a "
                    f"`concurrent.futures.Executor` instance, but instead got "
                    f"{executor}."
                )
            return executor
        else:
            raise NotImplementedError(
                f"Expected an instance of {StdLibExecutor}, or a tuple of such a class, "
                f"a tuple of args, and a dict of kwargs -- but got {executor}."
            )

    def __getstate__(self):
        state = super().__getstate__()
        state["future"] = None
        # Don't pass the future -- with the future in the state things work fine for
        # the simple pyiron_workflow.executors.CloudpickleProcessPoolExecutor, but for
        # the more complex executorlib.Executor we're getting:
        # TypeError: cannot pickle '_thread.RLock' object
        if isinstance(self.executor, StdLibExecutor):
            state["executor"] = None
        # Don't pass actual executors, they have an unserializable thread lock on them
        # _but_ if the user is just passing instructions on how to _build_ an executor,
        # we'll trust that those serialize OK (this way we can, hopefully, eventually
        # support nesting executors!)
        return state
