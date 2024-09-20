"""
A module to extract encapsulate for complex run mechanics, such as status, executor
interaction, etc.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Executor as StdLibExecutor, Future, ThreadPoolExecutor
from time import sleep
from typing import Any, Optional

from pyiron_workflow.mixin.has_interface_mixins import HasLabel, HasRun, UsesState


class ReadinessError(ValueError):
    """
    To be raised when :class:`Runnable` calls run and requests a readiness check, but
    isn't :attr:`ready`.
    """


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
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = False
        self.failed = False
        self.executor = None
        # We call it an executor, but it's just whether to use one.
        # This is a simply stop-gap as we work out more sophisticated ways to reference
        # (or create) an executor process without ever trying to pickle a `_thread.lock`
        self.future: None | Future = None
        self._thread_pool_sleep_time = 1e-6

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

    def process_run_result(self, run_output):
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
    def readiness_report(self) -> str:
        """A human-readable summary of the readiness to run."""
        report = (
            f"{self.label} readiness: {self.ready}\n"
            f"STATE:\n"
            f"running: {self.running}\n"
            f"failed: {self.failed}\n"
        )
        return report

    def executor_shutdown(self, wait=True, *, cancel_futures=False):
        """Invoke shutdown on the executor (if present)."""
        try:
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        except AttributeError:
            pass

    def run(
        self,
        check_readiness: bool = True,
        _finished_callback: Optional[callable] = None,
        **kwargs,
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
            _finished_callback (callable): What to do with the output of :meth:`on_run`
                after the execution is complete (including waiting for the future to
                finish!). This method is responsible for updating the status
                (:attr:`running`/:attr:`failed`) and should only be set by expert users.
                (Default is :meth:`_finish_run`.)
        """
        stop_early, result = self._before_run(check_readiness=check_readiness, **kwargs)
        if stop_early:
            return result

        self.running = True
        try:
            out = self._run(
                finished_callback=(
                    self._finish_run
                    if _finished_callback is None
                    else _finished_callback
                ),
            )
            return out
        except Exception as e:
            self.failed = True
            out = None
            raise e
        finally:
            # Leave the status as running if the method returns a future
            self.running = isinstance(out, Future)

    def _before_run(self, /, check_readiness, **kwargs) -> tuple[bool, Any]:
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
            raise ReadinessError(self._readiness_error_message)
        return False, None

    @property
    def _readiness_error_message(self) -> str:
        return (
            f"{self.label} received a run command but is not ready. The runnable "
            f"should be neither running nor failed.\n" + self.readiness_report
        )

    def _run(
        self,
        finished_callback: callable,
    ) -> Any | tuple | Future:
        args, kwargs = self.run_args
        if "self" in kwargs.keys():
            raise ValueError(
                f"{self.label} got 'self' as a run kwarg, but self is already the "
                f"first positional argument passed to :meth:`on_run`."
            )
        if self.executor is None:
            run_output = self.on_run(*args, **kwargs)
            return finished_callback(run_output)
        else:
            executor = self._parse_executor(self.executor)
            if isinstance(self.executor, ThreadPoolExecutor):
                self.future = executor.submit(self.thread_pool_run, *args, **kwargs)
            else:
                self.future = executor.submit(self.on_run, *args, **kwargs)
            self.future.add_done_callback(finished_callback)
            return self.future

    def thread_pool_run(self, *args, **kwargs):
        #
        result = self.on_run(*args, **kwargs)
        sleep(self._thread_pool_sleep_time)
        return result

    @staticmethod
    def _parse_executor(executor) -> StdLibExecutor:
        """
        We may want to allow users to specify how to build an executor rather than
        actually providing an executor instance -- so here we can interpret these.

        NOTE:
            `concurrent.futures.Executor` _won't_ actually work, because we need
            stuff with :mod:`cloudpickle` support. We're leaning on this for a guaranteed
            interface (has `submit` and returns a `Future`), and leaving it to the user
            to provide an executor that will actually work!!!

        NOTE:
            If, in the future, this parser is extended to instantiate new executors from
            instructions, these new instances may not be caught by the
            `executor_shutdown` method. This will require some re-engineering to make
            sure we don't have dangling executors.
        """
        if isinstance(executor, StdLibExecutor):
            return executor
        else:
            raise NotImplementedError(
                f"Expected an instance of {StdLibExecutor}, but got {executor}."
            )

    def _finish_run(self, run_output: tuple | Future) -> Any | tuple:
        """
        Switch the status, then process and return the run result.

        Sets the :attr:`failed` status to true if an exception is encountered.
        """
        if isinstance(run_output, Future):
            run_output = run_output.result()

        self.running = False
        try:
            processed_output = self.process_run_result(run_output)
            return processed_output
        except Exception as e:
            self.failed = True
            raise e

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
