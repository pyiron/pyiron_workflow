from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import _global_shutdown, _WorkItem, BrokenProcessPool
from sys import version_info

import cloudpickle


class CloudLoadsFuture(Future):
    def result(self, timeout=None):
        result = super().result(timeout=timeout)
        if isinstance(result, bytes):
            result = cloudpickle.loads(result)
        return result


class _CloudPickledCallable:
    def __init__(self, fnc: callable):
        self.fnc_serial = cloudpickle.dumps(fnc)

    def __call__(self, /, dumped_args, dumped_kwargs):
        fnc = cloudpickle.loads(self.fnc_serial)
        args = cloudpickle.loads(dumped_args)
        kwargs = cloudpickle.loads(dumped_kwargs)
        return cloudpickle.dumps(fnc(*args, **kwargs))

    @classmethod
    def dumps(cls, stuff):
        return cloudpickle.dumps(stuff)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """
    This class wraps :class:`concurrent.futures.ProcessPoolExecutor` such that the submitted
    callable, its arguments, and its return value are all pickled using :mod:`cloudpickle`.
    In this way, the executor extends support to all objects which are cloud-pickleable,
    e.g. dynamically defined or decorated classes.

    To accomplish this, the underlying :class:`concurrent.futures.Future` class used is
    replaced with our :class:`CloudLoadsFuture`, which is identical except that calls to
    :meth:`result()` will first try to :func:`cloudpickle.loads` and `bytes` results found.

    Examples:
        Consider a class created from a function dynamically with a decorator.
        These are not normally pickleable, so in this example we should how this class
        allows us to submit a method from such a class, that both takes as an argument
        and returns such an unpickleable class.
        Actions such as registering callbacks and waiting for results behave just like
        normal.

        >>> from functools import partialmethod
        >>>
        >>> from pyiron_workflow.executors.cloudpickleprocesspool import (
        ...     CloudpickleProcessPoolExecutor
        ... )
        >>>
        >>> class Foo:
        ...     '''
        ...     A base class to be dynamically modified for testing our executor.
        ...     '''
        ...     def __init__(self, fnc: callable):
        ...         self.fnc = fnc
        ...         self.result = None
        ...
        ...     @property
        ...     def run(self):
        ...         return self.fnc
        ...
        ...     def process_result(self, future):
        ...         self.result = future.result()
        >>>
        >>>
        >>> def dynamic_foo():
        ...     '''
        ...     A decorator for dynamically modifying the Foo class.
        ...
        ...     Overrides the `fnc` input of `Foo` with the decorated function.
        ...     '''
        ...     def as_dynamic_foo(fnc: callable):
        ...         return type(
        ...             "DynamicFoo",
        ...             (Foo,),  # Define parentage
        ...             {
        ...                 "__init__": partialmethod(
        ...                     Foo.__init__,
        ...                     fnc
        ...                 )
        ...             },
        ...         )
        ...
        ...     return as_dynamic_foo
        >>>
        >>> @dynamic_foo()
        ... def UnpicklableCallable(unpicklable_arg):
        ...     unpicklable_arg.result = "This was an arg"
        ...     return unpicklable_arg
        >>>
        >>>
        >>> instance = UnpicklableCallable()
        >>> arg = UnpicklableCallable()
        >>> executor = CloudpickleProcessPoolExecutor()
        >>> fs = executor.submit(instance.run, arg)
        >>> fs.add_done_callback(instance.process_result)
        >>> print(fs.done())
        False

        >>> print(fs.result().__class__.__name__)
        DynamicFoo

        >>> print(fs.done())
        True

        >>> import time
        >>> time.sleep(1)  # Debugging doctest on github CI for python3.10
        >>> print(instance.result.result)
        This was an arg

    """

    def submit(self, fn, /, *args, **kwargs):
        return self._submit(
            _CloudPickledCallable(fn),
            _CloudPickledCallable.dumps(args),
            _CloudPickledCallable.dumps(kwargs),
        )

    submit.__doc__ = ProcessPoolExecutor.submit.__doc__

    def _submit(self, fn, /, *args, **kwargs):
        """
        We override the regular `concurrent.futures.ProcessPoolExecutor` to use our
        custom future that unpacks cloudpickled results.

        This approach is simple, but the brute-force nature of it means we manually
        accommodate different implementations of `ProcessPoolExecutor` in different
        python versions.
        """
        if version_info.major != 3:
            raise RuntimeError(
                f"{self.__class__} is only built for python3, but got "
                f"{version_info.major}"
            )

        if version_info.minor == 8:
            return self._submit_3_8(fn, *args, **kwargs)
        elif version_info.minor >= 9:
            return self._submit_3_gt9(fn, *args, **kwargs)
        else:
            raise RuntimeError(
                f"{self.__class__} is only built for python 3.8+, but got "
                f"{version_info.major}.{version_info.minor}."
            )

    def _submit_3_gt9(self, fn, /, *args, **kwargs):
        with self._shutdown_lock:
            if self._broken:
                raise BrokenProcessPool(self._broken)
            if self._shutdown_thread:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if _global_shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after " "interpreter shutdown"
                )

            f = CloudLoadsFuture()
            w = _WorkItem(f, fn, args, kwargs)

            self._pending_work_items[self._queue_count] = w
            self._work_ids.put(self._queue_count)
            self._queue_count += 1
            # Wake up queue management thread
            self._executor_manager_thread_wakeup.wakeup()

            if self._safe_to_dynamically_spawn_children:
                self._adjust_process_count()
            self._start_executor_manager_thread()
            return f

    def _submit_3_8(*args, **kwargs):
        if len(args) >= 2:
            self, fn, *args = args
        elif not args:
            raise TypeError(
                "descriptor 'submit' of 'ProcessPoolExecutor' object "
                "needs an argument"
            )
        elif "fn" in kwargs:
            fn = kwargs.pop("fn")
            self, *args = args
            import warnings

            warnings.warn(
                "Passing 'fn' as keyword argument is deprecated",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            raise TypeError(
                "submit expected at least 1 positional argument, "
                "got %d" % (len(args) - 1)
            )

        with self._shutdown_lock:
            if self._broken:
                raise BrokenProcessPool(self._broken)
            if self._shutdown_thread:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if _global_shutdown:
                raise RuntimeError(
                    "cannot schedule new futures after " "interpreter shutdown"
                )

            f = CloudLoadsFuture()
            w = _WorkItem(f, fn, args, kwargs)

            self._pending_work_items[self._queue_count] = w
            self._work_ids.put(self._queue_count)
            self._queue_count += 1
            # Wake up queue management thread
            self._queue_management_thread_wakeup.wakeup()

            self._start_queue_management_thread()
            return f
