"""
Simple wrappers around executorlib executors, which are exclusively intended to
submit the core execution routine, and which override executorlib caching directory with
the run configuration directory and cache file roots with the lexical path.

This makes it possible to recover executorlib-cached data between python processes
(e.g. shutting down your notebook and coming back), but the onus is on the _user_ to
specify the right terminal input data to correspond with this run configuration
directory -- we don't do any data hashing.
I.e., make a fresh run config directory for each iteration of input data where the
caching executors are being leveraged.
"""

from typing import ClassVar

import executorlib
import executorlib.api as exlib_api

from pyiron_workflow._wfms import execution


class DedicatedExecutorError(TypeError):
    """
    To raise when you try to use one of these executors outside the context of a node.
    """


class ProtectedResourceError(ValueError):
    """
    Raise when a user provides executorlib resources that we need to override.
    """


class CacheOverride(executorlib.BaseExecutor):
    cache_directory: ClassVar[str] = "executorlib_cache"

    def submit(self, fn, /, *args, **kwargs):
        """
        Modify behaviour when submitting for a pyiron_workflow execution loop
        """
        if not self._recognized_submission(fn) or len(args) != 3 or len(kwargs) != 0:
            raise DedicatedExecutorError(
                f"{self.__class__.__name__} is only intended to work with the "
                f"run routine of pyiron_workflow: "
                f"{execution._return_mutated_state_with_any_exception.__module__}."
                f"{execution._return_mutated_state_with_any_exception.__qualname__}, and "
                f"its three expected arguments, but got submitted {fn!r} with input "
                f"{args!r}, and {kwargs!r}"
            )

        node, _, config = args
        cache_key_info = {
            "cache_key": node.lexical_path,
            "cache_directory": str(config.run_dir / self.cache_directory),
        }
        super_kwargs = {"resource_dict": cache_key_info}

        return super().submit(fn, *args, **super_kwargs)

    @staticmethod
    def _recognized_submission(fn):
        return fn is execution._return_mutated_state_with_any_exception or (
            type(fn).__name__ == "BoundWrapper"
            and getattr(fn, "func", None)
            is execution._return_mutated_state_with_any_exception
        )


class NodeSingleExecutor(CacheOverride, executorlib.SingleNodeExecutor): ...


class NodeSlurmExecutor(CacheOverride, executorlib.SlurmClusterExecutor): ...


class _CacheTestExecutor(CacheOverride, exlib_api.TestClusterExecutor): ...
