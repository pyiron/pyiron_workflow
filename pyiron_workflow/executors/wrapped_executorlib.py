import inspect
from typing import Any, ClassVar

from executorlib import BaseExecutor, SingleNodeExecutor, SlurmClusterExecutor
from executorlib.api import TestClusterExecutor

from pyiron_workflow.mixin import lexical, run


class DedicatedExecutorError(TypeError):
    """
    To raise when you try to use one of these executors outside the context of a node.
    """


class ProtectedResourceError(ValueError):
    """
    Raise when a user provides executorlib resources that we need to override.
    """


class CacheOverride(BaseExecutor):
    override_cache_file_name: ClassVar[str] = "executorlib_cache"

    def submit(self, fn, /, *args, **kwargs):
        """
        We demand that `fn` be the bound-method `on_run` of a `Lexical`+`Runnable`
        class (a `Node` is, of course, the intended resolution of this demand).
        """
        if (
            inspect.ismethod(fn)
            and fn.__name__ == "on_run"
            and isinstance(fn.__self__, lexical.Lexical)  # provides .as_path
            and isinstance(fn.__self__, run.Runnable)  # provides .on_run
        ):
            cache_key_info = {
                "cache_key": self.override_cache_file_name,
                "cache_directory": str(fn.__self__.as_path()),
            }
        else:
            raise DedicatedExecutorError(
                f"{self.__class__.__name__} is only intended to work with the "
                f"on_run method of pyiron_workflow.Node objects, but got {fn}"
            )

        _validate_existing_resource_dict(kwargs)

        if "resource_dict" in kwargs:
            kwargs["resource_dict"].update(cache_key_info)
        else:
            kwargs["resource_dict"] = cache_key_info

        return super().submit(fn, *args, **kwargs)


def _validate_existing_resource_dict(kwargs: dict[str, Any]):
    if "resource_dict" in kwargs:
        if "cache_key" in kwargs["resource_dict"]:
            raise ProtectedResourceError(
                f"pyiron_workflow needs the freedom to specify the cache, so the "
                f'requested "cache_directory" '
                f"({kwargs['resource_dict']['cache_key']}) would get overwritten."
            )
        if "cache_directory" in kwargs["resource_dict"]:
            raise ProtectedResourceError(
                f"pyiron_workflow needs the freedom to specify the cache, so the "
                f'requested "cache_directory" '
                f"({kwargs['resource_dict']['cache_directory']})would get "
                f"overwritten."
            )


class NodeSingleExecutor(CacheOverride, SingleNodeExecutor): ...


class NodeSlurmExecutor(CacheOverride, SlurmClusterExecutor): ...


class _CacheTestClusterExecutor(CacheOverride, TestClusterExecutor): ...


extra_info = """
This is a wrapper around executorlib's executors that is designed to only work with 
the submission of :mod:`pyiron_workflow` node calculations, and manipulates the 
:mod:`executorlib` caching information to exploit the lexical path of the node.
"""


def wrapped_executorlib_class(wrapped_class: type[BaseExecutor]):
    base_doc = wrapped_class.__doc__ or ""
    return type(
        "Wrapped" + wrapped_class.__name__,
        (
            CacheOverride,
            wrapped_class,
        ),
        {
            "__doc__": base_doc + extra_info,
            "__module__": wrapped_class.__module__,
        },
    )


def wrapped_executorlib(
    wrapped_class: type[BaseExecutor],
    *,
    resource_dict: dict[str, Any] | None = None,
    **kwargs,
):
    kwargs.update({"resource_dict": resource_dict})
    return wrapped_executorlib_class(wrapped_class)(**kwargs)
