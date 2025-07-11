import inspect
from typing import ClassVar

from executorlib import BaseExecutor, SingleNodeExecutor, SlurmClusterExecutor

from pyiron_workflow.mixin import lexical, run


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
            raise TypeError(
                f"{self.__name__} is only intended to work with the "
                f"on_run method of pyiron_workflow.Node objects, but got {fn}"
            )

        if "resource_dict" in kwargs:
            if "cache_key" in kwargs["resource_dict"]:
                raise ValueError(
                    f"pyiron_workflow needs the freedom to specify the cache, so the "
                    f'requested "cache_directory" '
                    f"({kwargs['resource_dict']['cache_key']}) would get overwritten."
                )
            if "cache_directory" in kwargs["resource_dict"]:
                raise ValueError(
                    f"pyiron_workflow needs the freedom to specify the cache, so the "
                    f'requested "cache_directory" '
                    f"({kwargs['resource_dict']['cache_directory']})would get "
                    f"overwritten."
                )
            kwargs["resource_dict"].update(cache_key_info)
        else:
            kwargs["resource_dict"] = cache_key_info

        return super().submit(fn, *args, **kwargs)


class CacheSingleNodeExecutor(SingleNodeExecutor, CacheOverride): ...


class CacheSlurmClusterExecutor(SlurmClusterExecutor, CacheOverride): ...


from typing import Callable, Optional

from executorlib.executor.base import BaseExecutor
from executorlib.task_scheduler.file.subprocess_spawner import execute_in_subprocess
from executorlib.task_scheduler.file.task_scheduler import FileTaskScheduler


class LocalFileExecutor(BaseExecutor):
    def __init__(
        self,
        max_workers: Optional[int] = None,
        cache_directory: Optional[str] = None,
        max_cores: Optional[int] = None,
        resource_dict: Optional[dict] = None,
        hostname_localhost: Optional[bool] = None,
        block_allocation: bool = False,
        init_function: Optional[Callable] = None,
        disable_dependencies: bool = False,
        refresh_rate: float = 0.01,
    ):
        default_resource_dict: dict = {
            "cores": 1,
            "threads_per_core": 1,
            "gpus_per_core": 0,
            "cwd": None,
            "openmpi_oversubscribe": False,
            "slurm_cmd_args": [],
        }
        if cache_directory is None:
            default_resource_dict["cache_directory"] = "executorlib_cache"
        else:
            default_resource_dict["cache_directory"] = cache_directory
        if resource_dict is None:
            resource_dict = {}
        resource_dict.update(
            {k: v for k, v in default_resource_dict.items() if k not in resource_dict}
        )
        super().__init__(
            executor=FileTaskScheduler(
                resource_dict=resource_dict,
                pysqa_config_directory=None,
                backend=None,
                disable_dependencies=disable_dependencies,
                execute_function=execute_in_subprocess,
            )
        )


class CacheLocalFileExecutor(LocalFileExecutor, CacheOverride): ...
