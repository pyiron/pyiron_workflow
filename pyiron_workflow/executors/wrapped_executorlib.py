import inspect

from executorlib import BaseExecutor, SingleNodeExecutor

from pyiron_workflow.mixin import lexical, run


class CacheOverride(BaseExecutor):
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
                "cache_key": str(fn.__self__.as_path()),
                "cache_directory": ".",  # Doesn't matter, the path in the key overrides
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
