from concurrent.futures import ProcessPoolExecutor

import cloudpickle


class DotDict(dict):
    def __getattr__(self, item):
        return self.__getitem__(item)

    def __setattr__(self, key, value):
        self[key] = value


def _apply_cloudpickle(fn, /, *args, **kwargs):
    fn = cloudpickle.loads(fn)
    return fn(*args, **kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """
    In our workflows, it is common to dynamically create classes from functions using a
    decorator;
    This makes the underlying function object mismatch with the pickle-findable
    "function" (actually a class after wrapping).
    The result is that a regular `ProcessPoolExecutor` cannot pickle our node functions.

    An alternative is to force the executor to use pickle under the hood, which _can_
    handle these sort of dynamic objects.
    This solution comes from u/mostsquares @ stackoverflow:
    https://stackoverflow.com/questions/62830970/submit-dynamically-loaded-functions-to-the-processpoolexecutor
    """
    def submit(self, fn, /, *args, **kwargs):
        return super().submit(
            _apply_cloudpickle, cloudpickle.dumps(fn), *args, **kwargs
        )
