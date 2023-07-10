from concurrent.futures import ProcessPoolExecutor

import cloudpickle


def _apply_cloudpickle(fn, /, *args, **kwargs):
    fn = cloudpickle.loads(fn)
    return fn(*args, **kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """
    This wrapper class replaces the `pickle` backend of the
    `concurrent.futures.ProcessPoolExecutor` with a backend from `cloudpickle`.
    In this way, even objects with no canonical import (e.g. those created dynamically
    from a decorator) can be submitted to the executor.

    This solution comes from u/mostsquares @ stackoverflow:
    https://stackoverflow.com/questions/62830970/submit-dynamically-loaded-functions-to-the-processpoolexecutor
    """

    def submit(self, fn, /, *args, **kwargs):
        return super().submit(
            _apply_cloudpickle, cloudpickle.dumps(fn), *args, **kwargs
        )
