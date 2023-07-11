from concurrent.futures import ProcessPoolExecutor

import cloudpickle


def _apply_cloudpickle(fn, /, *args, **kwargs):
    fn = cloudpickle.loads(fn)
    return fn(*args, **kwargs)


class CloudpickleProcessPoolExecutor(ProcessPoolExecutor):
    """
    This executor behaves like `concurrent.futures.ProcessPoolExecutor`, except that
    non-pickleable callables may also be submit (e.g. dynamically defined functions).

    This is accomplished by replacing the `pickle` backend of the
    `concurrent.futures.ProcessPoolExecutor` with a backend from `cloudpickle` when
    serializing the callable.

    This solution comes from u/mostsquares @ stackoverflow:
    https://stackoverflow.com/questions/62830970/submit-dynamically-loaded-functions-to-the-processpoolexecutor

    Note: Arguments and return values must still be regularly pickleable.
    """

    def submit(self, fn, /, *args, **kwargs):
        return super().submit(
            _apply_cloudpickle, cloudpickle.dumps(fn), *args, **kwargs
        )
