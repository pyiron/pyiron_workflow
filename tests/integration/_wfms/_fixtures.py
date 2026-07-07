import pathlib
import time

try:
    import fleche
    from fleche.caches import Cache
    from fleche.storage import CallPickleFile, ValuePickleFile

    import pyiron_workflow._wfms.api as pwf

    HAS_FLECHE = True
except ImportError:
    HAS_FLECHE = False


if HAS_FLECHE:
    # Currently, there is a bug where wrapped executorlib executors cannot handle
    # fleche-wrapped functions declared in the same file as the executor
    # Define them here behind a safety guardrail, then use them in the integration test

    @pwf.atomic
    @fleche.fleche
    def cached_sleep(t):
        time.sleep(t)
        return t

    @pwf.workflow
    def inner_caching(t):
        s = cached_sleep(t)
        return s

    @pwf.workflow
    def outer_caching(t):
        s = inner_caching(t)
        return s

    def make_cache(root: pathlib.Path):
        return Cache(
            values=ValuePickleFile.with_pickle(root=str(root / "values")),
            calls=CallPickleFile.with_pickle(root=str(root / "calls")),
        )

else:
    cached_sleep = None
    inner_caching = None
    inner_caching = None
