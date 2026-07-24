# Executors

This sub-module holds custom children of `concurrent.futures.Executor`.
For real use cases, check other `pyiron` projects (e.g. `executorlib`); this executor is a simple pedagogical toy for showing how to parallelize when some element (e.g. your data) can't be handled by `pickle` and is thus incompatible with `concurrent.futures.ProcessPoolExecutor`.