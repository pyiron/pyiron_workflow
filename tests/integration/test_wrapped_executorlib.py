import time
import unittest

import pyiron_workflow as pwf
from pyiron_workflow.executors.wrapped_executorlib import (
    CacheSingleNodeExecutor,
    _CacheTestClusterExecutor,
)


class TestWrappedExecutorlib(unittest.TestCase):

    def _test_cache(self, executor_class):
        t_sleep = 1

        wf = pwf.Workflow("passive_run")
        wf.n1 = pwf.std.UserInput(t_sleep)
        wf.n2 = pwf.std.Sleep(wf.n1)
        wf.n3 = pwf.std.UserInput(wf.n2)
        expected_output = {"n3__user_input": t_sleep}

        wf.use_cache = False
        wf.n2.use_cache = False
        wf.n2._remove_executorlib_cache = False

        wf.n2.executor = (executor_class, (), {})

        t0 = time.perf_counter()
        out1 = wf.run()
        t1 = time.perf_counter()
        out2 = wf.run()
        t2 = time.perf_counter()

        self.assertDictEqual(
            expected_output,
            out1,
            msg="Sanity check that the workflow ran ok",
        )
        self.assertDictEqual(
            expected_output,
            out2,
            msg="Sanity check that the workflow re-ran ok",
        )
        self.assertFalse(
            wf.n2.cache_hit, msg="Sanity check that we're not just using the cache"
        )

        t_first_run = t1 - t0
        t_second_run = t2 - t1
        self.assertGreater(
            t_first_run,
            t_sleep,
            msg="The initial run should be executing the sleep node",
        )
        self.assertLess(
            t_second_run,
            0.5 * t_sleep,
            msg="The second run should allow executorlib to find the cached result, "
            "and be much faster than the sleep time",
        )

        self.assertTrue(
            wf.n2._wrapped_executorlib_cache_file.is_file(),
            msg="Since we deactivated cache removal, we expect the executorlib cache "
            "file to still be there",
        )

        wf.n2.running = True  # Fake that it's still running
        wf.n2._remove_executorlib_cache = True  # Reactivate automatic cleanup
        out3 = wf.run()

        self.assertDictEqual(
            expected_output,
            out3,
            msg="Workflow should recover from a running child state when the wrapped "
            "executorlib executor can find a cached result",
        )
        self.assertFalse(
            wf.n2._wrapped_executorlib_cache_file.is_file(),
            msg="The cached result should be cleaned up",
        )

    def test_cache(self):
        for executor_class in [CacheSingleNodeExecutor, _CacheTestClusterExecutor]:
            with self.subTest(executor_class.__name__):
                self._test_cache(executor_class)

    def test_automatic_cleaning(self):
        n = pwf.std.UserInput(1)
        with _CacheTestClusterExecutor() as exe:
            n.executor = exe
            n.run()
        self.assertFalse(n._wrapped_executorlib_cache_file.is_file())
