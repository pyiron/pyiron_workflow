from __future__ import annotations

import pathlib
import shutil
import tempfile
import unittest
from concurrent import futures

from static._wfms import integration_fixtures

import pyiron_workflow._wfms.api as pwf

try:
    import fleche

    HAS_FLECHE = True
except ImportError:
    HAS_FLECHE = False


@unittest.skipUnless(HAS_FLECHE, "requires the optional 'fleche' dependency")
class TestFlecheCaching(unittest.TestCase):
    """
    A fleche cache lets a decorated node skip re-execution on a warm run. We
    prove this by sleeping for ``T`` seconds on the cold run and asserting the
    warm run finishes in less than ``T`` (so the sleep did not happen), across
    a matrix of executor placements on ``outer -> inner (macro) -> cached_sleep``.

    ``cached_sleep`` is fleche-content-keyed, so a fresh node with the same
    input hits the cache. Each test gets its own cache directory, so every cold
    run really is cold.
    """

    T = 2.5
    # Need big enough: local sleep > executor boot (slow) + non-local cache hit (fast)

    def setUp(self) -> None:
        self.root = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _run(self, node, cache) -> tuple[float, float]:
        out = node.run(
            pwf.RunConfig(run_dir=self.root / "run", fleche_cache=cache),
            t=self.T,
        )
        return out.duration.total_seconds(), out.outputs.s

    def _assert_speedup(self, place_executors, replace_executors) -> None:
        cache = integration_fixtures.make_cache(self.root)

        cold_node = pwf.node(integration_fixtures.outer_caching.flowrep_recipe)
        place_executors(cold_node)
        dt_cold, v_cold = self._run(cold_node, cache)

        warm_node = pwf.node(integration_fixtures.outer_caching.flowrep_recipe)
        replace_executors(warm_node)
        dt_warm, v_warm = self._run(warm_node, cache)

        self.assertEqual(v_cold, self.T, msg="Cold run returned the wrong value")
        self.assertEqual(v_warm, self.T, msg="Warm run returned the wrong value")
        self.assertLess(
            dt_warm,
            self.T,
            msg=f"Warm run took {dt_warm:.2f}s; expected < {self.T}s sleep "
            f"(cache hit should skip the sleep)",
        )
        self.assertLess(
            dt_warm,
            dt_cold,
            msg=f"Warm run ({dt_warm:.2f}s) should be faster than cold "
            f"({dt_cold:.2f}s)",
        )
        self.assertLess(
            self.T,
            dt_cold,
            msg=f"Cold run ({dt_cold:.2f}s) should be slower than {self.T}s sleep, as "
            f"even for local runs there should be overhead to the run -- this is just "
            f"a sanity check on the test structure.",
        )

    @staticmethod
    def _no_executors(node):
        pass

    @staticmethod
    def _executor_instance(node):
        node.inner_caching_0.cached_sleep_0.executor = futures.ProcessPoolExecutor(
            max_workers=1
        )

    @staticmethod
    def _executor_instructions(node):
        node.inner_caching_0.cached_sleep_0.executor = pwf.ExecutorInstructions(
            futures.ProcessPoolExecutor
        )

    @staticmethod
    def _executorlib_instructions(node):
        node.inner_caching_0.cached_sleep_0.executor = pwf.ExecutorInstructions(
            pwf.tools._CacheTestExecutor
        )

    def test_local_local(self) -> None:
        self._assert_speedup(self._no_executors, self._no_executors)

    def test_local_pool_executor(self) -> None:
        self._assert_speedup(self._no_executors, self._executor_instance)

    def test_local_pool_instructions(self) -> None:
        self._assert_speedup(self._no_executors, self._executor_instructions)

    def test_local_cache_test_executor(self) -> None:
        self._assert_speedup(self._no_executors, self._executorlib_instructions)

    def test_pool_executor_local(self) -> None:
        self._assert_speedup(self._executor_instance, self._no_executors)

    def test_pool_executor_pool_instructions(self) -> None:
        self._assert_speedup(self._executor_instructions, self._executor_instance)

    def test_run_config_overrides_fleche(self):
        node = pwf.node(integration_fixtures.outer_caching.flowrep_recipe)
        with fleche.cache(
            fleche.caches.Cache(
                values=fleche.storage.ValueMemory({}),
                calls=fleche.storage.CallMemory({}),
            ),
        ):
            t1, _ = self._run(node, None)
            t2, _ = self._run(node, None)
        self.assertLess(self.T, t1)
        self.assertLess(
            self.T,
            t2,
            msg="With no cache provided to the run config, we should not cache -- no "
            "matter what, even inside a cache context",
        )


if __name__ == "__main__":
    unittest.main()
