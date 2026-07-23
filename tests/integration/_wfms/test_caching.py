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
    A fleche cache lets a decorated node skip re-execution on a warm run, across
    a matrix of executor placements on ``outer -> inner (macro) -> cached_sleep``.

    ``cached_sleep`` is fleche-content-keyed, so a fresh node with the same
    input hits the cache. Each test gets its own cache directory, so every cold
    run really is cold.

    Two kinds of claim are made, and they are deliberately kept apart:

    * **Apples to apples** -- cold and warm share one executor configuration, so
      executor boot is paid by both runs and cancels out of the difference
      between them. Only there is wall-clock meaningful, and what we assert is
      that ``dt_cold - dt_warm`` recovers the skipped sleep. Comparing a warm
      duration against ``T`` directly would instead be asserting that executor
      boot is cheaper than the sleep, which is a fact about the start method
      and the machine, not about caching.
    * **Apples to oranges** -- cold and warm use *different* executors, so their
      overheads are not comparable and no timing claim is made at all. Instead,
      we count cache entries: the cold run must create one, and the warm run
      must add none, which is what a hit means.
    """

    T = 2.5
    # Need big enough to stay well clear of timing noise in the cold/warm delta

    def setUp(self) -> None:
        self.root = pathlib.Path(tempfile.mkdtemp())
        self._live_executors: list[futures.Executor] = []

    def tearDown(self) -> None:
        for executor in self._live_executors:
            executor.shutdown(wait=True)
        shutil.rmtree(self.root, ignore_errors=True)

    def _run(self, node, cache, t=None) -> tuple[float, float]:
        out = node.run(
            pwf.RunConfig(run_dir=self.root / "run", fleche_cache=cache),
            t=self.T if t is None else t,
        )
        return out.duration.total_seconds(), out.outputs.s

    def _fresh_node(self, place_executors):
        node = pwf.node(integration_fixtures.outer_caching.flowrep_recipe)
        place_executors(node)
        return node

    @staticmethod
    def _entry_count(cache) -> int:
        return len(list(cache.query()))

    def _assert_sleep_is_skipped(self, place_executors) -> None:
        """Apples to apples: one executor configuration, so timing is comparable."""
        cache = integration_fixtures.make_cache(self.root)

        dt_cold, v_cold = self._run(self._fresh_node(place_executors), cache)
        dt_warm, v_warm = self._run(self._fresh_node(place_executors), cache)

        self.assertEqual(v_cold, self.T, msg="Cold run returned the wrong value")
        self.assertEqual(v_warm, self.T, msg="Warm run returned the wrong value")
        self.assertLess(
            self.T,
            dt_cold,
            msg=f"Cold run ({dt_cold:.2f}s) should be slower than the {self.T}s sleep, "
            f"as even for local runs there should be overhead to the run -- this is "
            f"just a sanity check on the test structure.",
        )
        self.assertGreater(
            dt_cold - dt_warm,
            self.T / 2,
            msg=f"Cold run took {dt_cold:.2f}s and warm run {dt_warm:.2f}s, a saving "
            f"of only {dt_cold - dt_warm:.2f}s; a cache hit should skip most of the "
            f"{self.T}s sleep. Both runs share an executor configuration, so executor "
            f"boot is paid twice and cancels out of this difference.",
        )

    def _assert_cache_is_reused(self, place_cold, place_warm) -> None:
        """Apples to oranges: mismatched executors, so count entries, not seconds."""
        cache = integration_fixtures.make_cache(self.root)

        _, v_cold = self._run(self._fresh_node(place_cold), cache)
        n_cold = self._entry_count(cache)
        _, v_warm = self._run(self._fresh_node(place_warm), cache)
        n_warm = self._entry_count(cache)

        self.assertEqual(v_cold, self.T, msg="Cold run returned the wrong value")
        self.assertEqual(v_warm, self.T, msg="Warm run returned the wrong value")
        self.assertEqual(
            n_cold,
            1,
            msg=f"The cold run should have written exactly one call to the cache, "
            f"found {n_cold}",
        )
        self.assertEqual(
            n_warm,
            n_cold,
            msg=f"The warm run added {n_warm - n_cold} cache entries; a hit should "
            f"add none. The two runs use different executors, so we make no claim "
            f"about how long either took.",
        )

    @staticmethod
    def _no_executors(node):
        pass

    def _executor_instance(self, node):
        executor = futures.ProcessPoolExecutor(max_workers=1)
        self._live_executors.append(executor)
        node.inner_caching_0.cached_sleep_0.executor = executor

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

    def test_speedup_local(self) -> None:
        self._assert_sleep_is_skipped(self._no_executors)

    def test_speedup_pool_executor(self) -> None:
        self._assert_sleep_is_skipped(self._executor_instance)

    def test_speedup_pool_instructions(self) -> None:
        self._assert_sleep_is_skipped(self._executor_instructions)

    def test_speedup_cache_test_executor(self) -> None:
        self._assert_sleep_is_skipped(self._executorlib_instructions)

    def test_reuse_local_to_pool_executor(self) -> None:
        self._assert_cache_is_reused(self._no_executors, self._executor_instance)

    def test_reuse_local_to_pool_instructions(self) -> None:
        self._assert_cache_is_reused(self._no_executors, self._executor_instructions)

    def test_reuse_local_to_cache_test_executor(self) -> None:
        self._assert_cache_is_reused(self._no_executors, self._executorlib_instructions)

    def test_reuse_pool_executor_to_local(self) -> None:
        self._assert_cache_is_reused(self._executor_instance, self._no_executors)

    def test_reuse_pool_instructions_to_pool_executor(self) -> None:
        self._assert_cache_is_reused(
            self._executor_instructions, self._executor_instance
        )

    def test_distinct_input_misses_cache(self) -> None:
        """Guard the reuse tests: entry counts must move when a call is genuinely new.

        Without this, ``_assert_cache_is_reused`` would pass just as happily against
        a cache that had quietly stopped recording anything at all.
        """
        cache = integration_fixtures.make_cache(self.root)

        self._run(self._fresh_node(self._no_executors), cache)
        n_first = self._entry_count(cache)
        self._run(self._fresh_node(self._no_executors), cache, t=self.T / 2)
        n_second = self._entry_count(cache)

        self.assertEqual(
            n_second,
            n_first + 1,
            msg=f"A previously unseen input must miss and add an entry, but the count "
            f"went {n_first} -> {n_second}",
        )

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
