import os
import pathlib
import shutil
import tempfile
import time
import unittest
from concurrent import futures

import flowrep as fr

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import executorlib


def get_pid(trigger):
    pid = os.getpid()
    return trigger, pid


@fr.atomic
def linspace(start, stop, num):
    # The numpy linspace provides a fun example of how flowrep can build recipes
    # manually around functions that aren't inspectable (C bindings, etc.)
    # but let's not needlessly introduce a numpy dependency here
    samples = [start + (stop - start) * i / (num - 1) for i in range(num)]
    return samples


@fr.atomic  # sometimes useful, but not necessary
def almost_target(x, target, eps=1e-5):
    return abs(x - target) < eps


@fr.workflow
def conditional_value(x, x_target, x_alt):
    if almost_target(x, x_target):
        y, pid = get_pid(x)
    else:
        y, pid = get_pid(x)
    return y, pid


@fr.workflow  # necessary -- only way to get a workflow recipe
def some_complex_workflow(start, stop, n, target):
    xs = linspace(start=start, stop=stop, num=n)

    pids = []
    for x in xs:
        y, pid = conditional_value(x, target, stop)
        pids.append(pid)

    return pids


@fr.atomic
def sleepy(t_sleep):
    time.sleep(t_sleep)
    return t_sleep


@fr.workflow
def sleepy_array(times: list[float]) -> list[float]:
    slept_for = []
    for t in times:
        t_sleep = sleepy(t)
        slept_for.append(t_sleep)
    return slept_for


@fr.workflow
def slow_wf(t):
    s = sleepy(t)
    return s


@fr.workflow
def two_slow(a, b):
    x = sleepy(a)
    y = sleepy(b)
    return x, y


def plain_fn(x):
    return x


class TestExecutors(unittest.TestCase):
    def setUp(self) -> None:
        self.node = wfms.node(some_complex_workflow)
        self.n = 5
        self.expected_id = 2  # Mid-point of 0 and 4

    def _run(self):
        return self.node.run(start=0, stop=1, n=self.n, target=0.5)

    def test_local(self):
        out = self._run()
        self.assertEqual(
            out.outputs["pids"].value,
            [os.getpid()] * self.n,
            msg="Running locally all should have the same (main process) PID",
        )

    def test_process_instance_for_if_body(self):
        with futures.ProcessPoolExecutor(max_workers=self.n) as exe:
            # Deeply apply executor to the if-node's "if" branch (the 0th body case)
            self.node.for_each_0.body.conditional_value_0.if_0.body_0.get_pid_0.executor = (
                exe
            )
            out = self._run()
        else_ids = list(out.outputs["pids"].value)
        if_id = else_ids.pop(self.expected_id)
        self.assertEqual(
            else_ids,
            [else_ids[0]] * (self.n - 1),
            msg="Else-branch ids should all be the same",
        )
        self.assertEqual(
            else_ids[0],
            os.getpid(),
            msg="Else-branch should have been run on main process",
        )
        self.assertNotEqual(
            if_id,
            else_ids[0],
            msg="If-branch was given the process pool executor -- PID should differ",
        )

    def test_process_instructions_for_else_body(self):
        # Deeply apply executor to the if-node's "else" branch
        self.node.for_each_0.body.conditional_value_0.if_0.else_body.get_pid_0.executor = wfms.ExecutorInstructions(
            constructor=futures.ProcessPoolExecutor,
            kwargs={"max_workers": self.n},
        )
        out = self._run()
        else_ids = list(out.outputs["pids"].value)
        if_id = else_ids.pop(self.expected_id)
        self.assertEqual(
            if_id,
            os.getpid(),
            msg="If-branch should have been run on main process",
        )
        self.assertNotIn(
            os.getpid(),
            else_ids,
            msg="Else-branch was given the process pool executor -- PID should differ "
            "from main process",
        )
        self.assertEqual(
            len(else_ids),
            len(set(else_ids)),
            msg="Each else job got its own unique processor and should have its own PID",
        )

    def test_instructions_inside_instance(self):
        with futures.ProcessPoolExecutor(max_workers=1) as exe:
            _, for_each_pid = exe.submit(get_pid, 0).result()
            self.node.for_each_0.executor = exe
            self.node.for_each_0.body.conditional_value_0.if_0.else_body.get_pid_0.executor = wfms.ExecutorInstructions(
                constructor=futures.ProcessPoolExecutor, kwargs={"max_workers": self.n}
            )
            out = self._run()
        ids = out.outputs["pids"].value
        else_ids = list(ids)
        if_id = else_ids.pop(self.expected_id)
        self.assertNotIn(
            os.getpid(),
            ids,
            msg="All the else jobs were run on their own process, and this separate "
            "from the if-process.",
        )
        self.assertEqual(
            for_each_pid,
            if_id,
            msg="All the conditional nodes run on the for-node process, so the "
            "if-branch PID, which has no executor of its own, should match this parent "
            "process",
        )


class TestDagParallelism(unittest.TestCase):
    def setUp(self) -> None:
        self.node = wfms.node(sleepy_array.flowrep_recipe)
        self.times = [0.4, 0.3, 0.2, 0.1]
        self.tot_time = sum(self.times)
        self.max_time = max(self.times)

    def test_with_parallelism(self):
        t_start = time.time()
        out = self.node.run(times=self.times)
        t_diff = time.time() - t_start
        self.assertAlmostEqual(t_diff, self.max_time, delta=0.1)

        diff_to_max = abs(t_diff - self.max_time)
        diff_to_tot = abs(t_diff - self.tot_time)
        self.assertLess(diff_to_max, diff_to_tot)

        self.assertListEqual(
            out.outputs["slept_for"].value,
            self.times,
            msg="Regardless of the fact the last entry finished first, the for-loop"
            "should be re-aggregating the results according to the original error.",
        )

    def test_without_parallelism(self):
        cfg_off = wfms.schemas.RunConfig(dag_layers_multithreaded=False)
        cfg_choked = wfms.schemas.RunConfig(dag_layers_max_threads=1)
        for cfg in (cfg_off, cfg_choked):
            with self.subTest(cfg=cfg):
                t_start = time.time()
                self.node.run(cfg, times=self.times)
                t_diff = time.time() - t_start
                self.assertAlmostEqual(t_diff, self.tot_time, delta=0.1)

                diff_to_max = abs(t_diff - self.max_time)
                diff_to_tot = abs(t_diff - self.tot_time)
                self.assertLess(diff_to_tot, diff_to_max)


class TestCachingExecutors(unittest.TestCase):
    T = 1.0

    def setUp(self) -> None:
        self.run_root = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.run_root, ignore_errors=True)

    def _fresh_node(self, executor_cls):
        node = wfms.node(slow_wf.flowrep_recipe)
        node.sleepy_0.executor = wfms.ExecutorInstructions(executor_cls)
        return node

    def _cache_dir(self, run_dir):
        return run_dir / executorlib.CacheOverride.cache_directory

    def _assert_cached(self, run_dir, lexical_path):
        names = [p.name for p in self._cache_dir(run_dir).iterdir()]
        self.assertTrue(
            any(lexical_path in name for name in names),
            msg=f"Expected a cache file named for {lexical_path} among {names}",
        )

    def test_cache_lifecycle(self):
        for executor_cls in (
            wfms.tools._CacheTestExecutor,
            wfms.tools.NodeSingleExecutor,
        ):
            with self.subTest(executor=executor_cls.__name__):
                run_dir = self.run_root / executor_cls.__name__
                cfg = wfms.RunConfig(run_dir=run_dir)

                node = self._fresh_node(executor_cls)
                t0 = time.perf_counter()
                out_cold = node.run(cfg, t=self.T)
                dt_first = time.perf_counter() - t0
                self.assertEqual(out_cold.outputs["s"].value, self.T)
                self._assert_cached(run_dir, node.sleepy_0.lexical_path)

                # Fresh node instance + same run_dir -> reconnect to the cache
                warm = self._fresh_node(executor_cls)
                t1 = time.perf_counter()
                out_warm = warm.run(cfg, t=self.T)
                dt_second = time.perf_counter() - t1
                self.assertEqual(out_warm.outputs["s"].value, self.T)
                self.assertLess(dt_second, dt_first / 2)
                self.assertLess(dt_second, 1.0)

                # Footgun: different input, same node/dir -> stale cached value
                t2 = time.perf_counter()
                out_stale = warm.run(cfg, t=self.T * 99)
                dt_third = time.perf_counter() - t2
                self.assertEqual(
                    out_stale.outputs["s"].value,
                    self.T,
                    msg="Cache key is the lexical path only; stale value expected",
                )
                self.assertLess(dt_third, 1.0)

    def test_fresh_run_dir_recomputes(self):
        node_a = self._fresh_node(wfms.tools._CacheTestExecutor)
        out_a = node_a.run(wfms.RunConfig(run_dir=self.run_root / "a"), t=self.T)
        self.assertEqual(out_a.outputs["s"].value, self.T)

        # A different run_dir must miss the cache and recompute the new value
        node_b = self._fresh_node(wfms.tools._CacheTestExecutor)
        out_b = node_b.run(wfms.RunConfig(run_dir=self.run_root / "b"), t=self.T * 2)
        self.assertEqual(
            out_b.outputs["s"].value,
            self.T * 2,
            msg="A fresh run_dir must not return a stale cache hit",
        )

    def test_instance_form_caches(self):
        cfg = wfms.RunConfig(run_dir=self.run_root / "instance")

        cold = wfms.node(slow_wf.flowrep_recipe)
        with wfms.tools._CacheTestExecutor() as exe:
            cold.sleepy_0.executor = exe
            t0 = time.perf_counter()
            out_cold = cold.run(cfg, t=self.T)
            dt_first = time.perf_counter() - t0
        self.assertEqual(out_cold.outputs["s"].value, self.T)

        warm = wfms.node(slow_wf.flowrep_recipe)
        with wfms.tools._CacheTestExecutor() as exe:
            warm.sleepy_0.executor = exe
            t1 = time.perf_counter()
            out_warm = warm.run(cfg, t=self.T)
            dt_second = time.perf_counter() - t1
        self.assertEqual(out_warm.outputs["s"].value, self.T)
        self.assertLess(dt_second, dt_first / 2)
        self.assertLess(dt_second, 1.0)

    def test_distinct_nodes_distinct_cache(self):
        run_dir = self.run_root / "distinct"
        cfg = wfms.RunConfig(run_dir=run_dir)

        node = wfms.node(two_slow.flowrep_recipe)
        node.sleepy_0.executor = wfms.ExecutorInstructions(
            wfms.tools._CacheTestExecutor
        )
        node.sleepy_1.executor = wfms.ExecutorInstructions(
            wfms.tools._CacheTestExecutor
        )
        node.run(cfg, a=0.01, b=0.01)

        self.assertNotEqual(node.sleepy_0.lexical_path, node.sleepy_1.lexical_path)
        self._assert_cached(run_dir, node.sleepy_0.lexical_path)
        self._assert_cached(run_dir, node.sleepy_1.lexical_path)

    def test_dedicated_executor_error(self):
        with (
            wfms.tools._CacheTestExecutor() as exe,
            self.assertRaises(executorlib.DedicatedExecutorError),
        ):
            exe.submit(plain_fn, 1)
