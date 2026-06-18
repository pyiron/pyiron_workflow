import os
import time
import unittest
from concurrent import futures

import flowrep as fr

from pyiron_workflow._wfms import api as wfms


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
        with futures.ProcessPoolExecutor() as exe:
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
        cfg_off = wfms.schemas.RunConfig(
            prime_mover=self.node.lexical_path,
            dag_layers_multithreaded=False,
        )
        cfg_choked = wfms.schemas.RunConfig(
            prime_mover=self.node.lexical_path,
            dag_layers_max_threads=1,
        )
        for cfg in (cfg_off, cfg_choked):
            with self.subTest(cfg=cfg):
                t_start = time.time()
                self.node.run(cfg, times=self.times)
                t_diff = time.time() - t_start
                self.assertAlmostEqual(t_diff, self.tot_time, delta=0.1)

                diff_to_max = abs(t_diff - self.max_time)
                diff_to_tot = abs(t_diff - self.tot_time)
                self.assertLess(diff_to_tot, diff_to_max)
