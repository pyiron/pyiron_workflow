from time import perf_counter, sleep
import unittest

from pyiron_workflow import Workflow
from pyiron_workflow.channels import NOT_DATA


class TestSpeedup(unittest.TestCase):
    def test_speedup(self):

        def make_workflow(label):
            wf = Workflow(label)
            wf.a = Workflow.create.standard.Sleep(t)
            wf.b = Workflow.create.standard.Sleep(t)
            wf.c = Workflow.create.standard.Sleep(t)
            wf.d = wf.create.standard.UserInput(t)
            wf.automate_execution = False
            return wf

        t = 5

        wf = make_workflow("serial")
        wf.a >> wf.b >> wf.c >> wf.d
        wf.starting_nodes = [wf.a]
        t0 = perf_counter()
        wf()
        while wf.running:
            sleep(0.001)
        dt_serial = perf_counter() - t0
        t0_cache = perf_counter()
        wf()
        while wf.running:
            sleep(0.001)
        dt_cached_serial = perf_counter() - t0_cache

        self.assertLess(
            dt_cached_serial,
            0.01 * t,
            msg=f"The cache should be trivially fast compared to actual execution of "
                f"a sleep node"
        )

        wf = make_workflow("parallel")
        wf.d << (wf.a, wf.b, wf.c)
        wf.starting_nodes = [wf.a, wf.b, wf.c]

        with wf.create.ProcessPoolExecutor(max_workers=3) as executor:
            wf.a.executor = executor
            wf.b.executor = executor
            wf.c.executor = executor

            t1 = perf_counter()
            wf()
            while wf.running:
                sleep(0.001)
            dt_parallel = perf_counter() - t1

            t1_cache = perf_counter()
            wf()
            while wf.running:
                sleep(0.001)
            dt_cached_parallel = perf_counter() - t1_cache

        self.assertLess(
            dt_parallel,
            0.5 * dt_serial,
            msg=f"Expected the parallel solution to be at least 2x faster, but got"
                f"{dt_parallel}  and {dt_serial} for parallel and serial times, "
                f"respectively"
        )
        self.assertLess(
            dt_cached_parallel,
            0.01 * t,
            msg=f"The cache should be trivially fast compared to actual execution of "
                f"a sleep node"
        )


if __name__ == '__main__':
    unittest.main()
