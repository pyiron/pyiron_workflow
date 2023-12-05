

from time import perf_counter, sleep
from unittest import TestCase

from pyiron_workflow import Workflow
from pyiron_workflow.channels import NotData



class TestParallelSpeedup(TestCase):
    def test_speedup(self):
        @Workflow.wrap_as.single_value_node()
        def Wait(t):
            sleep(t)
            return True

        def make_workflow(label):
            wf = Workflow(label)
            wf.a = Wait(t)
            wf.b = Wait(t)
            wf.c = Wait(t)
            wf.d = wf.create.standard.UserInput(t)
            wf.automate_execution = False
            return wf

        t = 2

        wf = make_workflow("serial")
        wf.a > wf.b > wf.c > wf.d
        wf.starting_nodes = [wf.a]
        t0 = perf_counter()
        wf()
        while wf.outputs.d__user_input.value is NotData:
            sleep(0.001)
        dt_serial = perf_counter() - t0

        wf = make_workflow("parallel")
        wf.d << wf.a, wf.b, wf.c
        wf.starting_nodes = [wf.a, wf.b, wf.c]

        with wf.create.Executor(max_workers=3, cores_per_worker=1) as executor:
            wf.a.executor = executor
            wf.b.executor = executor
            wf.c.executor = executor

            t1 = perf_counter()
            wf()
            while wf.outputs.d__user_input.value is NotData:
                sleep(0.001)
            dt_parallel = perf_counter() - t1

        self.assertLess(
            dt_parallel,
            0.5 * dt_serial,
            msg=f"Expected the parallel solution to be at least 2x faster, but got"
                f"{dt_parallel}  and {dt_serial} for parallel and serial times, "
                f"respectively"
        )
