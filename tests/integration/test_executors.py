import unittest
from concurrent import futures

import pyiron_workflow as pwf
from pyiron_workflow import _tests

_tests.ensure_tests_in_python_path()
from static import demo_nodes  # noqa: E402


class TestNestingExecutors(unittest.TestCase):
    def test_executors_at_depth(self):
        def build_wf():
            wf = pwf.Workflow("exec")
            wf.six = demo_nodes.AddSix(0)
            return wf

        expected = {"six__add_six": 6}

        with self.subTest("parent"):
            wf = build_wf()
            wf.executor = (futures.ProcessPoolExecutor, (), {})
            future = wf.run()
            future.result()
            self.assertDictEqual(expected, wf.outputs.to_value_dict())

        with self.subTest("child"):
            wf = build_wf()
            wf.six.executor = (futures.ProcessPoolExecutor, (), {})
            self.assertDictEqual(expected, wf.run())

        with self.subTest("grandchild"):
            wf = build_wf()
            wf.six.a.executor = (futures.ProcessPoolExecutor, (), {})
            self.assertDictEqual(expected, wf.run())

        with self.subTest("function"):
            wf = build_wf()
            wf.six.a.two.executor = (futures.ProcessPoolExecutor, (), {})
            self.assertDictEqual(expected, wf.run())

    def test_future_and_rerun(self):
        t_sleep = 0.2
        n = pwf.std.Sleep(0.2)
        n.executor = (futures.ThreadPoolExecutor, (), {})
        f = n.run()
        f2 = n.run()
        self.assertIs(
            f,
            f2,
            msg="Running an already-running node with a future should return that future",
        )
        with self.assertRaises(
            RuntimeError,
            msg="Trying to re-run a running node with a live future should raise an error -- don't interrupt something that's in another thread/process!",
        ):
            n.run(rerun=True)
        self.assertEqual(
            t_sleep, f.result(), msg="Sanity check and wait for it to finish"
        )
