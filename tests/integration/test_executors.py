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
