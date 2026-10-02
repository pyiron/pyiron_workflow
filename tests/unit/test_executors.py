"""
Most executor tests are in the integration suite due to their non-trivial run times.
"""

import importlib.util
import sys
import unittest
from unittest import mock

from pyiron_snippets import import_alarm

from pyiron_workflow import execution, executorlib

try:
    import executorlib as _executorlib  # noqa: F401

    HAS_EXECUTORLIB = True
except ImportError:
    HAS_EXECUTORLIB = False


class TestMissingExecutorlib(unittest.TestCase):
    def test_instantiation_raises(self):
        with mock.patch.dict(
            sys.modules, {"executorlib": None, "executorlib.api": None}
        ):
            spec = importlib.util.find_spec("pyiron_workflow.executorlib")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

        for executor_class in (
            module.NodeSingleExecutor,
            module.NodeSlurmExecutor,
            module._CacheTestExecutor,
        ):
            with (
                self.subTest(executor_class.__name__),
                self.assertWarns(ImportWarning),
                self.assertRaises(import_alarm.ImportAlarmError),
            ):
                executor_class()


@unittest.skipUnless(HAS_EXECUTORLIB, "requires the optional 'executorlib' dependency")
class TestFailureHandling(unittest.TestCase):
    def test_wrong_callable_raiess(self):
        with (
            executorlib._CacheTestExecutor() as exe,
            self.assertRaises(executorlib.DedicatedExecutorError, msg="Wrong function"),
        ):
            exe.submit(int, 1)

    def test_wrong_args_raises(self):
        with (
            executorlib._CacheTestExecutor() as exe,
            self.assertRaises(
                executorlib.DedicatedExecutorError, msg="Wrong number of args"
            ),
        ):
            exe.submit(execution._return_mutated_state_with_any_exception, 1, 2, 3, 4)

    def test_kwargs_raises(self):
        with (
            executorlib._CacheTestExecutor() as exe,
            self.assertRaises(executorlib.DedicatedExecutorError, msg="Wrong kwargs"),
        ):
            exe.submit(
                execution._return_mutated_state_with_any_exception,
                1,
                2,
                3,
                anything="else",
            )
