"""
Most executor tests are in the integration suite due to their non-trivial run times.
"""

import unittest

from pyiron_workflow._wfms import execution, executorlib


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
