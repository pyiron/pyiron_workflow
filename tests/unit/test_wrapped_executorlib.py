import unittest

from pyiron_workflow.executors.wrapped_executorlib import (
    CacheSingleNodeExecutor,
    DedicatedExecutorError,
)


def foo(x):
    return x + 1


class TestWrappedExecutorlib(unittest.TestCase):
    def test_application_protection(self):
        with (
            CacheSingleNodeExecutor() as exe,
            self.assertRaises(
                DedicatedExecutorError,
                msg="These executors are specialized to work with node runs",
            ),
        ):
            exe.submit(foo, 1)
