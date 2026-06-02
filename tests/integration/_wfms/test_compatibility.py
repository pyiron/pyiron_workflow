from __future__ import annotations

import os
import unittest
from concurrent import futures

from pyiron_workflow._wfms import compatibility

# Module-level so the shadowed function is importable in the worker process.


@compatibility.as_function_node("total", "pid")
def add_with_pid(x, y):
    total = x + y
    return total, os.getpid()


class NestedScope:
    @staticmethod
    @compatibility.as_function_node("total", "pid")
    def add_with_pid(x, y):
        total = x + y
        return total, os.getpid()


class TestOutOfProcess(unittest.TestCase):
    """
    Exercise the real pickling path: a legacy-style node sent to a process pool.

    The fast unit tests cover the pickle-by-reference fix directly; this thin
    integration test confirms it actually survives a round trip through a separate
    interpreter process.
    """

    def test_process_pool_execution(self) -> None:
        for factory in (add_with_pid, NestedScope.add_with_pid):
            with self.subTest(factory=factory):
                node = add_with_pid("job")
                with futures.ProcessPoolExecutor() as exe:
                    node.executor = exe
                    result = node.run(x=-1, y=43)
                self.assertEqual(result.outputs["total"].value, 42)
                self.assertNotEqual(
                    result.outputs["pid"].value,
                    os.getpid(),
                    msg="The node should have executed in a separate process.",
                )


if __name__ == "__main__":
    unittest.main()
