import unittest

from pyiron_workflow.executors.wrapped_executorlib import (
    DedicatedExecutorError,
    NodeSingleExecutor,
    ProtectedResourceError,
)
from pyiron_workflow.nodes import standard as std


def foo(x):
    return x + 1


class TestWrappedExecutorlib(unittest.TestCase):
    def test_application_protection(self):
        with (
            NodeSingleExecutor() as exe,
            self.assertRaises(
                DedicatedExecutorError,
                msg="These executors are specialized to work with node runs",
            ),
        ):
            exe.submit(foo, 1)

    def test_resource_protection(self):
        protected_resources = (
            {"cache_key": "my_key"},
            {"cache_directory": "my_directory"},
            {"cache_key": "my_key", "cache_directory": "my_directory"},
        )
        for resource_dict in protected_resources:
            with (
                self.subTest(msg=f"Submit resource dict: {resource_dict}"),
                NodeSingleExecutor() as exe,
                self.assertRaises(ProtectedResourceError),
            ):
                n = std.UserInput()
                exe.submit(n.on_run, 42, resource_dict=resource_dict)
