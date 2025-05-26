import unittest

import pyiron_workflow.nodes.function as function
import pyiron_workflow.nodes.standard as std
from pyiron_workflow.nodes.while_loop import InvalidTestOutputError, while_node


@function.as_function_node
def UntypedComparison(a, b):
    return a > b


class TestWhileLoop(unittest.TestCase):

    def test_singular_output(self):
        with self.assertRaises(
            InvalidTestOutputError,
            msg="Test conditions without a singular output should fail",
        ):
            while_node(
                std.Add,
                std.Add,
                [("sum", "other")],
                [("sum", "obj")],
            )

        with self.assertRaises(
            InvalidTestOutputError, msg="Test conditions must be typed"
        ):
            while_node(
                UntypedComparison,
                std.Add,
                [("sum", "a")],
                [("sum", "obj")],
            )
