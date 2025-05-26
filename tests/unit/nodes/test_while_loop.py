import unittest

import pyiron_workflow.nodes.function as function
from pyiron_workflow.nodes.while_loop import InvalidTestOutputError, while_node


@function.as_function_node
def TypedComparison(candidate: int, limit: int) -> bool:
    return candidate < limit


@function.as_function_node
def UntypedComparison(a, b):
    return a > b


SIDE_EFFECT = 0

@function.as_function_node("add")
def AddWithSideEffect(obj, other):
    global SIDE_EFFECT
    SIDE_EFFECT += 1
    return obj + other


@function.as_function_node()
def TwoOutputs(a) -> tuple[bool, bool]:
    return True, False

N_NODES_PER_ITERATION = 2  # Body and test


class TestWhileLoop(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.awhile = while_node(
            TypedComparison,
            AddWithSideEffect,
            [("add", "candidate")],
            [("add", "obj")],
        )
        cls.limit = 3
        cls.awhile(
            test_candidate=0,
            test_limit=cls.limit,
            body_obj=0,
            body_other=1
        )
        global SIDE_EFFECT
        print("SIDE EFFECT INITIALLY", SIDE_EFFECT)
        SIDE_EFFECT = 0

    def test_basics(self):
        self.assertEqual(
            self.limit,
            self.awhile.outputs.add.value, msg="Expect to add 1 ${limit} times"
        )

        self.assertEqual(
            N_NODES_PER_ITERATION * (1 + self.limit),
            len(self.awhile.children),
            msg="Expect two nodes per iteration"
        )
        self.assertListEqual(
            list(self.awhile._body_node_class.preview_outputs().keys()),
            self.awhile.outputs.labels,
            msg="Expect output to duplicate the body node"
        )

    def test_reruns(self):
        result = self.awhile.outputs.add.value
        self.assertEqual(
            result, self.limit, msg="Sanity check on output"
        )
        self.awhile.run()
        global SIDE_EFFECT
        self.assertEqual(
            SIDE_EFFECT, 0, msg="With the cache, we should avoid re-running the body"
        )

        new_limit = 5
        self.awhile.inputs.test_limit = new_limit
        try:
            self.awhile.run()
            self.assertEqual(
                new_limit, SIDE_EFFECT, msg="With new input, we expect to re-run and cause our side effect"
            )
        finally:
            SIDE_EFFECT = 0  # Clean up
        self.assertEqual(
            (1 + new_limit) * N_NODES_PER_ITERATION,
            len(self.awhile.children),
            msg="On re-running, we expect the body to be cleaned and recreated -- not appended to"
        )

    def test_test_outputs(self):
        with self.assertRaises(
            InvalidTestOutputError,
            msg="Test conditions without a singular output should fail",
        ):
            while_node(
                TwoOutputs,
                AddWithSideEffect,
                [("add", "other")],
                [("add", "obj")],
            )

        with self.assertRaises(
            InvalidTestOutputError, msg="Test conditions must be typed"
        ):
            while_node(
                UntypedComparison,
                AddWithSideEffect,
                [("add", "a")],
                [("add", "obj")],
            )
