import unittest

from pyiron_workflow.nodes import function
from pyiron_workflow.nodes.while_loop import (
    InvalidEdgeError,
    InvalidTestOutputError,
    NonTerminatingLoopError,
    While,
    while_node,
    while_node_factory,
)


@function.as_function_node
def TypedComparison(candidate: int, limit: int) -> bool:
    return candidate < limit


@function.as_function_node
def UntypedComparison(a, b):
    return a > b


SIDE_EFFECT = 0


@function.as_function_node("add")
def AddWithSideEffect(obj, other):
    global SIDE_EFFECT  # noqa: PLW0603
    SIDE_EFFECT += 1
    return obj + other


@function.as_function_node()
def TwoOutputs(a) -> tuple[bool, bool]:
    return True, False


N_NODES_PER_ITERATION = 2  # Body and test


class TestWhileLoop(unittest.TestCase):

    def setUp(self):
        self.awhile = while_node(
            TypedComparison,
            AddWithSideEffect,
            (("add", "candidate"),),
            (("add", "obj"),),
        )
        self.limit = 3
        self.awhile(test_candidate=0, test_limit=self.limit, body_obj=0, body_other=1)
        global SIDE_EFFECT  # noqa: PLW0603
        SIDE_EFFECT = 0

    def test_basics(self):
        self.assertEqual(
            self.limit,
            self.awhile.outputs.add.value,
            msg="Expect to add 1 ${limit} times",
        )

        self.assertEqual(
            N_NODES_PER_ITERATION * (1 + self.limit),
            len(self.awhile.children),
            msg="Expect two nodes per iteration",
        )
        self.assertListEqual(
            list(self.awhile._body_node_class.preview_outputs().keys()),
            self.awhile.outputs.labels,
            msg="Expect output to duplicate the body node",
        )

    def test_reruns(self):
        result = self.awhile.outputs.add.value
        self.assertEqual(result, self.limit, msg="Sanity check on output")
        self.awhile.run()
        self.assertEqual(
            SIDE_EFFECT, 0, msg="With the cache, we should avoid re-running the body"
        )

        new_limit = 5
        self.awhile.inputs.test_limit = new_limit
        self.awhile.run()
        self.assertEqual(
            new_limit,
            SIDE_EFFECT,
            msg="With new input, we expect to re-run and cause our side effect",
        )
        self.assertEqual(
            (1 + new_limit) * N_NODES_PER_ITERATION,
            len(self.awhile.children),
            msg="On re-running, we expect the body to be cleaned and recreated -- not appended to",
        )

    def test_test_outputs(self):
        with self.assertRaises(
            InvalidTestOutputError,
            msg="Test conditions without a singular output should fail",
        ):
            while_node(
                TwoOutputs,
                AddWithSideEffect,
                (("add", "other"),),
                (("add", "obj"),),
            )

        with self.assertRaises(
            InvalidTestOutputError, msg="Test conditions must be typed"
        ):
            while_node(
                UntypedComparison,
                AddWithSideEffect,
                (("add", "a"),),
                (("add", "obj"),),
            )

        self.assertIsInstance(
            while_node(
                UntypedComparison,
                AddWithSideEffect,
                (("add", "a"),),
                (("add", "obj"),),
                strict_condition_hint=False,
            ),
            While,
            msg="Unless we explicitly allow the condition to lack a hint",
        )

    def test_edges(self):
        with self.assertRaises(InvalidEdgeError, msg="Missing test input should fail"):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (("add", "DOESNOTEXISIT"),),
                (("add", "obj"),),
            )

        with self.assertRaises(InvalidEdgeError, msg="Missing body input should fail"):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (("add", "candidate"),),
                (("add", "DOESNOTEXISIT"),),
            )

        with self.assertRaises(InvalidEdgeError, msg="Missing body output should fail"):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (("DOESNOTEXISIT", "candidate"),),
                (("add", "obj"),),
            )

        with self.assertRaises(InvalidEdgeError, msg="Missing body output should fail"):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (("add", "candidate"),),
                (("DOESNOTEXISIT", "obj"),),
            )

        with self.assertRaises(
            NonTerminatingLoopError, msg="Some connection from body to test is required"
        ):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (),
                (("add", "DOESNOTEXISIT"),),
            )

        with self.assertRaises(
            NonTerminatingLoopError, msg="Some connection from body to test is required"
        ):
            while_node_factory(
                TypedComparison,
                AddWithSideEffect,
                (("add", "candidate"),),
                (),
            )

    def test_iteration_limit(self):
        self.awhile(test_limit=5, max_iterations=6)
        self.assertEqual(
            self.awhile.inputs.test_limit.value,
            self.awhile.outputs.add.value,
            msg="Expect to be limited by the test",
        )
        self.awhile(test_limit=5, max_iterations=3)
        self.assertEqual(
            self.awhile.inputs.max_iterations.value,
            self.awhile.outputs.add.value,
            msg="Expect to be limited by the iteration cap",
        )
