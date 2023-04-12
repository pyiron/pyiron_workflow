from unittest import TestCase, skipUnless
from sys import version_info
from typing import Optional, Union

from pyiron_contrib.workflow.node import Node, node


def throw_error(x: Optional[int] = None):
    raise RuntimeError


def plus_one(x=1) -> Union[int, float]:
    return x + 1


def no_default(x, y):
    return x + y + 1


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestNode(TestCase):
    def test_defaults(self):
        node = Node(plus_one, "y")

    def test_instantiation_update(self):
        no_update = Node(plus_one, "y", update_on_instantiation=False)
        self.assertIsNone(no_update.outputs.y.value)

        update = Node(plus_one, "y", update_on_instantiation=True)
        self.assertEqual(2, update.outputs.y.value)

        with self.assertRaises(TypeError):
            Node(no_default, "z")
            # None + None + 1 -> error

        with self.assertRaises(TypeError):
            Node(no_default, "z", x=1)
            # 1 + None + 1 -> error

        deferred_update = Node(no_default, "z", x=1, y=1)
        self.assertEqual(
            deferred_update.outputs.z.value,
            3,
            msg="By default, all initial values should be parsed before triggering "
                "an update"
        )

    def test_input_kwargs(self):
        node = Node(plus_one, "y", x=2)
        self.assertEqual(3, node.outputs.y.value, msg="Initialize from value")

        node2 = Node(plus_one, "y", x=node.outputs.y)
        node.update()
        self.assertEqual(4, node2.outputs.y.value, msg="Initialize from connection")

    def test_automatic_updates(self):
        node = Node(throw_error, update_on_instantiation=False)

        with self.subTest("Shouldn't run for invalid input on update"):
            node.inputs.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.inputs.x.update(1)

    def test_signals(self):
        @node("y")
        def linear(x):
            return x

        @node("z")
        def times_two(y):
            return 2 * y

        l = linear(x=1)
        t2 = times_two(
            y=l.outputs.y, update_on_instantiation=False, run_automatically=False
        )
        self.assertIsNone(
            t2.outputs.z.value,
            msg="Without updates, the output should initially be None"
        )

        # Nodes should _all_ have the run and ran signals
        t2.signals.input.run = l.signals.output.ran
        l.run()
        self.assertEqual(
            t2.outputs.z.value, 2,
            msg="Running the upstream node should trigger a run here"
        )
