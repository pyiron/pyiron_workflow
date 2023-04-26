from unittest import TestCase, skipUnless
from sys import version_info
from typing import Optional, Union

from pyiron_contrib.workflow.node import FastNode, Node, SingleValueNode, node


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
            update_on_instantiation=False, run_automatically=False, y=l.outputs.y
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

    def test_fast_node(self):
        has_defaults_is_ok = FastNode(plus_one, "y")

        with self.assertRaises(ValueError):
            missing_defaults_should_fail = FastNode(no_default, "z")

    def test_single_value_node(self):
        with self.subTest("Test creation"):
            has_defaults_and_one_return = SingleValueNode(plus_one, "y")

            with self.assertRaises(ValueError):
                too_many_labels = SingleValueNode(plus_one, "z", "excess_label")

        with self.subTest("Test output attribute access as a fallback"):
            class Foo:
                some_attribute = "exists"
                connected = True  # Overlaps with an attribute of the node

                def __getitem__(self, item):
                    if item == 0:
                        return True
                    else:
                        return False

            def returns_foo() -> Foo:
                return Foo()

            svn = SingleValueNode(returns_foo, "foo")

            self.assertEqual(
                svn.some_attribute,
                "exists",
                msg="Should fall back to looking on the single value"
            )

            self.assertEqual(
                svn.connected,
                False,
                msg="Should return the _node_ attribute, not the single value attribute"
            )

            with self.assertRaises(AttributeError):
                svn.doesnt_exists_anywhere

            self.assertEqual(
                svn[0],
                True,
                msg="Should fall back to looking on the single value"
            )

            self.assertEqual(
                svn["some other key"],
                False,
                msg="Should fall back to looking on the single value"
            )
