from concurrent.futures import Future
from sys import version_info
from typing import Optional, Union
import unittest
import warnings

from pyiron_contrib.executors import CloudpickleProcessPoolExecutor
from pyiron_contrib.workflow.channels import NotData
from pyiron_contrib.workflow.files import DirectoryObject
from pyiron_contrib.workflow.function import (
    Slow, Function, SingleValue, function_node, single_value_node
)


def throw_error(x: Optional[int] = None):
    raise RuntimeError


def plus_one(x=1) -> Union[int, float]:
    y = x + 1
    return y


def no_default(x, y):
    return x + y + 1


def returns_multiple(x, y):
    return x, y, x + y


def void():
    pass


def multiple_branches(x):
    if x < 10:
        return True
    else:
        return False


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestFunction(unittest.TestCase):
    def test_instantiation(self):
        with self.subTest("Void function is allowable"):
            void_node = Function(void)
            self.assertEqual(len(void_node.outputs), 0)

        with self.subTest("Args and kwargs at initialization"):
            node = Function(returns_multiple, 1, y=2)
            self.assertEqual(
                node.inputs.x.value,
                1,
                msg="Should be able to set function input as args"
            )
            self.assertEqual(
                node.inputs.y.value,
                2,
                msg="Should be able to set function input as kwargs"
            )

            with self.assertRaises(ValueError):
                # Can't pass more args than the function takes
                Function(returns_multiple, 1, 2, 3)

    def test_defaults(self):
        with_defaults = Function(plus_one)
        self.assertEqual(
            with_defaults.inputs.x.value,
            1,
            msg=f"Expected to get the default provided in the underlying function but "
                f"got {with_defaults.inputs.x.value}",
        )
        without_defaults = Function(no_default)
        self.assertIs(
            without_defaults.inputs.x.value,
            NotData,
            msg=f"Expected values with no default specified to start as {NotData} but "
                f"got {without_defaults.inputs.x.value}",
        )
        self.assertFalse(
            without_defaults.ready,
            msg="I guess we should test for behaviour and not implementation... Without"
                "defaults, the node should not be ready!"
        )

    def test_label_choices(self):
        with self.subTest("Automatically scrape output labels"):
            n = Function(plus_one)
            self.assertListEqual(n.outputs.labels, ["y"])

        with self.subTest("Allow overriding them"):
            n = Function(no_default, output_labels=("sum_plus_one",))
            self.assertListEqual(n.outputs.labels, ["sum_plus_one"])

        with self.subTest("Allow forcing _one_ output channel"):
            n = Function(returns_multiple, output_labels="its_a_tuple")
            self.assertListEqual(n.outputs.labels, ["its_a_tuple"])

        with self.subTest("Fail on multiple return values"):
            with self.assertRaises(ValueError):
                # Can't automatically parse output labels from a function with multiple
                # return expressions
                Function(multiple_branches)

        with self.subTest("Override output label scraping"):
            switch = Function(multiple_branches, output_labels="bool")
            self.assertListEqual(switch.outputs.labels, ["bool"])

    def test_instantiation_update(self):
        no_update = Function(
            plus_one,
            run_on_updates=True,
            update_on_instantiation=False
        )
        self.assertIs(
            no_update.outputs.y.value,
            NotData,
            msg=f"Expected the output to be in its initialized and not-updated NotData "
                f"state, but got {no_update.outputs.y.value}"
        )

        update = Function(
            plus_one,
            run_on_updates=True,
            update_on_instantiation=True
        )
        self.assertEqual(2, update.outputs.y.value)

        default = Function(plus_one)
        self.assertEqual(
            2,
            default.outputs.y.value,
            msg="Default behaviour should be to run on updates and update on "
                "instantiation",
        )

        with self.assertRaises(TypeError):
            run_without_value = Function(no_default)
            run_without_value.run()
            # None + None + 1 -> error

        with self.assertRaises(TypeError):
            run_without_value = Function(no_default, x=1)
            run_without_value.run()
            # 1 + None + 1 -> error

        deferred_update = Function(no_default, x=1, y=1)
        deferred_update.run()
        self.assertEqual(
            deferred_update.outputs["x + y + 1"].value,
            3,
            msg="By default, all initial values should be parsed before triggering "
                "an update"
        )

    def test_input_kwargs(self):
        node = Function(plus_one, x=2)
        self.assertEqual(3, node.outputs.y.value, msg="Initialize from value")

        node2 = Function(plus_one, x=node.outputs.y)
        node.update()
        self.assertEqual(4, node2.outputs.y.value, msg="Initialize from connection")

    def test_automatic_updates(self):
        node = Function(throw_error, "no_return", update_on_instantiation=False)

        with self.subTest("Shouldn't run for invalid input on update"):
            node.inputs.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.inputs.x.update(1)

    def test_signals(self):
        @function_node()
        def linear(x):
            return x

        @function_node()
        def times_two(y):
            return 2 * y

        l = linear(x=1)
        t2 = times_two(
            update_on_instantiation=False,
            run_automatically=False,
            output_labels=["double"],
            y=l.outputs.x
        )
        self.assertIs(
            t2.outputs.double.value,
            NotData,
            msg=f"Without updates, expected the output to be {NotData} but got "
                f"{t2.outputs.double.value}"
        )

        # Nodes should _all_ have the run and ran signals
        t2.signals.input.run = l.signals.output.ran
        l.run()
        self.assertEqual(
            t2.outputs.double.value, 2,
            msg="Running the upstream node should trigger a run here"
        )

    def test_statuses(self):
        n = Function(plus_one, run_on_updates=False)
        self.assertTrue(n.ready)
        self.assertFalse(n.running)
        self.assertFalse(n.failed)

        # Can't really test "running" until we have a background executor, so fake a bit
        n.running = True
        with self.assertRaises(RuntimeError):
            # Running nodes can't be run
            n.run()
        n.running = False

        n.inputs.x = "Can't be added together with an int"
        with self.assertRaises(TypeError):
            # The function error should get passed up
            n.run()
        self.assertFalse(n.ready)
        # self.assertFalse(n.running)
        self.assertTrue(n.failed)

        n.inputs.x = 1
        n.update()
        self.assertFalse(
            n.ready,
            msg="Update _checks_ for ready, so should still have failed status"
        )
        # self.assertFalse(n.running)
        self.assertTrue(n.failed)

        n.run()
        self.assertTrue(
            n.ready,
            msg="A manual run() call bypasses checks, so readiness should reset"
        )
        self.assertTrue(n.ready)
        # self.assertFalse(n.running)
        self.assertFalse(n.failed, msg="Re-running should reset failed status")

    def test_with_self(self):
        def with_self(self, x: float) -> float:
            # Note: Adding internal state to the node like this goes against the best
            #  practice of keeping nodes "functional". Following python's paradigm of
            #  giving users lots of power, we want to guarantee that this behaviour is
            #  _possible_.
            # TODO: update this test with a better-conforming example of this power at
            #  a future date.
            if hasattr(self, "some_counter"):
                self.some_counter += 1
            else:
                self.some_counter = 1
            return x + 0.1

        node = Function(with_self, output_labels="output")
        self.assertTrue(
            "x" in node.inputs.labels,
            msg=f"Expected to find function input 'x' in the node input but got "
                f"{node.inputs.labels}"
        )
        self.assertFalse(
            "self" in node.inputs.labels,
            msg="Expected 'self' to be filtered out of node input, but found it in the "
                "input labels"
        )
        node.inputs.x = 1
        node.run()
        self.assertEqual(
            node.outputs.output.value,
            1.1,
            msg="Basic node functionality appears to have failed"
        )
        self.assertEqual(
            node.some_counter,
            1,
            msg="Function functions should be able to modify attributes on the node object."
        )

        node.executor = CloudpickleProcessPoolExecutor
        with self.assertRaises(NotImplementedError):
            # Submitting node_functions that use self is still raising
            # TypeError: cannot pickle '_thread.lock' object
            # For now we just fail cleanly
            node.run()

        def with_messed_self(x: float, self) -> float:
            return x + 0.1

        with warnings.catch_warnings(record=True) as warning_list:
            node = Function(with_messed_self)
            self.assertTrue("self" in node.inputs.labels)

        self.assertEqual(len(warning_list), 1)

    def test_call(self):
        node = Function(no_default, output_labels="output", run_on_updates=False)

        with self.subTest("Ensure desired failures occur"):
            with self.assertRaises(ValueError):
                # More input args than there are input channels
                node(1, 2, 3)

            with self.assertRaises(ValueError):
                # Using input as an arg _and_ a kwarg
                node(1, y=2, x=3)

        with self.subTest("Make sure data updates work as planned"):
            node(1, y=2)
            self.assertEqual(
                node.inputs.x.value, 1, msg="__call__ should accept args to update input"
            )
            self.assertEqual(
                node.inputs.y.value, 2, msg="__call__ should accept kwargs to update input"
            )
            self.assertEqual(
                node.outputs.output.value, NotData, msg="__call__ should not run things"
            )
            node.run_on_updates = True
            node(3)  # Implicitly test partial update
            self.assertEqual(
                no_default(3, 2),
                node.outputs.output.value,
                msg="__call__ should invoke update s.t. run gets called if run_on_updates"
            )

        with self.subTest("Check that bad kwargs don't stop good ones"):
            with self.assertWarns(Warning):
                node.run_on_updates = True
                node(4, run_on_updates=False, y=5, foobar="not a kwarg of any sort")

                self.assertTrue(
                    node.run_on_updates,
                    msg="You should only be able to update input on a call, that's "
                        "what the warning is for!"
                )
                self.assertTupleEqual(
                    (node.inputs.x.value, node.inputs.y.value),
                    (4, 5),
                    msg="The warning should not prevent other data from being parsed"
                )

            with self.assertWarns(Warning):
                # It's also fine if you just have a typo in your kwarg or whatever,
                # there should just be a warning that the data didn't get updated
                node(some_randome_kwaaaaarg="foo")

    def test_return_value(self):
        node = Function(plus_one)

        with self.subTest("Run on main process"):
            return_on_call = node(1)
            self.assertEqual(
                return_on_call,
                plus_one(1),
                msg="Run output should be returned on call"
            )

            return_on_update = node.update()
            self.assertEqual(
                return_on_update,
                plus_one(1),
                msg="Run output should be returned on update"
            )

            node.run_on_updates = False
            return_on_update_without_run = node.update()
            self.assertIsNone(
                return_on_update_without_run,
                msg="When not running on updates, the update should not return anything"
            )
            return_on_call_without_run = node(2)
            self.assertIsNone(
                return_on_call_without_run,
                msg="When not running on updates, the call should not return anything"
            )
            return_on_explicit_run = node.run()
            self.assertEqual(
                return_on_explicit_run,
                plus_one(2),
                msg="On explicit run, the most recent input data should be used and the "
                    "result should be returned"
            )

        with self.subTest("Run on executor"):
            node.executor = CloudpickleProcessPoolExecutor()
            node.run_on_updates = False

            return_on_update_without_run = node.update()
            self.assertIsNone(
                return_on_update_without_run,
                msg="When not running on updates, the update should not return "
                    "anything whether there is an executor or not"
            )
            return_on_explicit_run = node.run()
            self.assertIsInstance(
                return_on_explicit_run,
                Future,
                msg="Running with an executor should return the future"
            )
            with self.assertRaises(RuntimeError):
                # The executor run should take a second
                # So we can double check that attempting to run while already running
                # raises an error
                node.run()
            node.future.result()  # Wait for the remote execution to finish

            node.run_on_updates = True
            return_on_update_with_run = node.update()
            self.assertIsInstance(
                return_on_update_with_run,
                Future,
                msg="Updating should return the same as run when we get a run from the "
                    "update, obviously..."
            )
            node.future.result()  # Wait for the remote execution to finish

@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestSlow(unittest.TestCase):
    def test_instantiation(self):
        slow = Slow(plus_one)
        self.assertIs(
            slow.outputs.y.value,
            NotData,
            msg="Slow nodes should not run at instantiation",
        )
        slow.inputs.x = 10
        self.assertIs(
            slow.outputs.y.value,
            NotData,
            msg="Slow nodes should not run on updates",
        )
        slow.run()
        self.assertEqual(
            slow.outputs.y.value,
            11,
            msg=f"Slow nodes should still run when asked! Expected 11 but got "
                f"{slow.outputs.y.value}"
        )

        node = Slow(no_default, 1, y=2, output_labels="output")
        node.run()
        self.assertEqual(
            no_default(1, 2),
            node.outputs.output.value,
            msg="Slow nodes should allow input initialization by arg and kwarg"
        )
        node(2, y=3)
        node.run()
        self.assertEqual(
            no_default(2, 3),
            node.outputs.output.value,
            msg="Slow nodes should allow input update on call by arg and kwarg"
        )


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestSingleValue(unittest.TestCase):
    def test_instantiation(self):
        node = SingleValue(no_default, 1, y=2, output_labels="output")
        self.assertEqual(
            no_default(1, 2),
            node.outputs.output.value,
            msg="Single value node should allow function input by arg and kwarg"
        )

        with self.assertRaises(ValueError):
            # Too many labels
            SingleValue(plus_one, output_labels=["z", "excess_label"])

    def test_item_and_attribute_access(self):
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

        svn = SingleValue(returns_foo, output_labels="foo")

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

    def test_repr(self):
        with self.subTest("Filled data"):
            svn = SingleValue(plus_one)
            self.assertEqual(
                svn.__repr__(), svn.outputs.y.value.__repr__(),
                msg="SingleValueNodes should have their output as their representation"
            )

        with self.subTest("Not data"):
            svn = SingleValue(no_default, output_labels="output")
            self.assertIs(svn.outputs.output.value, NotData)
            self.assertTrue(
                svn.__repr__().endswith(NotData.__name__),
                msg="When the output is still not data, the representation should "
                    "indicate this"
            )

    def test_str(self):
        svn = SingleValue(plus_one)
        self.assertTrue(
            str(svn).endswith(str(svn.single_value)),
            msg="SingleValueNodes should have their output as a string in their string "
                "representation (e.g., perhaps with a reminder note that this is "
                "actually still a Function and not just the value you're seeing.)"
        )

    def test_easy_output_connection(self):
        svn = SingleValue(plus_one)
        regular = Function(plus_one)

        regular.inputs.x = svn

        self.assertIn(
            svn.outputs.y, regular.inputs.x.connections,
            msg="SingleValueNodes should be able to make connections between their "
                "output and another node's input by passing themselves"
        )

        regular.run()
        self.assertEqual(
            regular.outputs.y.value, 3,
            msg="SingleValue connections should pass data just like usual; in this "
                "case default->plus_one->plus_one = 1 + 1 +1 = 3"
        )

        at_instantiation = Function(plus_one, x=svn)
        self.assertIn(
            svn.outputs.y, at_instantiation.inputs.x.connections,
            msg="The parsing of SingleValue output as a connection should also work"
                "from assignment at instantiation"
        )

    def test_channels_requiring_update_after_run(self):
        @single_value_node(output_labels="sum")
        def my_node(x: int = 0, y: int = 0, z: int = 0):
            return x + y + z

        n = my_node(channels_requiring_update_after_run=["x"])
        n.inputs.y.require_update_after_node_runs()
        n.inputs.z.require_update_after_node_runs(wait_now=True)

        self.assertTrue(
            n.inputs.x.waiting_for_update,
            msg="Should have to wait because it was passed at init"
        )
        self.assertFalse(
            n.inputs.y.waiting_for_update,
            msg="Should not have to wait, because the node has not run since it was set "
                "as requiring updates after runs."
        )
        self.assertTrue(
            n.inputs.z.waiting_for_update,
            msg="Should have to wait because it was told to wait now"
        )

        n.inputs.y.wait_for_update()

        n.inputs.x = 1
        self.assertFalse(
            n.inputs.x.waiting_for_update,
            msg="It got updated",
        )
        self.assertTrue(
            n.inputs.y.waiting_for_update and n.inputs.z.waiting_for_update,
            msg="They did not get updated"
        )
        self.assertFalse(
            n.ready,
            msg="Should still be waiting for y and z to get updated"
        )

        n.inputs.y = 2
        n.inputs.z = 3
        self.assertEqual(
            n.outputs.sum.value, 6,
            msg="Should have run after all inputs got updated"
        )
        self.assertTrue(
            n.inputs.x.waiting_for_update and
            n.inputs.y.waiting_for_update and
            n.inputs.z.waiting_for_update,
            msg="After the run, all three should now be waiting for updates again"
        )

    def test_working_directory(self):
        n_f = Function(plus_one)
        self.assertTrue(n_f._working_directory is None)
        self.assertIsInstance(n_f.working_directory, DirectoryObject)
        self.assertTrue(str(n_f.working_directory.path).endswith(n_f.label))
        n_f.working_directory.delete()


if __name__ == '__main__':
    unittest.main()
