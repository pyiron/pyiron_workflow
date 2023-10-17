from concurrent.futures import Future
from sys import version_info
from typing import Optional, Union
import unittest
import warnings

# from pyiron_contrib.executors import CloudpickleProcessPoolExecutor as Executor
# from pympipool.mpi.executor import PyMPISingleTaskExecutor as Executor

from pyiron_workflow.executors import CloudpickleProcessPoolExecutor as Executor

from pyiron_workflow.channels import NotData, ChannelConnectionError
from pyiron_workflow.files import DirectoryObject
from pyiron_workflow.function import (
    Function, SingleValue, function_node, single_value_node
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
            node = Function(plus_one)
            self.assertIs(
                node.outputs.y.value,
                NotData,
                msg="Nodes should not run at instantiation",
            )
            node.inputs.x = 10
            self.assertIs(
                node.outputs.y.value,
                NotData,
                msg="Nodes should not run on input updates",
            )
            node.run()
            self.assertEqual(
                node.outputs.y.value,
                11,
                msg=f"Slow nodes should still run when asked! Expected 11 but got "
                    f"{node.outputs.y.value}"
            )

            node = Function(no_default, 1, y=2, output_labels="output")
            node.run()
            self.assertEqual(
                no_default(1, 2),
                node.outputs.output.value,
                msg="Nodes should allow input initialization by arg and kwarg"
            )
            node(2, y=3)
            node.run()
            self.assertEqual(
                no_default(2, 3),
                node.outputs.output.value,
                msg="Nodes should allow input update on call by arg and kwarg"
            )

            with self.assertRaises(ValueError):
                # Can't pass more args than the function takes
                Function(returns_multiple, 1, 2, 3)

        with self.subTest("Initializing with connections"):
            node = Function(plus_one, x=2)
            node2 = Function(plus_one, x=node.outputs.y)
            self.assertIs(
                node2.inputs.x.connections[0],
                node.outputs.y,
                msg="Should be able to make a connection at initialization"
            )
            node > node2
            node.run()
            self.assertEqual(4, node2.outputs.y.value, msg="Initialize from connection")

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

    def test_availability_of_node_function(self):
        @function_node()
        def linear(x):
            return x

        @function_node()
        def bilinear(x, y):
            xy = linear.node_function(x) * linear.node_function(y)
            return xy

        self.assertEqual(
            bilinear(2, 3).run(),
            2 * 3,
            msg="Children of `Function` should have their `node_function` exposed for "
                "use at the class level"
        )

    def test_signals(self):
        @function_node()
        def linear(x):
            return x

        @function_node()
        def times_two(y):
            return 2 * y

        l = linear(x=1)
        t2 = times_two(
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

        with self.subTest("Test syntactic sugar"):
            t2.signals.input.run.disconnect_all()
            l > t2
            self.assertIn(
                l.signals.output.ran,
                t2.signals.input.run.connections,
                msg="> should be equivalent to run/ran connection"
            )

            t2.signals.input.run.disconnect_all()
            l > t2.signals.input.run
            self.assertIn(
                l.signals.output.ran,
                t2.signals.input.run.connections,
                msg="> should allow us to mix and match nodes and signal channels"
            )

            t2.signals.input.run.disconnect_all()
            l.signals.output.ran > t2
            self.assertIn(
                l.signals.output.ran,
                t2.signals.input.run.connections,
                msg="Mixing and matching should work both directions"
            )

            t2.signals.input.run.disconnect_all()
            l > t2 > l
            self.assertTrue(
                l.signals.input.run.connections[0] is t2.signals.output.ran
                and t2.signals.input.run.connections[0] is l.signals.output.ran,
                msg="> should allow chaining signal connections"
            )

    def test_statuses(self):
        n = Function(plus_one)
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
        self.assertFalse(n.running)
        self.assertTrue(n.failed)

        n.inputs.x = 1
        self.assertFalse(
            n.ready,
            msg="Should not be ready while it has failed status"
        )

        n.failed = False  # Manually reset the failed status
        self.assertTrue(
            n.ready,
            msg="Input is ok, not running, not failed -- should be ready!"
        )
        n.run()
        self.assertTrue(n.ready)
        self.assertFalse(n.running)
        self.assertFalse(
            n.failed,
            msg="Should be back to a good state and ready to run again"
        )

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

        node.executor = Executor()
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
        node = Function(no_default, output_labels="output")

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
                node.inputs.x.value,
                1,
                msg="__call__ should accept args to update input"
            )
            self.assertEqual(
                node.inputs.y.value,
                2,
                msg="__call__ should accept kwargs to update input"
            )
            self.assertEqual(
                node.outputs.output.value, 1 + 2 + 1, msg="__call__ should run things"
            )

            node(3)  # Implicitly test partial update
            self.assertEqual(
                no_default(3, 2),
                node.outputs.output.value,
                msg="__call__ should allow updating only _some_ input before running"
            )

        with self.subTest("Check that bad kwargs don't stop good ones"):
            with self.assertWarns(Warning):
                original_label = node.label
                node(4, label="won't get read", y=5, foobar="not a kwarg of any sort")

                self.assertEqual(
                    node.label,
                    original_label,
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

            node.inputs.x = 2
            return_on_explicit_run = node.run()
            self.assertEqual(
                return_on_explicit_run,
                plus_one(2),
                msg="On explicit run, the most recent input data should be used and the "
                    "result should be returned"
            )

        with self.subTest("Run on executor"):
            node.executor = Executor()

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

    def test_copy_connections(self):
        node = Function(plus_one)

        upstream = Function(plus_one)
        to_copy = Function(plus_one, x=upstream.outputs.y)
        downstream = Function(plus_one, x=to_copy.outputs.y)
        upstream > to_copy > downstream

        wrong_io = Function(
            returns_multiple, x=upstream.outputs.y, y=upstream.outputs.y
        )
        downstream.inputs.x.connect(wrong_io.outputs.y)

        with self.subTest("Successful copy"):
            node.copy_connections(to_copy)
            self.assertIn(upstream.outputs.y, node.inputs.x.connections)
            self.assertIn(upstream.signals.output.ran, node.signals.input.run)
            self.assertIn(downstream.inputs.x, node.outputs.y.connections)
            self.assertIn(downstream.signals.input.run, node.signals.output.ran)
        node.disconnect()  # Make sure you've got a clean slate

        def plus_one_hinted(x: int = 0) -> int:
            y = x + 1
            return y

        hinted_node = Function(plus_one_hinted)

        with self.subTest("Ensure failed copies fail cleanly"):
            with self.assertRaises(AttributeError, msg="Wrong labels"):
                node.copy_connections(wrong_io)
            self.assertFalse(
                node.connected,
                msg="The x-input connection should have been copied, but should be "
                    "removed when the copy fails."
            )

            with self.assertRaises(
                ChannelConnectionError,
                msg="An unhinted channel is not a valid connection for a hinted "
                    "channel, and should raise and exception"
            ):
                hinted_node.copy_connections(to_copy)
        hinted_node.disconnect()# Make sure you've got a clean slate
        node.disconnect()  # Make sure you've got a clean slate

        with self.subTest("Ensure that failures can be continued past"):
            node.copy_connections(wrong_io, fail_hard=False)
            self.assertIn(upstream.outputs.y, node.inputs.x.connections)
            self.assertIn(downstream.inputs.x, node.outputs.y.connections)

            hinted_node.copy_connections(to_copy, fail_hard=False)
            self.assertFalse(
                hinted_node.inputs.connected,
                msg="Without hard failure the copy should be allowed to proceed, but "
                    "we don't actually expect any connections to get copied since the "
                    "only one available had type hint problems"
            )
            self.assertTrue(
                hinted_node.outputs.connected,
                msg="Without hard failure the copy should be allowed to proceed, so "
                    "the output should connect fine since feeding hinted to un-hinted "
                    "is a-ok"
            )

    def test_copy_values(self):
        @function_node()
        def reference(x=0, y: int = 0, z: int | float = 0, omega=None, extra_here=None):
            out = 42
            return out

        @function_node()
        def all_floats(x=1.1, y=1.1, z=1.1, omega=NotData, extra_there=None) -> float:
            out = 42.1
            return out

        # Instantiate the nodes and run them (so they have output data too)
        ref = reference()
        floats = all_floats()
        ref()
        floats()

        ref.copy_values(floats)
        self.assertEqual(
            ref.inputs.x.value,
            1.1,
            msg="Untyped channels should copy freely"
        )
        self.assertEqual(
            ref.inputs.y.value,
            0,
            msg="Typed channels should ignore values where the type check fails"
        )
        self.assertEqual(
            ref.inputs.z.value,
            1.1,
            msg="Typed channels should copy values that conform to their hint"
        )
        self.assertEqual(
            ref.inputs.omega.value,
            None,
            msg="NotData should be ignored when copying"
        )
        self.assertEqual(
            ref.outputs.out.value,
            42.1,
            msg="Output data should also get copied"
        )
        # Note also that these nodes each have extra channels the other doesn't that
        # are simply ignored

        @function_node()
        def extra_channel(x=1, y=1, z=1, not_present=42):
            out = 42
            return out

        extra = extra_channel()
        extra()

        ref.inputs.x = 0  # Revert the value
        with self.assertRaises(
            TypeError,
            msg="Type hint should prevent update when we fail hard"
        ):
            ref.copy_values(floats, fail_hard=True)

        ref.copy_values(extra)  # No problem
        with self.assertRaises(
            AttributeError,
            msg="Missing a channel that holds data is also grounds for failure"
        ):
            ref.copy_values(extra, fail_hard=True)


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestSingleValue(unittest.TestCase):
    def test_instantiation(self):
        node = SingleValue(no_default, 1, y=2, output_labels="output")
        node.run()
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
        svn.run()

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
            svn.run()
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
        svn.run()
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

        svn > regular
        svn.run()
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

    def test_working_directory(self):
        n_f = Function(plus_one)
        self.assertTrue(n_f._working_directory is None)
        self.assertIsInstance(n_f.working_directory, DirectoryObject)
        self.assertTrue(str(n_f.working_directory.path).endswith(n_f.label))
        n_f.working_directory.delete()

    def test_disconnection(self):
        n1 = Function(no_default, output_labels="out")
        n2 = Function(no_default, output_labels="out")
        n3 = Function(no_default, output_labels="out")
        n4 = Function(plus_one)

        n3.inputs.x = n1.outputs.out
        n3.inputs.y = n2.outputs.out
        n4.inputs.x = n3.outputs.out
        n2 > n3 > n4
        disconnected = n3.disconnect()
        self.assertListEqual(
            disconnected,
            [
                # Inputs
                (n3.inputs.x, n1.outputs.out),
                (n3.inputs.y, n2.outputs.out),
                # Outputs
                (n3.outputs.out, n4.inputs.x),
                # Signals (inputs, then output)
                (n3.signals.input.run, n2.signals.output.ran),
                (n3.signals.output.ran, n4.signals.input.run),
            ],
            msg="Expected to find pairs (starting with the node disconnect was called "
                "on) of all broken connections among input, output, and signals."
        )


if __name__ == '__main__':
    unittest.main()
