from typing import Optional, Union
import unittest
import warnings

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.function import node_from_function, as_function_node
from pyiron_workflow.io import ConnectionCopyError, ValueCopyError
from pyiron_workflow.create import Executor


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


class TestFunction(unittest.TestCase):
    def test_instantiation(self):
        with self.subTest("Void function is allowable"):
            void_node = node_from_function(void)
            self.assertEqual(len(void_node.outputs), 1)

        with self.subTest("Args and kwargs at initialization"):
            node = node_from_function(plus_one)
            self.assertIs(
                NOT_DATA,
                node.outputs.y.value,
                msg="Sanity check that output just has the standard not-data value at "
                    "instantiation",
            )
            node.inputs.x = 10
            self.assertIs(
                node.outputs.y.value,
                NOT_DATA,
                msg="Nodes should not run on input updates",
            )
            node.run()
            self.assertEqual(
                node.outputs.y.value,
                11,
                msg=f"Expected the run to update the output -- did the test function"
                    f"change or something?"
            )

            node = node_from_function(no_default, 1, y=2, output_labels="output")
            node.run()
            self.assertEqual(
                no_default(1, 2),
                node.outputs.output.value,
                msg="Nodes should allow input initialization by arg _and_ kwarg"
            )
            node(2, y=3)
            self.assertEqual(
                no_default(2, 3),
                node.outputs.output.value,
                msg="Nodes should allow input update on call by arg and kwarg"
            )

            with self.assertRaises(ValueError):
                # Can't pass more args than the function takes
                node_from_function(returns_multiple, 1, 2, 3)

        with self.subTest("Initializing with connections"):
            node = node_from_function(plus_one, x=2)
            node2 = node_from_function(plus_one, x=node.outputs.y)
            self.assertIs(
                node2.inputs.x.connections[0],
                node.outputs.y,
                msg="Should be able to make a connection at initialization"
            )
            node >> node2
            node.run()
            self.assertEqual(4, node2.outputs.y.value, msg="Initialize from connection")

    def test_defaults(self):
        with_defaults = node_from_function(plus_one)
        self.assertEqual(
            with_defaults.inputs.x.value,
            1,
            msg=f"Expected to get the default provided in the underlying function but "
                f"got {with_defaults.inputs.x.value}",
        )
        without_defaults = node_from_function(no_default)
        self.assertIs(
            without_defaults.inputs.x.value,
            NOT_DATA,
            msg=f"Expected values with no default specified to start as {NOT_DATA} but "
                f"got {without_defaults.inputs.x.value}",
        )
        self.assertFalse(
            without_defaults.ready,
            msg="I guess we should test for behaviour and not implementation... Without"
                "defaults, the node should not be ready!"
        )

    def test_label_choices(self):
        with self.subTest("Automatically scrape output labels"):
            n = node_from_function(plus_one)
            self.assertListEqual(n.outputs.labels, ["y"])

        with self.subTest("Allow overriding them"):
            n = node_from_function(no_default, output_labels=("sum_plus_one",))
            self.assertListEqual(n.outputs.labels, ["sum_plus_one"])

        with self.subTest("Allow forcing _one_ output channel"):
            n = node_from_function(returns_multiple, output_labels="its_a_tuple")
            self.assertListEqual(n.outputs.labels, ["its_a_tuple"])

        with self.subTest("Fail on multiple return values"):
            with self.assertRaises(ValueError):
                # Can't automatically parse output labels from a function with multiple
                # return expressions
                node_from_function(multiple_branches)

        with self.subTest("Override output label scraping"):
            switch = node_from_function(multiple_branches, output_labels="bool")
            self.assertListEqual(switch.outputs.labels, ["bool"])

    def test_default_label(self):
        n = node_from_function(plus_one)
        self.assertEqual(plus_one.__name__, n.label)

    def test_availability_of_node_function(self):
        @as_function_node()
        def linear(x):
            return x

        @as_function_node()
        def bilinear(x, y):
            xy = linear.node_function(x) * linear.node_function(y)
            return xy

        self.assertEqual(
            bilinear(2, 3).run(),
            2 * 3,
            msg="Children of `node_from_function` should have their `node_function` exposed for "
                "use at the class level"
        )

    def test_preview_output_channels(self):
        @as_function_node()
        def Foo(x):
            return x

        self.assertDictEqual(
            {"x": None},
            Foo.preview_output_channels(),
            msg="Should parse without label or hint."
        )

        @as_function_node("y")
        def Foo(x) -> None:
            return x

        self.assertDictEqual(
            {"y": type(None)},
            Foo.preview_output_channels(),
            msg="Should parse with label and hint."
        )

        with self.assertRaises(
            ValueError,
            msg="Should fail when scraping incommensurate hints and returns"
        ):
            @as_function_node()
            def Foo(x) -> int:
                y, z = 5.0, 5
                return x, y, z

        with self.assertRaises(
            ValueError,
            msg="Should fail when provided labels are incommensurate with hints"
        ):
            @as_function_node("xo", "yo", "zo")
            def Foo(x) -> int:
                y, z = 5.0, 5
                return x, y, z

        @as_function_node("xo", "yo")
        def Foo(x) -> tuple[int, float]:
            y, z = 5.0, 5
            return x

        self.assertDictEqual(
            {"xo": int, "yo": float},
            Foo.preview_output_channels(),
            msg="The user carries extra responsibility if they specify return values "
                "-- we don't even try scraping the returned stuff and it's up to them "
                "to make sure everything is commensurate! This is necessary so that "
                "source code scraping can get bypassed sometimes (e.g. for dynamically "
                "generated code that is only in memory and thus not inspectable)"
        )

    def test_preview_input_channels(self):
        @as_function_node()
        def Foo(x, y: int = 42):
            return x + y

        self.assertDictEqual(
            {"x": (None, NOT_DATA), "y": (int, 42)},
            Foo.preview_input_channels(),
            msg="Input specifications should be available at the class level, with or "
                "without type hints and/or defaults provided."
        )

    def test_statuses(self):
        n = node_from_function(plus_one)
        self.assertTrue(n.ready)
        self.assertFalse(n.running)
        self.assertFalse(n.failed)

        n.inputs.x = "Can't be added together with an int"
        with self.assertRaises(
            TypeError,
            msg="We expect the int+str type error because there were no type hints "
                "guarding this function from running with bad data"
        ):
            n.run()
        self.assertFalse(n.ready)
        self.assertFalse(n.running)
        self.assertTrue(n.failed)

    def test_with_self(self):
        def with_self(self, x: float) -> float:
            # Note: Adding internal state to the node like this goes against the best
            #  practice of keeping nodes "functional". Following python's paradigm of
            #  giving users lots of power, we want to guarantee that this behaviour is
            #  _possible_.
            if "some_counter" in self._user_data:
                self._user_data["some_counter"] += 1
            else:
                self._user_data["some_counter"] = 1
            return x + 0.1

        node = node_from_function(with_self, output_labels="output")
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
        node.inputs.x = 1.0
        node.run()
        self.assertEqual(
            node.outputs.output.value,
            1.1,
            msg="Basic node functionality appears to have failed"
        )
        self.assertEqual(
            node._user_data["some_counter"],
            1,
            msg="node_from_function functions should be able to modify attributes on the node "
                "object."
        )

        node.executor = Executor()
        with self.assertRaises(
            ValueError,
            msg="We haven't implemented any way to update a function node's `self` when"
                "it runs on an executor, so trying to do so should fail hard"
        ):
            node.run()
            node.executor_shutdown()  # Shouldn't get this far, but if we do shutdown
        node.executor = None

        def with_messed_self(x: float, self) -> float:
            return x + 0.1

        with warnings.catch_warnings(record=True) as warning_list:
            node = node_from_function(with_messed_self)
            self.assertTrue("self" in node.inputs.labels)

        self.assertEqual(len(warning_list), 1)

    def test_call(self):
        node = node_from_function(no_default, output_labels="output")

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
        node = node_from_function(plus_one)

        with self.subTest("Run on main process"):
            node.inputs.x = 2
            return_on_explicit_run = node.run()
            self.assertEqual(
                return_on_explicit_run,
                plus_one(2),
                msg="On explicit run, the most recent input data should be used and "
                    "the result should be returned"
            )

            return_on_call = node(1)
            self.assertEqual(
                return_on_call,
                plus_one(1),
                msg="Run output should be returned on call"
                # This is a duplicate test, since __call__ just invokes run, but it is
                # such a core promise that let's just double-check it
            )

    def test_copy_connections(self):
        node = node_from_function(plus_one)

        upstream = node_from_function(plus_one)
        to_copy = node_from_function(plus_one, x=upstream.outputs.y)
        downstream = node_from_function(plus_one, x=to_copy.outputs.y)
        upstream >> to_copy >> downstream

        wrong_io = node_from_function(
            returns_multiple, x=upstream.outputs.y, y=upstream.outputs.y
        )
        downstream.inputs.x.connect(wrong_io.outputs.y)

        with self.subTest("Successful copy"):
            node._copy_connections(to_copy)
            self.assertIn(upstream.outputs.y, node.inputs.x.connections)
            self.assertIn(upstream.signals.output.ran, node.signals.input.run)
            self.assertIn(downstream.inputs.x, node.outputs.y.connections)
            self.assertIn(downstream.signals.input.run, node.signals.output.ran)
        node.disconnect()  # Make sure you've got a clean slate

        def plus_one_hinted(x: int = 0) -> int:
            y = x + 1
            return y

        hinted_node = node_from_function(plus_one_hinted)

        with self.subTest("Ensure failed copies fail cleanly"):
            with self.assertRaises(ConnectionCopyError, msg="Wrong labels"):
                node._copy_connections(wrong_io)
            self.assertFalse(
                node.connected,
                msg="The x-input connection should have been copied, but should be "
                    "removed when the copy fails."
            )

            with self.assertRaises(
                ConnectionCopyError,
                msg="An unhinted channel is not a valid connection for a hinted "
                    "channel, and should raise and exception"
            ):
                hinted_node._copy_connections(to_copy)
        hinted_node.disconnect()# Make sure you've got a clean slate
        node.disconnect()  # Make sure you've got a clean slate

        with self.subTest("Ensure that failures can be continued past"):
            node._copy_connections(wrong_io, fail_hard=False)
            self.assertIn(upstream.outputs.y, node.inputs.x.connections)
            self.assertIn(downstream.inputs.x, node.outputs.y.connections)

            hinted_node._copy_connections(to_copy, fail_hard=False)
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
        @as_function_node()
        def reference(x=0, y: int = 0, z: int | float = 0, omega=None, extra_here=None):
            out = 42
            return out

        @as_function_node()
        def all_floats(x=1.1, y=1.1, z=1.1, omega=NOT_DATA, extra_there=None) -> float:
            out = 42.1
            return out

        # Instantiate the nodes and run them (so they have output data too)
        ref = reference()
        floats = all_floats()
        ref()
        floats.run(
            check_readiness=False,
            # We force-skip the readiness check since we are explicitly _trying_ to
            # have one of the inputs be `NOT_DATA` -- a value which triggers the channel
            # to be "not ready"
        )

        ref._copy_values(floats)
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
            msg="NOT_DATA should be ignored when copying"
        )
        self.assertEqual(
            ref.outputs.out.value,
            42.1,
            msg="Output data should also get copied"
        )
        # Note also that these nodes each have extra channels the other doesn't that
        # are simply ignored

        @as_function_node()
        def extra_channel(x=1, y=1, z=1, not_present=42):
            out = 42
            return out

        extra = extra_channel()
        extra()

        ref.inputs.x = 0  # Revert the value
        with self.assertRaises(
            ValueCopyError,
            msg="Type hint should prevent update when we fail hard"
        ):
            ref._copy_values(floats, fail_hard=True)

        ref._copy_values(extra)  # No problem
        with self.assertRaises(
            ValueCopyError,
            msg="Missing a channel that holds data is also grounds for failure"
        ):
            ref._copy_values(extra, fail_hard=True)
            
    def test_easy_output_connection(self):
        n1 = node_from_function(plus_one)
        n2 = node_from_function(plus_one)

        n2.inputs.x = n1

        self.assertIn(
            n1.outputs.y, n2.inputs.x.connections,
            msg="Single-output functions should be able to make connections between "
                "their output and another node's input by passing themselves"
        )

        n1 >> n2
        n1.run()
        self.assertEqual(
            n2.outputs.y.value, 3,
            msg="Single-output function connections should pass data just like usual; "
                "in this case default->plus_one->plus_one = 1 + 1 +1 = 3"
        )

        at_instantiation = node_from_function(plus_one, x=n1)
        self.assertIn(
            n1.outputs.y, at_instantiation.inputs.x.connections,
            msg="The parsing of Single-output functions' output as a connection should "
                "also work from assignment at instantiation"
        )

    def test_nested_declaration(self):
        # It's really just a silly case of running without a parent, where you don't
        # store references to all the nodes declared
        node = node_from_function(
            plus_one,
            x=node_from_function(
                plus_one,
                x=node_from_function(
                    plus_one,
                    x=2
                )
            )
        )
        self.assertEqual(2 + 1 + 1 + 1, node.pull())
        
    def test_single_output_item_and_attribute_access(self):
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

        single_output = node_from_function(returns_foo, output_labels="foo")

        self.assertEqual(
            single_output.connected,
            False,
            msg="Should return the _node_ attribute, not acting on the output channel"
        )

        injection = single_output[0]  # Should pass cleanly, even though it tries to run
        single_output.run()

        self.assertEqual(
            single_output.some_attribute.value,  # The call runs the dynamic node
            "exists",
            msg="Should fall back to acting on the output channel and creating a node"
        )

        self.assertEqual(
            single_output.connected,
            True,
            msg="Should now be connected to the dynamically created nodes"
        )

        with self.assertRaises(
            AttributeError,
            msg="Aggressive running hits the problem that no such attribute exists"
        ):
            single_output.doesnt_exists_anywhere

        self.assertEqual(
            injection(),
            True,
            msg="Should be able to query injection later"
        )

        self.assertEqual(
            single_output["some other key"].value,
            False,
            msg="Should fall back to looking on the single value"
        )

        with self.assertRaises(
            AttributeError,
            msg="Attribute injection should not work for private attributes"
        ):
            single_output._some_nonexistant_private_var


if __name__ == '__main__':
    unittest.main()
