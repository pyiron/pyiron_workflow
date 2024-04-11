import sys
from concurrent.futures import Future
from functools import partialmethod

from time import sleep
import unittest


from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.function import function_node
from pyiron_workflow.macro import Macro, macro_node, as_macro_node
from pyiron_workflow.topology import CircularDataFlowError


def add_one(x):
    result = x + 1
    return result


def add_three_macro(macro, one__x):
    macro.one = function_node(add_one, x=one__x)
    function_node(add_one, macro.one, label="two", parent=macro)
    macro.add_child(function_node(add_one, macro.two, label="three"))
    # Cover a handful of addition methods,
    # although these are more thoroughly tested in Workflow tests
    return macro.three


def wrong_return_macro(macro):
    macro.one = function_node(add_one)
    return 3


class TestMacro(unittest.TestCase):

    def test_io_independence(self):
        m = macro_node(add_three_macro, output_labels="three__result")
        self.assertIsNot(
            m.inputs.one__x,
            m.one.inputs.x,
            msg="Expect input to be by value, not by reference"
        )
        self.assertIsNot(
            m.outputs.three__result,
            m.three.outputs.result,
            msg="Expect output to be by value, not by reference"
        )
        self.assertFalse(
            m.connected,
            msg="Macro should talk to its children by value links _not_ graph "
                "connections"
        )

    def test_value_links(self):
        m = macro_node(add_three_macro, output_labels="three__result")
        self.assertIs(
            m.one.inputs.x,
            m.inputs.one__x.value_receiver,
            msg="Sanity check that value link exists"
        )
        self.assertIs(
            m.outputs.three__result,
            m.three.outputs.result.value_receiver,
            msg="Sanity check that value link exists"
        )
        self.assertNotEqual(
            42, m.one.inputs.x.value, msg="Sanity check that we start from expected"
        )
        self.assertNotEqual(
            42,
            m.three.outputs.result.value,
            msg="Sanity check that we start from expected"
        )
        m.inputs.one__x.value = 0
        self.assertEqual(
            0, m.one.inputs.x.value, msg="Expected values to stay synchronized"
        )
        m.three.outputs.result.value = 0
        self.assertEqual(
            0, m.outputs.three__result.value, msg="Expected values to stay synchronized"
        )

    def test_execution_automation(self):
        fully_automatic = add_three_macro

        def fully_defined(macro, one__x):
            add_three_macro(macro, one__x=one__x)
            macro.one >> macro.two >> macro.three
            macro.starting_nodes = [macro.one]
            return macro.three

        def only_order(macro, one__x):
            add_three_macro(macro, one__x=one__x)
            macro.two >> macro.three
            return macro.three

        def only_starting(macro, one__x):
            add_three_macro(macro, one__x=one__x)
            macro.starting_nodes = [macro.one]
            return macro.three

        m_auto = macro_node(fully_automatic, output_labels="three__result")
        m_user = macro_node(fully_defined, output_labels="three__result")

        x = 0
        expected = add_one(add_one(add_one(x)))
        self.assertEqual(
            m_auto(one__x=x).three__result,
            expected,
            "DAG macros should run fine without user specification of execution."
        )
        self.assertEqual(
            m_user(one__x=x).three__result,
            expected,
            "Macros should run fine if the user nicely specifies the exeuction graph."
        )

        with self.subTest("Partially specified execution should fail"):
            # We don't yet check for _crappy_ user-defined execution,
            # But we should make sure it's at least valid in principle
            with self.assertRaises(ValueError):
                macro_node(only_order, output_labels="three__result")

            with self.assertRaises(ValueError):
                macro_node(only_starting, output_labels="three__result")

    def test_default_label(self):
        m = macro_node(add_three_macro, output_labels="three__result")
        self.assertEqual(
            m.label,
            add_three_macro.__name__,
            msg="Label should be automatically generated"
        )
        label = "custom_name"
        m2 = macro_node(add_three_macro, label=label, output_labels="three__result")
        self.assertEqual(m2.label, label, msg="Should be able to specify a label")

    def test_creation_from_decorator(self):
        m = as_macro_node("three__result")(add_three_macro)()

        self.assertIs(
            m.outputs.three__result.value,
            NOT_DATA,
            msg="Output should be accessible with the usual naming convention, but we "
                "have not run yet so there shouldn't be any data"
        )

        input_x = 1
        expected_value = add_one(add_one(add_one(input_x)))
        print(m.inputs.labels, m.outputs.labels, m.child_labels)
        out = m(one__x=input_x)  # Take kwargs to set input at runtime

        self.assertEqual(
            out.three__result,
            expected_value,
            msg="Macros should return the output, just like other nodes"
        )
        self.assertEqual(
            m.outputs.three__result.value,
            expected_value,
            msg="Macros should get output updated, just like other nodes"
        )

    def test_creation_from_subclass(self):
        class MyMacro(Macro):
            _provided_output_labels = ("three__result",)

            @staticmethod
            def graph_creator(self, one__x):
                add_three_macro(self, one__x)
                return self.three

        x = 0
        m = MyMacro(one__x=x)
        m.run()
        self.assertEqual(
            m.outputs.three__result.value,
            add_one(add_one(add_one(x))),
            msg="Subclasses should be able to simply override the graph_creator arg"
        )

    def test_nesting(self):
        def nested_macro(macro, a__x):
            macro.a = function_node(add_one, a__x)
            macro.b = macro_node(
                add_three_macro,
                one__x=macro.a,
                output_labels="three__result"
            )
            macro.c = macro_node(
                add_three_macro,
                one__x=macro.b.outputs.three__result,
                output_labels="three__result"
            )
            macro.d = function_node(
                add_one,
                x=macro.c.outputs.three__result,
            )
            macro.a >> macro.b >> macro.c >> macro.d
            macro.starting_nodes = [macro.a]
            # This definition of the execution graph is not strictly necessary in this
            # simple DAG case; we just do it to make sure nesting definied/automatic
            # macros works ok
            return macro.d

        m = macro_node(nested_macro, output_labels="d__result")
        self.assertEqual(m(a__x=0).d__result, 8)

    def test_with_executor(self):
        macro = macro_node(add_three_macro, output_labels="three__result")
        downstream = function_node(add_one, x=macro.outputs.three__result)
        macro >> downstream  # Manually specify since we'll run the macro but look
        # at the downstream output, and none of this is happening in a workflow

        original_one = macro.one
        macro.executor = macro.create.Executor()

        self.assertIs(
            NOT_DATA,
            macro.outputs.three__result.value,
            msg="Sanity check that test is in right starting condition"
        )

        result = macro.run(one__x=0)
        self.assertIsInstance(
            result,
            Future,
            msg="Should be running as a parallel process"
        )
        self.assertIs(
            NOT_DATA,
            downstream.outputs.result.value,
            msg="Downstream events should not yet have triggered either, we should wait"
                "for the callback when the result is ready"
        )

        returned_nodes = result.result(timeout=120)  # Wait for the process to finish
        sleep(1)
        self.assertFalse(
            macro.running,
            msg="Macro should be done running"
        )
        self.assertIsNot(
            original_one,
            returned_nodes.one,
            msg="Executing in a parallel process should be returning new instances"
        )
        # self.assertIs(
        #     returned_nodes.one,
        #     macro.nodes.one,
        #     msg="Returned nodes should be taken as children"
        # )  # You can't do this, result.result() is returning new instances each call
        self.assertIs(
            macro,
            macro.one.parent,
            msg="Returned nodes should get the macro as their parent"
            # Once upon a time there was some evidence that this test was failing
            # stochastically, but I just ran the whole test suite 6 times and this test
            # 8 times and it always passed fine, so maybe the issue is resolved...
        )
        self.assertIsNone(
            original_one.parent,
            msg="Original nodes should be orphaned"
            # Note: At time of writing, this is accomplished in Node.__getstate__,
            #       which feels a bit dangerous...
        )
        self.assertEqual(
            0 + 3,
            macro.outputs.three__result.value,
            msg="And of course we expect the calculation to actually run"
        )
        self.assertIs(
            downstream.inputs.x.connections[0],
            macro.outputs.three__result,
            msg=f"The macro output should still be connected to downstream"
        )
        sleep(0.2)  # Give a moment for the ran signal to emit and downstream to run
        # I'm a bit surprised this sleep is necessary
        self.assertEqual(
            0 + 3 + 1,
            downstream.outputs.result.value,
            msg="The finishing callback should also fire off the ran signal triggering"
                "downstream execution"
        )

        macro.executor_shutdown()

    def test_pulling_from_inside_a_macro(self):
        upstream = function_node(add_one, x=2)
        macro = macro_node(add_three_macro, one__x=upstream, output_labels="three__result")
        macro.inputs.one__x = 0  # Set value
        # Now macro.one.inputs.x has both value and a connection

        self.assertEqual(
            0 + 1 + 1,
            macro.two.pull(run_parent_trees_too=False),
            msg="Without running parent trees, the pulling should only run upstream "
                "nodes _inside_ the scope of the macro, relying on the explicit input"
                "value"
        )

        self.assertEqual(
            (2 + 1) + 1 + 1,
            macro.two.pull(run_parent_trees_too=True),
            msg="Running with parent trees, the pulling should also run the parents "
                "data dependencies first"
        )

    def test_recovery_after_failed_pull(self):
        def grab_x_and_run(node):
            """Grab a couple connections from an add_one-like node"""
            return node.inputs.x.connections + node.signals.input.run.connections

        with self.subTest("When the local scope has cyclic data flow"):
            def cyclic_macro(macro):
                macro.one = function_node(add_one)
                macro.two = function_node(add_one, x=macro.one)
                macro.one.inputs.x = macro.two
                macro.one >> macro.two
                macro.starting_nodes = [macro.one]
                # We need to manually specify execution since the data flow is cyclic

            m = macro_node(cyclic_macro)

            initial_labels = list(m.children.keys())

            def grab_connections(macro):
                return grab_x_and_run(macro.one) + grab_x_and_run(macro.two)

            initial_connections = grab_connections(m)

            with self.assertRaises(
                CircularDataFlowError,
                msg="Pull should only work for DAG workflows"
            ):
                m.two.pull()
            self.assertListEqual(
                initial_labels,
                list(m.children.keys()),
                msg="Labels should be restored after failing to pull because of "
                    "acyclicity"
            )
            self.assertTrue(
                all(
                    c is ic for (c, ic) in zip(grab_connections(m), initial_connections)
                ),
                msg="Connections should be restored after failing to pull because of "
                    "cyclic data flow"
            )

        with self.subTest("When the parent scope has cyclic data flow"):
            n1 = function_node(add_one, label="n1", x=0)
            n2 = function_node(add_one, label="n2", x=n1)
            m = macro_node(
                add_three_macro, label="m", one__x=n2, output_labels="three__result"
            )

            self.assertEqual(
                0 + 1 + 1 + (1 + 1 + 1),
                m.three.pull(run_parent_trees_too=True),
                msg="Sanity check, without cyclic data flows pulling here should be ok"
            )

            n1.inputs.x = n2

            initial_connections = grab_x_and_run(n1) + grab_x_and_run(n2)
            with self.assertRaises(
                CircularDataFlowError,
                msg="Once the outer scope has circular data flows, pulling should fail"
            ):
                m.three.pull(run_parent_trees_too=True)
            self.assertTrue(
                all(
                    c is ic
                    for (c, ic) in zip(
                        grab_x_and_run(n1) + grab_x_and_run(n2), initial_connections
                    )
                ),
                msg="Connections should be restored after failing to pull because of "
                    "cyclic data flow in the outer scope"
            )
            self.assertEqual(
                "n1",
                n1.label,
                msg="Labels should get restored in the outer scope"
            )
            self.assertEqual(
                "one",
                m.one.label,
                msg="Labels should not have even gotten perturbed to start with in the"
                    "inner scope"
            )

        with self.subTest("When a node breaks upstream"):
            def fail_at_zero(x):
                y = 1 / x
                return y

            n1 = function_node(fail_at_zero, x=0)
            n2 = function_node(add_one, x=n1, label="n1")
            n_not_used = function_node(add_one)
            n_not_used >> n2  # Just here to make sure it gets restored

            with self.assertRaises(
                ZeroDivisionError,
                msg="The underlying error should get raised"
            ):
                n2.pull()
            self.assertEqual(
                "n1",
                n2.label,
                msg="Original labels should get restored on upstream failure"
            )
            self.assertIs(
                n_not_used,
                n2.signals.input.run.connections[0].owner,
                msg="Original connections should get restored on upstream failure"
            )

    def test_output_labels_vs_return_values(self):
        def no_return(macro):
            macro.foo = macro.create.standard.UserInput()

        macro_node(no_return)  # Neither is fine

        @as_macro_node("some_return")
        def LabelsAndReturnsMatch(macro):
            macro.foo = macro.create.standard.UserInput()
            return macro.foo

        LabelsAndReturnsMatch()  # Both is fine

        with self.assertRaises(
            ValueError,
            msg="The number of output labels and return values must match"
        ):
            @as_macro_node("some_return", "nonexistent")
            def MissingReturn(macro):
                macro.foo = macro.create.standard.UserInput()
                return macro.foo

        with self.assertRaises(
            TypeError,
            msg="Output labels must be there if return values are"
        ):
            @as_macro_node()
            def MissingLabel(macro):
                macro.foo = macro.create.standard.UserInput()
                return macro.foo

        with self.assertRaises(
            TypeError,
            msg="Return values must be there if output labels are"
        ):
            @as_macro_node("some_label")
            def MissingLabel(macro):
                macro.foo = macro.create.standard.UserInput()

    def test_functionlike_io_parsing(self):
        """
        Check that various aspects of the IO are parsing from the function signature
        and returns, and labels
        """

        @as_macro_node("lout", "n_plus_2")
        def LikeAFunction(macro, lin: list,  n: int = 2):
            macro.plus_two = n + 2
            macro.sliced_list = lin[n:macro.plus_two]
            macro.double_fork = 2 * n
            # ^ This is vestigial, just to show we don't need to blacklist it
            # Test returning both a single value node and an output channel,
            # even though here we could just use the node both times
            return macro.sliced_list, macro.plus_two.channel

        macro = LikeAFunction(n=1, lin=[1, 2, 3, 4, 5, 6])
        self.assertListEqual(["lin", "n"], macro.inputs.labels)
        self.assertDictEqual({"n_plus_2": 3, "lout": [2, 3]}, macro())

    def test_efficient_signature_interface(self):
        with self.subTest("Forked input"):
            @as_macro_node("output")
            def MutlipleUseInput(macro, x):
                macro.n1 = macro.create.standard.UserInput(x)
                macro.n2 = macro.create.standard.UserInput(x)
                return macro.n1

            m = MutlipleUseInput()
            self.assertEqual(
                2 + 1,
                len(m),
                msg="Signature input that is forked to multiple children should result "
                    "in the automatic creation of a new node to manage the forking."

            )

        with self.subTest("Single destination input"):
            @as_macro_node("output")
            def SingleUseInput(macro, x):
                macro.n = macro.create.standard.UserInput(x)
                return macro.n

            m = SingleUseInput()
            self.assertEqual(
                1,
                len(m),
                msg=f"Signature input with only one destination should not create an "
                    f"interface node. Found nodes {m.child_labels}"
            )

        with self.subTest("Mixed input"):
            @as_macro_node("output")
            def MixedUseInput(macro, x, y):
                macro.n1 = macro.create.standard.UserInput(x)
                macro.n2 = macro.create.standard.UserInput(y)
                macro.n3 = macro.create.standard.UserInput(y)
                return macro.n1

            m = MixedUseInput()
            self.assertEqual(
                3 + 1,
                len(m),
                msg=f"Mixing forked and single-use input should not cause problems. "
                    f"Expected four children but found {m.child_labels}"
            )

        with self.subTest("Pass through"):
            @as_macro_node("output")
            def PassThrough(macro, x):
                return x

            m = PassThrough()
            print(m.child_labels, m.inputs, m.outputs)

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_storage_for_modified_macros(self):
        ensure_tests_in_python_path()
        Macro.register("static.demo_nodes", domain="demo")

        for backend in Macro.allowed_backends():
            with self.subTest(backend):
                try:
                    macro = Macro.create.demo.AddThree(
                        label="m", x=0, storage_backend=backend
                    )
                    original_result = macro()
                    macro.replace_child(
                        macro.two,
                        Macro.create.demo.AddPlusOne()
                    )


                    modified_result = macro()

                    macro.save()
                    reloaded = Macro.create.demo.AddThree(
                        label="m", storage_backend=backend
                    )
                    self.assertDictEqual(
                        modified_result,
                        reloaded.outputs.to_value_dict(),
                        msg="Updated IO should have been (de)serialized"
                    )
                    self.assertSetEqual(
                        set(macro.children.keys()),
                        set(reloaded.children.keys()),
                        msg="All nodes should have been (de)serialized."
                    )  # Note that this snags the _new_ one in the case of h5io!
                    self.assertEqual(
                        Macro.create.demo.AddThree.__name__,
                        reloaded.__class__.__name__,
                        msg=f"LOOK OUT! This all (de)serialized nicely, but what we "
                            f"loaded is _falsely_ claiming to be an "
                            f"{Macro.create.demo.AddThree.__name__}. This is "
                            f"not any sort of technical error -- what other class name "
                            f"would we load? -- but is a deeper problem with saving "
                            f"modified objects that we need ot figure out some better "
                            f"solution for later."
                    )
                    rerun = reloaded()

                    if backend == "h5io":
                        self.assertDictEqual(
                            modified_result,
                            rerun,
                            msg="Rerunning should re-execute the _modified_ "
                                "functionality"
                        )
                    elif backend == "tinybase":
                        self.assertDictEqual(
                            original_result,
                            rerun,
                            msg="Rerunning should re-execute the _original_ "
                                "functionality"
                        )
                    else:
                        raise ValueError(f"Unexpected backend {backend}?")
                finally:
                    macro.storage.delete()

    def test_wrong_return(self):
        with self.assertRaises(
            TypeError,
            msg="Macro returning object without channel did not raise an error"
        ):
            macro_node(wrong_return_macro)


if __name__ == '__main__':
    unittest.main()
