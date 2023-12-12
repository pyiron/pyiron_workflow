from concurrent.futures import Future
from functools import partialmethod

from time import sleep
import unittest

from pyiron_workflow.channels import NotData
from pyiron_workflow.function import SingleValue
from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.topology import CircularDataFlowError


def add_one(x):
    result = x + 1
    return result


def add_three_macro(macro):
    macro.one = SingleValue(add_one)
    SingleValue(add_one, macro.one, label="two", parent=macro)
    macro.add_node(SingleValue(add_one, macro.two, label="three"))
    # Cover a handful of addition methods,
    # although these are more thoroughly tested in Workflow tests


class TestMacro(unittest.TestCase):

    def test_static_input(self):
        m = Macro(add_three_macro)
        inp = m.inputs
        inp_again = m.inputs
        self.assertIs(
            inp, inp_again, msg="Should not be rebuilding just to look at it"
        )
        m._rebuild_data_io()
        new_inp = m.inputs
        self.assertIsNot(
            inp, new_inp, msg="After rebuild we should get a new object"
        )

    def test_io_independence(self):
        m = Macro(add_three_macro)
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
        m = Macro(add_three_macro)
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

        def fully_defined(macro):
            add_three_macro(macro)
            macro.one >> macro.two >> macro.three
            macro.starting_nodes = [macro.one]

        def only_order(macro):
            add_three_macro(macro)
            macro.two >> macro.three

        def only_starting(macro):
            add_three_macro(macro)
            macro.starting_nodes = [macro.one]

        m_auto = Macro(fully_automatic)
        m_user = Macro(fully_defined)

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
                Macro(only_order)

            with self.assertRaises(ValueError):
                Macro(only_starting)

    def test_default_label(self):
        m = Macro(add_three_macro)
        self.assertEqual(
            m.label,
            add_three_macro.__name__,
            msg="Label should be automatically generated"
        )
        label = "custom_name"
        m2 = Macro(add_three_macro, label=label)
        self.assertEqual(m2.label, label, msg="Should be able to specify a label")

    def test_creation_from_decorator(self):
        m = Macro(add_three_macro)

        self.assertIs(
            m.outputs.three__result.value,
            NotData,
            msg="Output should be accessible with the usual naming convention, but we "
                "have not run yet so there shouldn't be any data"
        )

        input_x = 1
        expected_value = add_one(add_one(add_one(input_x)))
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
            def build_graph(self):
                add_three_macro(self)

            __init__ = partialmethod(
                Macro.__init__,
                build_graph,
            )

        x = 0
        m = MyMacro(one__x=x)
        m.run()
        self.assertEqual(
            m.outputs.three__result.value,
            add_one(add_one(add_one(x))),
            msg="Subclasses should be able to simply override the graph_creator arg"
        )

    def test_nesting(self):
        def nested_macro(macro):
            macro.a = SingleValue(add_one)
            macro.b = Macro(
                add_three_macro,
                one__x=macro.a,
                outputs_map={"two__result": "intermediate_result"}
            )
            macro.c = Macro(
                add_three_macro,
                one__x=macro.b.outputs.three__result,
                outputs_map={"two__result": "intermediate_result"}
            )
            macro.d = SingleValue(
                add_one,
                x=macro.c.outputs.three__result,
            )
            macro.a >> macro.b >> macro.c >> macro.d
            macro.starting_nodes = [macro.a]
            # This definition of the execution graph is not strictly necessary in this
            # simple DAG case; we just do it to make sure nesting definied/automatic
            # macros works ok
            macro.outputs_map = {"b__intermediate_result": "deep_output"}

        m = Macro(nested_macro)
        self.assertEqual(m(a__x=0).d__result, 8)

    def test_with_executor(self):
        macro = Macro(add_three_macro)
        downstream = SingleValue(add_one, x=macro.outputs.three__result)
        macro >> downstream  # Manually specify since we'll run the macro but look
        # at the downstream output, and none of this is happening in a workflow

        original_one = macro.one
        macro.executor = macro.create.Executor()

        self.assertIs(
            NotData,
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
            NotData,
            downstream.outputs.result.value,
            msg="Downstream events should not yet have triggered either, we should wait"
                "for the callback when the result is ready"
        )

        returned_nodes = result.result(timeout=120)  # Wait for the process to finish
        sleep(1)
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
            macro.nodes.one.parent,
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
            msg="The macro should still be connected to "
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
        upstream = SingleValue(add_one, x=2)
        macro = Macro(add_three_macro, one__x=upstream)
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
                macro.one = SingleValue(add_one)
                macro.two = SingleValue(add_one, x=macro.one)
                macro.one.inputs.x = macro.two
                macro.one >> macro.two
                macro.starting_nodes = [macro.one]
                # We need to manually specify execution since the data flow is cyclic

            m = Macro(cyclic_macro)

            initial_labels = list(m.nodes.keys())

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
                list(m.nodes.keys()),
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
            n1 = SingleValue(add_one, label="n1", x=0)
            n2 = SingleValue(add_one, label="n2", x=n1)
            m = Macro(add_three_macro, label="m", one__x=n2)

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

            n1 = SingleValue(fail_at_zero, x=0)
            n2 = SingleValue(add_one, x=n1, label="n1")
            n_not_used = SingleValue(add_one)
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
                n2.signals.input.run.connections[0].node,
                msg="Original connections should get restored on upstream failure"
            )

    def test_output_labels_vs_return_values(self):
        def no_return(macro):
            macro.foo = macro.create.standard.UserInput()

        Macro(no_return)  # Neither is fine

        with self.assertRaises(
            TypeError,
            msg="Output labels and return values must match"
        ):
            Macro(no_return, output_labels="not_None")

        @macro_node("some_return")
        def HasReturn(macro):
            macro.foo = macro.create.standard.UserInput()
            return macro.foo

        HasReturn()  # Both is fine

        with self.assertRaises(
            TypeError,
            msg="Output labels and return values must match"
        ):
            HasReturn(output_labels=None)  # Override those gotten by the decorator

        with self.assertRaises(
            ValueError,
            msg="Output labels and return values must have commensurate length"
        ):
            HasReturn(output_labels=["one_label", "too_many"])

    def test_maps_vs_functionlike_definitions(self):
        """
        Check that the full-detail IO maps and the white-listing like-a-function
        approach are equivalent
        """
        @macro_node()
        def WithIOMaps(macro):
            macro.forked = macro.create.standard.UserInput()
            macro.forked.inputs.user_input.type_hint = int
            macro.list_in = macro.create.standard.UserInput()
            macro.list_in.inputs.user_input.type_hint = list
            macro.n_plus_2 = macro.forked + 2
            macro.sliced_list = macro.list_in[macro.forked:macro.n_plus_2]
            macro.double_fork = 2 * macro.forked
            macro.inputs_map = {
                macro.forked.inputs.user_input.scoped_label: "n",
                "list_in__user_input": "lin",
                "n_plus_2__other": None,
                "list_in__user_input_Slice_forked__user_input_n_plus_2__add_None__step": None,
                macro.double_fork.inputs.other.scoped_label: None,
            }
            macro.outputs_map = {
                macro.sliced_list.outputs.getitem.scoped_label: "lout",
                macro.n_plus_2.outputs.add.scoped_label: "n_plus_2",
                "double_fork__rmul": None
            }

        @macro_node("lout", "n_plus_2")
        def LikeAFunction(macro, n: int, lin: list):
            macro.plus_two = n + 2
            macro.sliced_list = lin[n:macro.plus_two]
            # Test returning both a single value node and an output channel,
            # even though here we could just use the node both times
            return macro.sliced_list, macro.plus_two.channel

        n = 2
        lin = [1, 2, 3, 4, 5, 6]
        expected_input_labels = ["n", "lin"]
        expected_result = {"n_plus_2": 4, "lout": [3, 4]}

        for MacroClass in [WithIOMaps, LikeAFunction]:
            with self.subTest(f"{MacroClass.__name__}"):
                macro = MacroClass(n=n, lin=lin)
                self.assertListEqual(macro.inputs.labels, expected_input_labels)
                self.assertDictEqual(macro(), expected_result)


if __name__ == '__main__':
    unittest.main()
