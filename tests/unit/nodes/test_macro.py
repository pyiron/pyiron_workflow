import pickle
import unittest
from concurrent.futures import Future
from time import sleep

from static import demo_nodes

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.nodes.function import as_function_node, function_node
from pyiron_workflow.nodes.macro import Macro, as_macro_node, macro_node
from pyiron_workflow.storage import PickleStorage, available_backends
from pyiron_workflow.topology import CircularDataFlowError

ensure_tests_in_python_path()


def add_one(x):
    result = x + 1
    return result


def add_three_macro(self, one__x):
    self.one = function_node(add_one, x=one__x)
    function_node(add_one, self.one, label="two", parent=self)
    self.add_child(function_node(add_one, self.two, label="three"))
    # Cover a handful of addition methods,
    # although these are more thoroughly tested in Workflow tests
    return self.three


def wrong_return_macro(macro):
    macro.one = function_node(add_one)
    return 3


@as_function_node
def SomeNode(x):
    return x


class TestMacro(unittest.TestCase):
    def test_io_independence(self):
        m = macro_node(add_three_macro, output_labels="three__result")
        self.assertIsNot(
            m.inputs.one__x,
            m.one.inputs.x,
            msg="Expect input to be by value, not by reference",
        )
        self.assertIsNot(
            m.outputs.three__result,
            m.three.outputs.result,
            msg="Expect output to be by value, not by reference",
        )
        self.assertFalse(
            m.connected,
            msg="Macro should talk to its children by value links _not_ graph "
            "connections",
        )

    def test_value_links(self):
        m = macro_node(add_three_macro, output_labels="three__result")
        self.assertIs(
            m.one.inputs.x,
            m.inputs.one__x.value_receiver,
            msg="Sanity check that value link exists",
        )
        self.assertIs(
            m.outputs.three__result,
            m.three.outputs.result.value_receiver,
            msg="Sanity check that value link exists",
        )
        self.assertNotEqual(
            42, m.one.inputs.x.value, msg="Sanity check that we start from expected"
        )
        self.assertNotEqual(
            42,
            m.three.outputs.result.value,
            msg="Sanity check that we start from expected",
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

        def fully_defined(self, one__x):
            add_three_macro(self, one__x=one__x)
            self.one >> self.two >> self.three
            self.starting_nodes = [self.one]
            return self.three

        def only_order(self, one__x):
            add_three_macro(self, one__x=one__x)
            self.two >> self.three
            return self.three

        def only_starting(self, one__x):
            add_three_macro(self, one__x=one__x)
            self.starting_nodes = [self.one]
            return self.three

        m_auto = macro_node(fully_automatic, output_labels="three__result")
        m_user = macro_node(fully_defined, output_labels="three__result")

        x = 0
        expected = add_one(add_one(add_one(x)))
        self.assertEqual(
            m_auto(one__x=x).three__result,
            expected,
            "DAG macros should run fine without user specification of execution.",
        )
        self.assertEqual(
            m_user(one__x=x).three__result,
            expected,
            "Macros should run fine if the user nicely specifies the exeuction graph.",
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
            msg="Label should be automatically generated",
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
            "have not run yet so there shouldn't be any data",
        )

        input_x = 1
        expected_value = add_one(add_one(add_one(input_x)))
        print(m.inputs.labels, m.outputs.labels, m.child_labels)
        out = m(one__x=input_x)  # Take kwargs to set input at runtime

        self.assertEqual(
            out.three__result,
            expected_value,
            msg="Macros should return the output, just like other nodes",
        )
        self.assertEqual(
            m.outputs.three__result.value,
            expected_value,
            msg="Macros should get output updated, just like other nodes",
        )

    def test_creation_from_subclass(self):
        class MyMacro(Macro):
            _output_labels = ("three__result",)

            def graph_creator(self, one__x):
                add_three_macro(self, one__x)
                return self.three

        x = 0
        m = MyMacro(one__x=x)
        m.run()
        self.assertEqual(
            m.outputs.three__result.value,
            add_one(add_one(add_one(x))),
            msg="Subclasses should be able to simply override the graph_creator arg",
        )

    def test_nesting(self):
        def nested_macro(self, a__x):
            self.a = function_node(add_one, a__x)
            self.b = macro_node(
                add_three_macro, one__x=self.a, output_labels="three__result"
            )
            self.c = macro_node(
                add_three_macro,
                one__x=self.b.outputs.three__result,
                output_labels="three__result",
            )
            self.d = function_node(
                add_one,
                x=self.c.outputs.three__result,
            )
            self.a >> self.b >> self.c >> self.d
            self.starting_nodes = [self.a]
            # This definition of the execution graph is not strictly necessary in this
            # simple DAG case; we just do it to make sure nesting definied/automatic
            # selfs works ok
            return self.d

        m = macro_node(nested_macro, output_labels="d__result")
        self.assertEqual(m(a__x=0).d__result, 8)

    def test_with_executor(self):
        macro = macro_node(add_three_macro, output_labels="three__result")
        downstream = function_node(add_one, x=macro.outputs.three__result)
        macro >> downstream  # Manually specify since we'll run the macro but look
        # at the downstream output, and none of this is happening in a workflow

        original_one = macro.one
        macro.executor = macro.create.ProcessPoolExecutor()

        self.assertIs(
            NOT_DATA,
            macro.outputs.three__result.value,
            msg="Sanity check that test is in right starting condition",
        )

        result = macro.run(one__x=0)
        self.assertIsInstance(
            result, Future, msg="Should be running as a parallel process"
        )
        self.assertIs(
            NOT_DATA,
            downstream.outputs.result.value,
            msg="Downstream events should not yet have triggered either, we should wait"
            "for the callback when the result is ready",
        )

        returned_nodes = result.result(timeout=120)  # Wait for the process to finish
        sleep(1)
        self.assertFalse(macro.running, msg="Macro should be done running")
        self.assertIsNot(
            original_one,
            returned_nodes.one,
            msg="Executing in a parallel process should be returning new instances",
        )
        self.assertIs(
            returned_nodes.one,
            macro.one,
            msg="Returned nodes should be taken as children",
        )
        self.assertIs(
            macro,
            macro.one.parent,
            msg="Returned nodes should get the macro as their parent",
            # Once upon a time there was some evidence that this test was failing
            # stochastically, but I just ran the whole test suite 6 times and this test
            # 8 times and it always passed fine, so maybe the issue is resolved...
        )
        self.assertIsNone(
            original_one.parent,
            msg="Original nodes should be orphaned",
            # Note: At time of writing, this is accomplished in Node.__getstate__,
            #       which feels a bit dangerous...
        )
        self.assertEqual(
            0 + 3,
            macro.outputs.three__result.value,
            msg="And of course we expect the calculation to actually run",
        )
        self.assertIs(
            downstream.inputs.x.connections[0],
            macro.outputs.three__result,
            msg="The macro output should still be connected to downstream",
        )
        sleep(0.2)  # Give a moment for the ran signal to emit and downstream to run
        # I'm a bit surprised this sleep is necessary
        self.assertEqual(
            0 + 3 + 1,
            downstream.outputs.result.value,
            msg="The finishing callback should also fire off the ran signal triggering"
            "downstream execution",
        )

        macro.executor_shutdown()

    def test_pulling_from_inside_a_macro(self):
        upstream = function_node(add_one, x=2)
        macro = macro_node(
            add_three_macro, one__x=upstream, output_labels="three__result"
        )
        macro.inputs.one__x = 0  # Set value
        # Now macro.one.inputs.x has both value and a connection

        self.assertEqual(
            0 + 1 + 1,
            macro.two.pull(run_parent_trees_too=False),
            msg="Without running parent trees, the pulling should only run upstream "
            "nodes _inside_ the scope of the macro, relying on the explicit input"
            "value",
        )

        self.assertEqual(
            (2 + 1) + 1 + 1,
            macro.two.pull(run_parent_trees_too=True),
            msg="Running with parent trees, the pulling should also run the parents "
            "data dependencies first",
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
                CircularDataFlowError, msg="Pull should only work for DAG workflows"
            ):
                m.two.pull()
            self.assertListEqual(
                initial_labels,
                list(m.children.keys()),
                msg="Labels should be restored after failing to pull because of "
                "acyclicity",
            )
            self.assertTrue(
                all(
                    c is ic
                    for (c, ic) in zip(
                        grab_connections(m), initial_connections, strict=False
                    )
                ),
                msg="Connections should be restored after failing to pull because of "
                "cyclic data flow",
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
                msg="Sanity check, without cyclic data flows pulling here should be ok",
            )

            n1.inputs.x = n2

            initial_connections = grab_x_and_run(n1) + grab_x_and_run(n2)
            with self.assertRaises(
                CircularDataFlowError,
                msg="Once the outer scope has circular data flows, pulling should fail",
            ):
                m.three.pull(run_parent_trees_too=True)
            self.assertTrue(
                all(
                    c is ic
                    for (c, ic) in zip(
                        grab_x_and_run(n1) + grab_x_and_run(n2),
                        initial_connections,
                        strict=False,
                    )
                ),
                msg="Connections should be restored after failing to pull because of "
                "cyclic data flow in the outer scope",
            )
            self.assertEqual(
                "n1", n1.label, msg="Labels should get restored in the outer scope"
            )
            self.assertEqual(
                "one",
                m.one.label,
                msg="Labels should not have even gotten perturbed to start with in the"
                "inner scope",
            )

        with self.subTest("When a node breaks upstream"):

            def fail_at_zero(x):
                y = 1 / x
                return y

            n1 = function_node(fail_at_zero, x=0)
            n1.recovery = None  # We expect it to fail and don't want a file
            n2 = function_node(add_one, x=n1, label="n1")
            n_not_used = function_node(add_one)
            n_not_used >> n2  # Just here to make sure it gets restored

            with self.assertRaises(
                ZeroDivisionError, msg="The underlying error should get raised"
            ):
                n2.pull()
            self.assertEqual(
                "n1",
                n2.label,
                msg="Original labels should get restored on upstream failure",
            )
            self.assertIs(
                n_not_used,
                n2.signals.input.run.connections[0].owner,
                msg="Original connections should get restored on upstream failure",
            )

    def test_efficient_signature_interface(self):
        with self.subTest("Forked input"):

            @as_macro_node("output")
            def MutlipleUseInput(self, x):
                self.n1 = self.create.standard.UserInput(x)
                self.n2 = self.create.standard.UserInput(x)
                return self.n1

            m = MutlipleUseInput()
            self.assertEqual(
                2 + 1,
                len(m),
                msg="Signature input that is forked to multiple children should result "
                "in the automatic creation of a new node to manage the forking.",
            )

        with self.subTest("Single destination input"):

            @as_macro_node("output")
            def SingleUseInput(self, x):
                self.n = self.create.standard.UserInput(x)
                return self.n

            m = SingleUseInput()
            self.assertEqual(
                1,
                len(m),
                msg=f"Signature input with only one destination should not create an "
                f"interface node. Found nodes {m.child_labels}",
            )

        with self.subTest("Mixed input"):

            @as_macro_node("output")
            def MixedUseInput(self, x, y):
                self.n1 = self.create.standard.UserInput(x)
                self.n2 = self.create.standard.UserInput(y)
                self.n3 = self.create.standard.UserInput(y)
                return self.n1

            m = MixedUseInput()
            self.assertEqual(
                3 + 1,
                len(m),
                msg=f"Mixing forked and single-use input should not cause problems. "
                f"Expected four children but found {m.child_labels}",
            )

        with self.subTest("Pass through"):

            @as_macro_node("output")
            def PassThrough(self, x):
                return x

            m = PassThrough()
            print(m.child_labels, m.inputs, m.outputs)

    def test_storage_for_modified_macros(self):
        for backend in available_backends():
            with self.subTest(backend):
                try:
                    macro = demo_nodes.AddThree(label="m", x=0)
                    macro.replace_child(macro.two, demo_nodes.AddPlusOne())

                    modified_result = macro()

                    if isinstance(backend, PickleStorage):
                        macro.save(backend)
                        reloaded = demo_nodes.AddThree(label="m", autoload=backend)
                        self.assertDictEqual(
                            modified_result,
                            reloaded.outputs.to_value_dict(),
                            msg="Updated IO should have been (de)serialized",
                        )
                        self.assertSetEqual(
                            set(macro.children.keys()),
                            set(reloaded.children.keys()),
                            msg="All nodes should have been (de)serialized.",
                        )
                        self.assertEqual(
                            demo_nodes.AddThree.__name__,
                            reloaded.__class__.__name__,
                            msg=f"LOOK OUT! This all (de)serialized nicely, but what we "
                            f"loaded is _falsely_ claiming to be an "
                            f"{demo_nodes.AddThree.__name__}. This is "
                            f"not any sort of technical error -- what other class name "
                            f"would we load? -- but is a deeper problem with saving "
                            f"modified objects that we need ot figure out some better "
                            f"solution for later.",
                        )
                        rerun = reloaded()

                        self.assertIsInstance(
                            reloaded.two,
                            demo_nodes.AddPlusOne,
                            msg="pickle instantiates the macro node class, but "
                            "but then uses its serialized state, so we retain "
                            "the replaced node.",
                        )
                        self.assertDictEqual(
                            modified_result,
                            rerun,
                            msg="Rerunning re-executes the _replaced_ functionality",
                        )
                    else:
                        raise ValueError(
                            f"Backend {backend} not recognized -- write a test for it"
                        )
                finally:
                    macro.delete_storage(backend)

    def test_output_label_stripping(self):
        """Test extensions to the `ScrapesIO` mixin."""

        @as_macro_node
        def OutputScrapedFromFilteredReturn(macro):
            macro.foo = macro.create.standard.UserInput()
            return macro.foo

        self.assertListEqual(
            ["foo"],
            list(OutputScrapedFromFilteredReturn.preview_outputs().keys()),
            msg="The first, self-like argument, should get stripped from output labels",
        )

        with self.assertRaises(
            ValueError,
            msg="Return values with extra dots are not permissible as scraped labels",
        ):

            @as_macro_node
            def ReturnHasDot(macro):
                macro.foo = macro.create.standard.UserInput()
                return macro.foo.outputs.user_input

    def test_pickle(self):
        m = macro_node(add_three_macro)
        m(1)
        reloaded_m = pickle.loads(pickle.dumps(m))
        self.assertTupleEqual(
            m.child_labels,
            reloaded_m.child_labels,
            msg="Spot check values are getting reloaded correctly",
        )
        self.assertDictEqual(
            m.outputs.to_value_dict(),
            reloaded_m.outputs.to_value_dict(),
            msg="Spot check values are getting reloaded correctly",
        )
        self.assertTrue(
            reloaded_m.two.connected,
            msg="The macro should reload with all its child connections",
        )

        self.assertTrue(m.two.connected, msg="Sanity check")
        reloaded_two = pickle.loads(pickle.dumps(m.two))
        self.assertFalse(
            reloaded_two.connected,
            msg="Children are expected to be de-parenting on serialization, so that if "
            "we ship them off to another process, they don't drag their whole "
            "graph with them",
        )
        self.assertEqual(
            m.two.outputs.to_value_dict(),
            reloaded_two.outputs.to_value_dict(),
            msg="The remainder of the child node state should be recovering just "
            "fine on (de)serialization, this is a spot-check",
        )

    def test_autoload(self):
        existing_node = SomeNode()
        existing_node(42)
        # Name clashes with a macro-node name
        existing_node.save("pickle")

        try:

            @as_macro_node
            def AutoloadsChildren(self, x):
                self.some_child = SomeNode(x, autoload="pickle")
                return self.some_child

            self.assertEqual(
                AutoloadsChildren().some_child.outputs.x.value,
                existing_node.outputs.x.value,
                msg="Autoloading macro children can result in a child node coming with "
                "pre-loaded data if the child's label at instantiation results in a "
                "match with some already-saved node (if the load is compatible). This "
                "is almost certainly undesirable",
            )

            @as_macro_node
            def AutofailsChildren(self, x):
                self.some_child = function_node(
                    add_one, x, label=SomeNode.__name__, autoload="pickle"
                )
                return self.some_child

            with self.assertRaises(
                TypeError,
                msg="When the macro auto-loads a child but the loaded type is not "
                "compatible with the child type, we will even get an error at macro "
                "instantiation time! Autoloading macro children is really not wise.",
            ):
                AutofailsChildren()

            @as_macro_node
            def DoesntAutoloadChildren(self, x):
                self.some_child = SomeNode(x)
                return self.some_child

            self.assertIs(
                DoesntAutoloadChildren().some_child.outputs.x.value,
                NOT_DATA,
                msg="Despite having the same label as a saved node at instantiation time, "
                "without autoloading children, our macro safely gets a fresh instance. "
                "Since this is clearly preferable, here we leave autoload to take its "
                "default value (which for macros should thus not autoload.)",
            )
        finally:
            existing_node.delete_storage("pickle")


if __name__ == "__main__":
    unittest.main()
