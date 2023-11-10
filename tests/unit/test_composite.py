from sys import version_info
import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NotData
from pyiron_workflow.composite import Composite
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.topology import CircularDataFlowError


def plus_one(x: int = 0) -> int:
    y = x + 1
    return y


class AComposite(Composite):
    def __init__(self, label):
        super().__init__(label=label)

    def _get_linking_channel(self, child_reference_channel, composite_io_key):
        return child_reference_channel  # IO by reference

    @property
    def inputs(self) -> Inputs:
        return self._build_inputs()  # Dynamic IO reflecting current children

    @property
    def outputs(self) -> Outputs:
        return self._build_outputs()  # Dynamic IO reflecting current children


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestComposite(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_tests_in_python_path()
        super().setUpClass()

    def test_node_decorator_access(self):
        @Composite.wrap_as.function_node("y")
        def foo(x: int = 0) -> int:
            return x + 1

        from_class = foo()
        self.assertEqual(from_class.run(), 1, msg="Node should be fully functioning")
        self.assertIsNone(
            from_class.parent,
            msg="Wrapping from the class should give no parent"
        )

        comp = AComposite("my_composite")

        @comp.wrap_as.function_node("y")
        def bar(x: int = 0) -> int:
            return x + 2

        from_instance = bar()
        self.assertEqual(from_instance.run(), 2, msg="Node should be fully functioning")
        self.assertIsNone(
            from_instance.parent,
            msg="Wrappers are not creators, wrapping from the instance makes no "
                "difference"
        )

    def test_creator_access_and_registration(self):
        comp = AComposite("my_composite")
        comp.register("demo", "static.demo_nodes")

        # Test invocation
        comp.create.demo.OptionallyAdd(label="by_add")
        # Test invocation with attribute assignment
        comp.by_assignment = comp.create.demo.OptionallyAdd()
        node = AComposite.create.demo.OptionallyAdd()

        self.assertSetEqual(
            set(comp.nodes.keys()),
            set(["by_add", "by_assignment"]),
            msg=f"Expected one node label generated automatically from the class and "
                f"the other from the attribute assignment, but got {comp.nodes.keys()}"
        )
        self.assertIsNone(
            node.parent,
            msg="Creating from the class directly should not parent the created nodes"
        )

    def test_node_addition(self):
        comp = AComposite("my_composite")

        # Validate the four ways to add a node
        comp.add(Composite.create.Function(plus_one, label="foo"))
        comp.create.Function(plus_one, label="bar")
        comp.baz = comp.create.Function(plus_one, label="whatever_baz_gets_used")
        Composite.create.Function(plus_one, label="qux", parent=comp)
        # node = Composite.create.Function(plus_one, label="quux")
        # node.parent = comp
        self.assertListEqual(
            list(comp.nodes.keys()),
            ["foo", "bar", "baz", "qux",], # "quux"],
            msg="Expected every above syntax to add a node OK"
        )
        comp.boa = comp.qux
        self.assertListEqual(
            list(comp.nodes.keys()),
            ["foo", "bar", "baz", "boa"], # "quux"],
            msg="Reassignment should remove the original instance"
        )
                
    def test_node_access(self):
        node = Composite.create.Function(plus_one)
        comp = AComposite("my_composite")
        comp.child = node
        self.assertIs(
            comp.child,
            node,
            msg="Access should be possible by attribute"
        )
        self.assertIs(
            comp.nodes.child,
            node,
            msg="Access should be possible by attribute on nodes collection"
        )
        self.assertIs(
            comp.nodes["child"],
            node,
            msg="Access should be possible by item on nodes collection"
        )
        
        for n in comp:
            self.assertIs(
                node,
                n,
                msg="Should be able to iterate through (the one and only) nodes"
            )

    def test_node_removal(self):
        comp = AComposite("my_composite")
        comp.owned = Composite.create.Function(plus_one)
        node = Composite.create.Function(plus_one)
        comp.foo = node
        # Add it to starting nodes manually, otherwise it's only there at run time
        comp.starting_nodes = [comp.foo]
        # Connect it inside the composite
        comp.foo.inputs.x = comp.owned.outputs.y

        disconnected = comp.remove(node)
        self.assertIsNone(node.parent, msg="Removal should de-parent")
        self.assertFalse(node.connected, msg="Removal should disconnect")
        self.assertListEqual(
            [(node.inputs.x, comp.owned.outputs.y)],
            disconnected,
            msg="Removal should return destroyed connections"
        )
        self.assertListEqual(
            comp.starting_nodes,
            [],
            msg="Removal should also remove from starting nodes"
        )

        node_owned = comp.owned
        disconnections = comp.remove(node_owned.label)
        self.assertEqual(
            node_owned.parent,
            None,
            msg="Should be able to remove nodes by label as well as by object"
        )
        self.assertListEqual(
            [],
            disconnections,
            msg="node1 should have no connections left"
        )

    def test_label_uniqueness(self):
        comp = AComposite("my_composite")
        comp.foo = Composite.create.Function(plus_one)

        comp.strict_naming = True
        # Validate name preservation for each node addition path
        with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
            comp.add(comp.create.Function(plus_one, label="foo"))

        with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
            comp.create.Function(plus_one, label="foo")

        with self.assertRaises(
            AttributeError,
            msg="The provided label is ok, but then assigning to baz should give "
                "trouble since that name is already occupied"
        ):
            comp.foo = Composite.create.Function(plus_one, label="whatever")

        with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
            Composite.create.Function(plus_one, label="foo", parent=comp)

        # with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
        #     node = Composite.create.Function(plus_one, label="foo")
        #     node.parent = comp

        with self.subTest("Make sure trivial re-assignment has no impact"):
            original_foo = comp.foo
            n_nodes = len(comp.nodes)
            comp.foo = original_foo
            self.assertIs(
                original_foo,
                comp.foo,
                msg="Reassigning a node to the same name should have no impact",
            )
            self.assertEqual(
                n_nodes,
                len(comp.nodes),
                msg="Reassigning a node to the same name should have no impact",
            )

        print("\nKEYS", list(comp.nodes.keys()))
        comp.strict_naming = False
        comp.add(Composite.create.Function(plus_one, label="foo"))
        print("\nKEYS", list(comp.nodes.keys()))
        self.assertEqual(
            2,
            len(comp),
            msg="Without strict naming, we should be able to add to an existing name"
        )
        self.assertListEqual(
            ["foo", "foo0"],
            list(comp.nodes.keys()),
            msg="When adding a node with an existing name and relaxed naming, the new "
                "node should get an index on its label so each label is still unique"
        )

    def test_singular_ownership(self):
        comp1 = AComposite("one")
        comp1.create.Function(plus_one, label="node1")
        node2 = AComposite.create.Function(
            plus_one, label="node2", parent=comp1, x=comp1.node1.outputs.y
        )
        self.assertTrue(node2.connected, msg="Sanity check that node connection works")

        comp2 = AComposite("two")
        with self.assertRaises(ValueError, msg="Can't belong to two parents"):
            comp2.add(node2)
        comp1.remove(node2)
        comp2.add(node2)
        self.assertEqual(
            node2.parent,
            comp2,
            msg="Freed nodes should be able to join other parents"
        )

    def test_replace(self):
        n1 = Composite.create.SingleValue(plus_one)
        n2 = Composite.create.SingleValue(plus_one)
        n3 = Composite.create.SingleValue(plus_one)

        @Composite.wrap_as.function_node(("y", "minus"))
        def x_plus_minus_z(x: int = 0, z=2) -> tuple[int, int]:
            """
            A commensurate but different node: has _more_ than the necessary channels,
            but old channels are all there with the same hints
            """
            return x + z, x - z

        replacement = x_plus_minus_z()

        @Composite.wrap_as.single_value_node("y")
        def different_input_channel(z: int = 0) -> int:
            return z + 10

        @Composite.wrap_as.single_value_node("z")
        def different_output_channel(x: int = 0) -> int:
            return x + 100

        comp = AComposite("my_composite")
        comp.n1 = n1
        comp.n2 = n2
        comp.n3 = n3
        comp.n2.inputs.x = comp.n1
        comp.n3.inputs.x = comp.n2
        comp.inputs_map = {"n1__x": "x"}
        comp.outputs_map = {"n3__y": "y"}
        comp.set_run_signals_to_dag_execution()

        with self.subTest("Verify success cases"):
            self.assertEqual(3, comp.run().y, msg="Sanity check")

            comp.replace(n1, replacement)
            out = comp.run(x=0)
            self.assertEqual(
                (0+2) + 1 + 1, out.y, msg="Should be able to replace by instance"
            )
            self.assertEqual(
                0 - 2, out.n1__minus, msg="Replacement output should also appear"
            )
            comp.replace(replacement, n1)
            self.assertFalse(
                replacement.connected, msg="Replaced nodes should be disconnected"
            )
            self.assertIsNone(
                replacement.parent, msg="Replaced nodes should be orphaned"
            )

            comp.replace("n2", replacement)
            out = comp.run(x=0)
            self.assertEqual(
                (0 + 1) + 2 + 1, out.y, msg="Should be able to replace by label"
            )
            self.assertEqual(1 - 2, out.n2__minus)
            comp.replace(replacement, n2)

            comp.replace(n3, x_plus_minus_z)
            out = comp.run(x=0)
            self.assertEqual(
                (0 + 1) + 2 + 1, out.y, msg="Should be able to replace with a class"
            )
            self.assertEqual(2 - 2, out.n3__minus)
            self.assertIsNot(
                comp.n3,
                replacement,
                msg="Sanity check -- when replacing with class, a _new_ instance "
                    "should be created"
            )
            comp.replace(comp.n3, n3)

            comp.n1 = x_plus_minus_z
            self.assertEqual(
                (0+2) + 1 + 1,
                comp.run(x=0).y,
                msg="Assigning a new _class_ to an existing node should be a shortcut "
                    "for replacement"
            )
            comp.replace(comp.n1, n1)  # Return to original state

            comp.n1 = different_input_channel
            self.assertEqual(
                (0 + 10) + 1 + 1,
                comp.run(n1__z=0).y,
                msg="Different IO should be compatible as long as what's missing is "
                    "not connected"
            )
            comp.replace(comp.n1, n1)

            comp.n3 = different_output_channel
            self.assertEqual(
                (0 + 1) + 1 + 100,
                comp.run(x=0).n3__z,
                msg="Different IO should be compatible as long as what's missing is "
                    "not connected"
            )
            comp.replace(comp.n3, n3)

        with self.subTest("Verify failure cases"):
            self.assertEqual(3, comp.run().y, msg="Sanity check")

            another_comp = AComposite("another")
            another_node = x_plus_minus_z(parent=another_comp)

            with self.assertRaises(
                ValueError,
                msg="Should fail when replacement has a parent"
            ):
                comp.replace(comp.n1, another_node)

            another_comp.remove(another_node)
            another_node.inputs.x = replacement.outputs.y
            with self.assertRaises(
                ValueError,
                msg="Should fail when replacement is connected"
            ):
                comp.replace(comp.n1, another_node)

            another_node.disconnect()
            with self.assertRaises(
                ValueError,
                msg="Should fail if the node being replaced isn't a child"
            ):
                comp.replace(replacement, another_node)

            @Composite.wrap_as.single_value_node("y")
            def wrong_hint(x: float = 0) -> float:
                return x + 1.1

            with self.assertRaises(
                TypeError,
                msg="Should not be able to replace with the wrong type hints"
            ):
                comp.n1 = wrong_hint

            with self.assertRaises(
                AttributeError,
                msg="Should not be able to replace with any missing connected channels"
            ):
                comp.n2 = different_input_channel

            with self.assertRaises(
                AttributeError,
                msg="Should not be able to replace with any missing connected channels"
            ):
                comp.n2 = different_output_channel

            self.assertEqual(
                3,
                comp.run().y,
                msg="Failed replacements should always restore the original state "
                    "cleanly"
            )

    def test_working_directory(self):
        comp = AComposite("my_composite")
        comp.plus_one = Composite.create.Function(plus_one)
        self.assertTrue(
            str(comp.plus_one.working_directory.path).endswith(comp.plus_one.label),
            msg="Child nodes should have their own working directories nested inside"
        )
        comp.working_directory.delete()  # Clean up

    def test_length(self):
        comp = AComposite("my_composite")
        comp.child = Composite.create.Function(plus_one)
        l1 = len(comp)
        comp.child2 = Composite.create.Function(plus_one)
        self.assertEqual(
            l1 + 1,
            len(comp),
            msg="Expected length to count the number of children"
        )

    def test_run(self):
        comp = AComposite("my_composite")
        comp.create.SingleValue(plus_one, label="n1", x=0)
        comp.create.SingleValue(plus_one, label="n2", x=comp.n1)
        comp.create.SingleValue(plus_one, label="n3", x=42)
        comp.n1 > comp.n2
        comp.starting_nodes = [comp.n1]

        comp.run()
        self.assertEqual(
            2,
            comp.n2.outputs.y.value,
            msg="Expected to start from starting node and propagate"
        )
        self.assertIs(
            NotData,
            comp.n3.outputs.y.value,
            msg="n3 was omitted from the execution diagram, it should not have run"
        )

    def test_set_run_signals_to_dag(self):
        # Like the run test, but manually invoking this first

        comp = AComposite("my_composite")
        comp.create.SingleValue(plus_one, label="n1", x=0)
        comp.create.SingleValue(plus_one, label="n2", x=comp.n1)
        comp.create.SingleValue(plus_one, label="n3", x=42)
        comp.set_run_signals_to_dag_execution()
        comp.run()
        self.assertEqual(
            1,
            comp.n1.outputs.y.value,
            msg="Expected all nodes to run"
        )
        self.assertEqual(
            2,
            comp.n2.outputs.y.value,
            msg="Expected all nodes to run"
        )
        self.assertEqual(
            43,
            comp.n3.outputs.y.value,
            msg="Expected all nodes to run"
        )

        comp.n1.inputs.x = comp.n2
        with self.assertRaises(
            CircularDataFlowError,
            msg="Should not be able to automate graphs with circular data"
        ):
            comp.set_run_signals_to_dag_execution()

    def test_return(self):
        comp = AComposite("my_composite")
        comp.n1 = Composite.create.SingleValue(plus_one, x=0)
        not_dottable_string = "can't dot this"
        not_dottable_name_node = comp.create.SingleValue(
            plus_one, x=42, label=not_dottable_string
        )
        comp.starting_nodes = [comp.n1, not_dottable_name_node]
        out = comp.run()
        self.assertEqual(
            1,
            comp.outputs.n1__y.value,
            msg="Sanity check that the output has been filled and is stored under the "
                "name we think it is"
        )
        # Make sure the returned object is functionally a dot-dict
        self.assertEqual(1, out["n1__y"], msg="Should work with item-access")
        self.assertEqual(1, out.n1__y, msg="Should work with dot-access")
        # We can give nodes crazy names, but then we're stuck with item access
        self.assertIs(
            not_dottable_name_node,
            comp.nodes[not_dottable_string],
            msg="Should be able to access the node by item"
        )
        self.assertEqual(
            43,
            out[not_dottable_string + "__y"],
            msg="Should always be able to fall back to item access with crazy labels"
        )

    def test_io_maps(self):
        # input and output, renaming, accessing connected, and deactivating disconnected
        comp = AComposite("my_composite")
        comp.n1 = Composite.create.SingleValue(plus_one, x=0)
        comp.n2 = Composite.create.SingleValue(plus_one, x=comp.n1)
        comp.n3 = Composite.create.SingleValue(plus_one, x=comp.n2)
        comp.m = Composite.create.SingleValue(plus_one, x=42)
        comp.inputs_map = {
            "n1__x": "x",  # Rename
            "n2__x": "intermediate_x",  # Expose
            "m__x": None,  # Hide
        }
        comp.outputs_map = {
            "n3__y": "y",  # Rename
            "n2__y": "intermediate_y",  # Expose,
            "m__y": None,  # Hide
        }
        self.assertIn("x", comp.inputs.labels, msg="Should be renamed")
        self.assertIn("y", comp.outputs.labels, msg="Should be renamed")
        self.assertIn("intermediate_x", comp.inputs.labels, msg="Should be exposed")
        self.assertIn("intermediate_y", comp.outputs.labels, msg="Should be exposed")
        self.assertNotIn("m__x", comp.inputs.labels, msg="Should be hidden")
        self.assertNotIn("m__y", comp.outputs.labels, msg="Should be hidden")
        self.assertNotIn("m__y", comp.outputs.labels, msg="Should be hidden")

        comp.set_run_signals_to_dag_execution()
        out = comp.run()
        self.assertEqual(
            3,
            out.y,
            msg="New names should be propagated to the returned value"
        )
        self.assertNotIn(
            "m__y",
            list(out.keys()),
            msg="IO filtering should be evident in returned value"
        )
        self.assertEqual(
            43,
            comp.m.outputs.y.value,
            msg="The child channel should still exist and have run"
        )
        self.assertEqual(
            1,
            comp.inputs.intermediate_x.value,
            msg="IO should be up-to-date post-run"
        )
        self.assertEqual(
            2,
            comp.outputs.intermediate_y.value,
            msg="IO should be up-to-date post-run"
        )

    def test_de_activate_strict_connections(self):
        comp = AComposite("my_composite")
        comp.sub_comp = AComposite("sub")
        comp.sub_comp.n1 = Composite.create.SingleValue(plus_one, x=0)
        self.assertTrue(
            comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Sanity check that test starts in the expected condition"
        )
        comp.deactivate_strict_hints()
        self.assertFalse(
            comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Deactivating should propagate to children"
        )
        comp.activate_strict_hints()
        self.assertTrue(
            comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Activating should propagate to children"
        )


if __name__ == '__main__':
    unittest.main()
