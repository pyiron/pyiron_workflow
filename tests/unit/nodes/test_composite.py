from concurrent.futures import ProcessPoolExecutor
import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.mixin.injection import OutputsWithInjection
from pyiron_workflow.io import Inputs, ConnectionCopyError
from pyiron_workflow.topology import CircularDataFlowError

ensure_tests_in_python_path()
from static import demo_nodes


def plus_one(x: int = 0) -> int:
    y = x + 1
    return y


class AComposite(Composite):
    def __init__(self, label):
        super().__init__(label=label)

    def _get_linking_channel(self, child_reference_channel, composite_io_key):
        pass  # Shouldn't even be abstract honestly
        # return child_reference_channel  # IO by reference

    @property
    def inputs(self) -> Inputs:
        # Dynamic IO reflecting current children
        inp = Inputs()
        for child in self:
            for channel in child.inputs:
                inp[channel.scoped_label] = channel
        return inp

    @property
    def outputs(self) -> OutputsWithInjection:
        # Dynamic IO reflecting current children
        out = OutputsWithInjection()
        for child in self:
            for channel in child.outputs:
                out[channel.scoped_label] = channel
        return out


class TestComposite(unittest.TestCase):

    def setUp(self) -> None:
        self.comp = AComposite("my_composite")
        super().setUp()

    def test_node_decorator_access(self):
        @Composite.wrap.as_function_node("y")
        def foo(x: int = 0) -> int:
            return x + 1

        from_class = foo()
        self.assertEqual(from_class.run(), 1, msg="Node should be fully functioning")
        self.assertIsNone(
            from_class.parent,
            msg="Wrapping from the class should give no parent"
        )

        comp = self.comp
        @comp.wrap.as_function_node("y")
        def bar(x: int = 0) -> int:
            return x + 2

        from_instance = bar()
        self.assertEqual(from_instance.run(), 2, msg="Node should be fully functioning")
        self.assertIsNone(
            from_instance.parent,
            msg="Wrappers are not creators, wrapping from the instance makes no "
                "difference"
        )

    def test_node_addition(self):
        # Validate the four ways to add a node
        self.comp.add_child(Composite.create.function_node(plus_one, label="foo"))
        self.comp.baz = self.comp.create.function_node(plus_one, label="whatever_baz_gets_used")
        Composite.create.function_node(plus_one, label="qux", parent=self.comp)
        self.assertListEqual(
            list(self.comp.children.keys()),
            ["foo", "baz", "qux"],
            msg="Expected every above syntax to add a node OK"
        )
        print(self.comp.children)
        self.comp.boa = self.comp.qux
        self.assertListEqual(
            list(self.comp.children.keys()),
            ["foo", "baz", "boa"],
            msg="Reassignment should remove the original instance"
        )
                
    def test_node_access(self):
        node = Composite.create.function_node(plus_one)
        self.comp.child = node
        self.assertIs(
            self.comp.child,
            node,
            msg="Access should be possible by attribute"
        )
        self.assertIs(
            self.comp["child"],
            node,
            msg="Access should be possible by item"
        )
        self.assertIs(
            self.comp.children["child"],
            node,
            msg="Access should be possible by item on children collection"
        )
        
        for n in self.comp:
            self.assertIs(
                node,
                n,
                msg="Should be able to iterate through (the one and only) nodes"
            )

        with self.assertRaises(
            AttributeError,
            msg="Composites should override the attribute access portion of their "
                "`HasIOWithInjection` mixin to guarantee that attribute access is "
                "always looking for children. If attribute access is actually desired, "
                " it can be accomplished with a `GetAttr` node."
        ):
            self.comp.not_a_child_or_attribute

    def test_node_removal(self):
        self.comp.owned = Composite.create.function_node(plus_one)
        node = Composite.create.function_node(plus_one)
        self.comp.foo = node
        # Add it to starting nodes manually, otherwise it's only there at run time
        self.comp.starting_nodes = [self.comp.foo]
        # Connect it inside the composite
        self.comp.foo.inputs.x = self.comp.owned.outputs.y

        disconnected = self.comp.remove_child(node)
        self.assertIsNone(node.parent, msg="Removal should de-parent")
        self.assertFalse(node.connected, msg="Removal should disconnect")
        self.assertListEqual(
            [(node.inputs.x, self.comp.owned.outputs.y)],
            disconnected,
            msg="Removal should return destroyed connections"
        )
        self.assertListEqual(
            self.comp.starting_nodes,
            [],
            msg="Removal should also remove from starting nodes"
        )

        node_owned = self.comp.owned
        disconnections = self.comp.remove_child(node_owned.label)
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
        self.comp.foo = Composite.create.function_node(plus_one)

        self.comp.strict_naming = True
        # Validate name preservation for each node addition path
        with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
            self.comp.add_child(self.comp.create.function_node(plus_one, label="foo"))

        with self.assertRaises(
            AttributeError,
            msg="The provided label is ok, but then assigning to baz should give "
                "trouble since that name is already occupied"
        ):
            self.comp.foo = Composite.create.function_node(plus_one, label="whatever")

        with self.assertRaises(AttributeError, msg="We have 'foo' at home"):
            Composite.create.function_node(plus_one, label="foo", parent=self.comp)

        with self.assertRaises(AttributeError, msg="The parent already has 'foo'"):
            node = Composite.create.function_node(plus_one, label="foo")
            node.parent = self.comp

        with self.subTest("Make sure trivial re-assignment has no impact"):
            original_foo = self.comp.foo
            n_nodes = len(self.comp.children)
            self.comp.foo = original_foo
            self.assertIs(
                original_foo,
                self.comp.foo,
                msg="Reassigning a node to the same name should have no impact",
            )
            self.assertEqual(
                n_nodes,
                len(self.comp.children),
                msg="Reassigning a node to the same name should have no impact",
            )

        self.comp.strict_naming = False
        self.comp.add_child(Composite.create.function_node(plus_one, label="foo"))
        self.assertEqual(
            2,
            len(self.comp),
            msg="Without strict naming, we should be able to add to an existing name"
        )
        self.assertListEqual(
            ["foo", "foo0"],
            list(self.comp.children.keys()),
            msg="When adding a node with an existing name and relaxed naming, the new "
                "node should get an index on its label so each label is still unique"
        )

    def test_singular_ownership(self):
        comp1 = AComposite("one")
        comp1.node1 = comp1.create.function_node(plus_one)
        node2 = comp1.create.function_node(
            plus_one, label="node2", parent=comp1, x=comp1.node1.outputs.y
        )
        self.assertTrue(node2.connected, msg="Sanity check that node connection works")

        comp2 = AComposite("two")
        with self.assertRaises(ValueError, msg="Can't belong to two parents"):
            comp2.add_child(node2)
        comp1.remove_child(node2)
        comp2.add_child(node2)
        self.assertEqual(
            node2.parent,
            comp2,
            msg="Freed nodes should be able to join other parents"
        )

    def test_replace(self):
        n1 = Composite.create.function_node(plus_one)
        n2 = Composite.create.function_node(plus_one)
        n3 = Composite.create.function_node(plus_one)

        @Composite.wrap.as_function_node("y", "minus")
        def x_plus_minus_z(x: int = 0, z=2) -> tuple[int, int]:
            """
            A commensurate but different node: has _more_ than the necessary channels,
            but old channels are all there with the same hints
            """
            return x + z, x - z

        replacement = x_plus_minus_z()

        @Composite.wrap.as_function_node("y")
        def different_input_channel(z: int = 0) -> int:
            return z + 10

        @Composite.wrap.as_function_node("z")
        def different_output_channel(x: int = 0) -> int:
            return x + 100

        self.comp.n1 = n1
        self.comp.n2 = n2
        self.comp.n3 = n3
        self.comp.n2.inputs.x = self.comp.n1
        self.comp.n3.inputs.x = self.comp.n2
        self.comp.set_run_signals_to_dag_execution()

        with self.subTest("Verify success cases"):
            self.assertEqual(3, self.comp.run().n3__y, msg="Sanity check")

            self.comp.replace_child(n1, replacement)
            out = self.comp.run(n1__x=0)
            self.assertEqual(
                (0+2) + 1 + 1, out.n3__y, msg="Should be able to replace by instance"
            )
            self.assertEqual(
                0 - 2, out.n1__minus, msg="Replacement output should also appear"
            )
            self.comp.replace_child(replacement, n1)
            self.assertFalse(
                replacement.connected, msg="Replaced nodes should be disconnected"
            )
            self.assertIsNone(
                replacement.parent, msg="Replaced nodes should be orphaned"
            )

            self.comp.replace_child("n2", replacement)
            out = self.comp.run(n1__x=0)
            self.assertEqual(
                (0 + 1) + 2 + 1, out.n3__y, msg="Should be able to replace by label"
            )
            self.assertEqual(1 - 2, out.n2__minus)
            self.comp.replace_child(replacement, n2)

            self.comp.replace_child(n3, x_plus_minus_z)
            out = self.comp.run(n1__x=0)
            self.assertEqual(
                (0 + 1) + 2 + 1, out.n3__y, msg="Should be able to replace with a class"
            )
            self.assertEqual(2 - 2, out.n3__minus)
            self.assertIsNot(
                self.comp.n3,
                replacement,
                msg="Sanity check -- when replacing with class, a _new_ instance "
                    "should be created"
            )
            self.comp.replace_child(self.comp.n3, n3)

            self.comp.n1 = x_plus_minus_z
            self.assertEqual(
                (0+2) + 1 + 1,
                self.comp.run(n1__x=0).n3__y,
                msg="Assigning a new _class_ to an existing node should be a shortcut "
                    "for replacement"
            )
            self.comp.replace_child(self.comp.n1, n1)  # Return to original state

            self.comp.n1 = different_input_channel
            self.assertEqual(
                (0 + 10) + 1 + 1,
                self.comp.run(n1__z=0).n3__y,
                msg="Different IO should be compatible as long as what's missing is "
                    "not connected"
            )
            self.comp.replace_child(self.comp.n1, n1)

            self.comp.n3 = different_output_channel
            self.assertEqual(
                (0 + 1) + 1 + 100,
                self.comp.run(n1__x=0).n3__z,
                msg="Different IO should be compatible as long as what's missing is "
                    "not connected"
            )
            self.comp.replace_child(self.comp.n3, n3)

        with self.subTest("Verify failure cases"):
            self.assertEqual(3, self.comp.run().n3__y, msg="Sanity check")

            another_comp = AComposite("another")
            another_node = x_plus_minus_z(parent=another_comp)

            with self.assertRaises(
                ValueError,
                msg="Should fail when replacement has a parent"
            ):
                self.comp.replace_child(self.comp.n1, another_node)

            another_comp.remove_child(another_node)
            another_node.inputs.x = replacement.outputs.y
            with self.assertRaises(
                ValueError,
                msg="Should fail when replacement is connected"
            ):
                self.comp.replace_child(self.comp.n1, another_node)

            another_node.disconnect()
            with self.assertRaises(
                ValueError,
                msg="Should fail if the node being replaced isn't a child"
            ):
                self.comp.replace_child(replacement, another_node)

            @Composite.wrap.as_function_node("y")
            def wrong_hint(x: float = 0) -> float:
                return x + 1.1

            with self.assertRaises(
                TypeError,
                msg="Should not be able to replace with the wrong type hints"
            ):
                self.comp.n1 = wrong_hint

            with self.assertRaises(
                ConnectionCopyError,
                msg="Should not be able to replace with any missing connected channels"
            ):
                self.comp.n2 = different_input_channel

            with self.assertRaises(
                ConnectionCopyError,
                msg="Should not be able to replace with any missing connected channels"
            ):
                self.comp.n2 = different_output_channel

            self.assertEqual(
                3,
                self.comp.run().n3__y,
                msg="Failed replacements should always restore the original state "
                    "cleanly"
            )

    def test_working_directory(self):
        self.comp.plus_one = Composite.create.function_node(plus_one)
        self.assertTrue(
            str(self.comp.plus_one.working_directory.path).endswith(self.comp.plus_one.label),
            msg="Child nodes should have their own working directories nested inside"
        )
        self.comp.working_directory.delete()  # Clean up

    def test_length(self):
        self.comp.child = Composite.create.function_node(plus_one)
        l1 = len(self.comp)
        self.comp.child2 = Composite.create.function_node(plus_one)
        self.assertEqual(
            l1 + 1,
            len(self.comp),
            msg="Expected length to count the number of children"
        )

    def test_run(self):
        self.comp.n1 = self.comp.create.function_node(plus_one, x=0)
        self.comp.n2 = self.comp.create.function_node(plus_one, x=self.comp.n1)
        self.comp.n3 = self.comp.create.function_node(plus_one, x=42)
        self.comp.n1 >> self.comp.n2
        self.comp.starting_nodes = [self.comp.n1]

        self.comp.run()
        self.assertEqual(
            2,
            self.comp.n2.outputs.y.value,
            msg="Expected to start from starting node and propagate"
        )
        self.assertIs(
            NOT_DATA,
            self.comp.n3.outputs.y.value,
            msg="n3 was omitted from the execution diagram, it should not have run"
        )

    def test_set_run_signals_to_dag(self):
        # Like the run test, but manually invoking this first
        self.comp.n1 = self.comp.create.function_node(plus_one, x=0)
        self.comp.n2 = self.comp.create.function_node(plus_one, x=self.comp.n1)
        self.comp.n3 = self.comp.create.function_node(plus_one, x=42)
        self.comp.set_run_signals_to_dag_execution()
        self.comp.run()
        self.assertEqual(
            1,
            self.comp.n1.outputs.y.value,
            msg="Expected all nodes to run"
        )
        self.assertEqual(
            2,
            self.comp.n2.outputs.y.value,
            msg="Expected all nodes to run"
        )
        self.assertEqual(
            43,
            self.comp.n3.outputs.y.value,
            msg="Expected all nodes to run"
        )

        self.comp.n1.inputs.x = self.comp.n2
        with self.assertRaises(
            CircularDataFlowError,
            msg="Should not be able to automate graphs with circular data"
        ):
            self.comp.set_run_signals_to_dag_execution()

    def test_return(self):
        self.comp.n1 = Composite.create.function_node(plus_one, x=0)
        not_dottable_string = "can't dot this"
        not_dottable_name_node = self.comp.create.function_node(
            plus_one, x=42, label=not_dottable_string, parent=self.comp
        )
        self.comp.starting_nodes = [self.comp.n1, not_dottable_name_node]
        out = self.comp.run()
        self.assertEqual(
            1,
            self.comp.outputs.n1__y.value,
            msg="Sanity check that the output has been filled and is stored under the "
                "name we think it is"
        )
        # Make sure the returned object is functionally a dot-dict
        self.assertEqual(1, out["n1__y"], msg="Should work with item-access")
        self.assertEqual(1, out.n1__y, msg="Should work with dot-access")
        # We can give nodes crazy names, but then we're stuck with item access
        self.assertIs(
            not_dottable_name_node,
            self.comp.children[not_dottable_string],
            msg="Should be able to access the node by item"
        )
        self.assertEqual(
            43,
            out[not_dottable_string + "__y"],
            msg="Should always be able to fall back to item access with crazy labels"
        )

    def test_de_activate_strict_connections(self):
        self.comp.sub_comp = AComposite("sub")
        self.comp.sub_comp.n1 = Composite.create.function_node(plus_one, x=0)
        self.assertTrue(
            self.comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Sanity check that test starts in the expected condition"
        )
        self.comp.deactivate_strict_hints()
        self.assertFalse(
            self.comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Deactivating should propagate to children"
        )
        self.comp.activate_strict_hints()
        self.assertTrue(
            self.comp.sub_comp.n1.inputs.x.strict_hints,
            msg="Activating should propagate to children"
        )

    def test_graph_info(self):
        top = AComposite("topmost")
        top.middle_composite = AComposite("middle_composite")
        top.middle_composite.deep_node = Composite.create.function_node(plus_one)
        top.middle_function = Composite.create.function_node(plus_one)

        with self.subTest("test_graph_path"):
            self.assertEqual(
                top.semantic_delimiter + top.label,
                top.graph_path,
                msg="The parent-most node should be its own path."
            )
            self.assertTrue(
                top.middle_composite.graph_path.startswith(top.graph_path),
                msg="The path should go to the parent-most object."
            )
            self.assertTrue(
                top.middle_function.graph_path.startswith(top.graph_path),
                msg="The path should go to the parent-most object."
            )
            self.assertTrue(
                top.middle_composite.deep_node.graph_path.startswith(top.graph_path),
                msg="The path should go to the parent-most object, recursively from "
                    "all depths."
            )

        with self.subTest("test_graph_root"):
            self.assertIs(
                top,
                top.graph_root,
                msg="The parent-most node should be its own graph_root."
            )
            self.assertIs(
                top,
                top.middle_composite.graph_root,
                msg="The parent-most node should be the graph_root."
            )
            self.assertIs(
                top,
                top.middle_function.graph_root,
                msg="The parent-most node should be the graph_root."
            )
            self.assertIs(
                top,
                top.middle_composite.deep_node.graph_root,
                msg="The parent-most node should be the graph_root, recursively accessible "
                    "from all depths."
            )

    def test_import_ready(self):

        totally_findable = demo_nodes.OptionallyAdd()
        self.assertTrue(
            totally_findable.import_ready,
            msg="The node class is well defined and in an importable module"
        )
        bad_class = demo_nodes.Dynamic()
        self.assertFalse(
            bad_class.import_ready,
            msg="The node is in an importable location, but the imported object is not "
                "the node class (but rather the node function)"
        )
        with self.subTest(msg="Made up module"):
            og_module = totally_findable.__class__.__module__
            try:
                totally_findable.__class__.__module__ = "something I totally made up"
                self.assertFalse(
                    totally_findable.import_ready,
                    msg="The node class is well defined, but the module is not in the "
                        "python path so import fails"
                )
            finally:
                totally_findable.__class__.__module__ = og_module  # Fix what you broke

        self.assertTrue(
            self.comp.import_ready,
            msg="Sanity check on initial condition -- tests are in the path, so this "
                "is importable"
        )
        self.comp.totally_findable = totally_findable
        self.assertTrue(
            self.comp.import_ready,
            msg="Adding importable children should leave the parent import-ready"
        )
        self.comp.bad_class = bad_class
        self.assertFalse(
            self.comp.import_ready,
            msg="Adding un-importable children should make the parent not import ready"
        )

    def test_with_executor(self):
        self.comp.add_child(demo_nodes.AddThree(label="sub_composite", x=0))
        with ProcessPoolExecutor() as exe:
            self.comp.sub_composite.executor = exe
            self.comp.run()
        self.comp.run()
        self.assertIs(
            self.comp.sub_composite.parent,
            self.comp,
            msg="After processing a remotely-executed self, the local self should "
                "retain its parent"
        )
        self.assertIs(
            self.comp.sub_composite.executor,
            exe,
            msg="After processing a remotely-executed self, the local self should "
                "retain its executor"
        )


if __name__ == '__main__':
    unittest.main()
