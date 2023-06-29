from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.node_library.package import NodePackage
from pyiron_contrib.workflow.workflow import Workflow


@Workflow.wrap_as.function_node("x")
def dummy(x: int = 0):
    return x


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestNodePackage(TestCase):
    def setUp(self) -> None:
        self.wf = Workflow("test_workflow")
        self.package = NodePackage(self.wf, dummy)

    def test_init(self):
        self.assertTrue(
            hasattr(self.package, dummy.__name__),
            msg="Classes should be added at instantiation"
        )

    def test_access(self):
        node = self.package.Dummy()
        self.assertIsInstance(node, dummy)
        self.assertIs(
            node.parent,
            self.package._parent,
            msg="Package workflow should get assigned to node instances"
        )

    def test_update(self):
        with self.assertRaises(KeyError):
            self.package.Dummy = "This is already a node class name"

        with self.assertRaises(KeyError):
            self.package.update = "This is already a method"

        with self.assertRaises(TypeError):
            self.package.available_name = "But we can still only assign node classes"

        @Workflow.wrap_as.function_node("y")
        def add(x: int = 0):
            return x + 1

        self.package.node_class_and_free_key = add  # Should work!

        with self.assertRaises(KeyError):
            # This is already occupied by another node class
            self.package.Dummy = add

        old_dummy_instance = self.package.Dummy(label="old_dummy_instance")

        @Workflow.wrap_as.function_node("y")
        def dummy(x: int = 0):
            return x + 1

        self.package.update(dummy)

        self.assertEqual(len(self.package), 2, msg="Update should replace, not extend")

        new_dummy_instance = self.package.Dummy(label="new_dummy_instance")

        self.assertEqual(
            old_dummy_instance.outputs.x.value, 0, msg="Should have old functionality"
        )
        self.assertEqual(
            new_dummy_instance.outputs.y.value, 1, msg="Should have new functionality"
        )
