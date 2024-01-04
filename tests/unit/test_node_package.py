import unittest

from pyiron_workflow.node_package import NodePackage
from pyiron_workflow.function import function_node


@function_node()
def Dummy(x: int = 0):
    return x


class TestNodePackage(unittest.TestCase):
    def setUp(self) -> None:
        self.package = NodePackage("test_node_package", Dummy)

    def test_init(self):
        self.assertTrue(
            hasattr(self.package, Dummy.__name__),
            msg="Classes should be added at instantiation"
        )

    def test_access(self):
        node = self.package.Dummy()
        self.assertIsInstance(node, Dummy)

    def test_update(self):
        with self.assertRaises(KeyError):
            self.package.Dummy = "This is already a node class name"

        with self.assertRaises(KeyError):
            self.package.update = "This is already a method"

        with self.assertRaises(TypeError):
            self.package.available_name = "But we can still only assign node classes"

        @function_node("y")
        def Add(x: int = 0):
            return x + 1

        self.package.node_class_and_free_key = Add  # Should work!

        with self.assertRaises(KeyError):
            # This is already occupied by another node class
            self.package.Dummy = Add

        old_dummy_instance = self.package.Dummy(label="old_dummy_instance")

        @function_node()
        def Dummy(x: int = 0):
            y = x + 1
            return y

        self.package.update(Dummy)

        self.assertEqual(len(self.package), 2, msg="Update should replace, not extend")

        new_dummy_instance = self.package.Dummy(label="new_dummy_instance")

        old_dummy_instance.run()
        new_dummy_instance.run()
        self.assertEqual(
            old_dummy_instance.outputs.x.value, 0, msg="Should have old functionality"
        )
        self.assertEqual(
            new_dummy_instance.outputs.y.value, 1, msg="Should have new functionality"
        )


if __name__ == '__main__':
    unittest.main()
