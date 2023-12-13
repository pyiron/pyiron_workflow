import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.interfaces import Creator
from pyiron_workflow.node_package import NodePackage


class TestCreator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.creator = Creator()
        ensure_tests_in_python_path()
        super().setUpClass()

    def test_registration(self):

        with self.assertRaises(
            AttributeError,
            msg="Sanity check that the package isn't there yet and the test setup is "
                "what we want"
        ):
            self.creator.demo_nodes

        self.creator.register("demo", "static.demo_nodes")

        node = self.creator.demo.OptionallyAdd(1, 2)
        self.assertEqual(
            3,
            node(),
            msg="Node should get instantiated from creator and be operable"
        )

        self.creator.register("sub", "static.nodes_subpackage")
        self.assertIsInstance(self.creator.sub.demo_nodes, NodePackage)
        self.assertIsInstance(self.creator.sub.subsub_package.demo_nodes, NodePackage)

        with self.subTest("Test re-registration"):
            self.creator.register("demo", "static.demo_nodes")
            # Same thing to the same location should be fine

            self.creator.register("a_key_other_than_demo", "static.demo_nodes")
            # The same thing to another key is usually dumb, but totally permissible

            with self.assertRaises(
                KeyError,
                msg="Should not be able to register a new package to an existing domain"
            ):
                self.creator.register("demo", "pyiron_workflow.node_library.standard")

            with self.assertRaises(
                AttributeError,
                msg="Should not be able to register to existing fields"
            ):
                some_field = self.creator.dir()[0]
                self.creator.register(some_field, "static.demo_nodes")

        with self.subTest("Test failure cases"):
            n_initial_packages = len(self.creator._node_packages)

            with self.assertRaises(
                ValueError,
                msg="Mustn't allow importing from things that are not node packages"
            ):
                self.creator.register("not_even", "static.not_a_node_package")

            with self.assertRaises(
                ValueError,
                msg="Must require a `nodes` property in the module"
            ):
                self.creator.register("forgetful", "static.forgetful_node_package")

            with self.assertRaises(
                TypeError,
                msg="Must have only node classes in the iterable `nodes` property"
            ):
                self.creator.register("faulty", "static.faulty_node_package")

            self.assertEqual(
                n_initial_packages,
                len(self.creator._node_packages),
                msg="Packages should not be getting added if exceptions are raised"
            )


if __name__ == '__main__':
    unittest.main()
