import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.interfaces import Creator
from pyiron_workflow.node_package import NodePackage, NotANodePackage


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

        with self.subTest("Test access by item"):
            node = self.creator["static.demo_nodes"].OptionallyAdd(1, 2)
            self.assertEqual(
                3,
                node(),
                msg="Node should get instantiated from creator and be operable"
            )

        with self.subTest("Test access by attribute"):
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
            self.assertIs(
                self.creator.demo,
                self.creator.a_key_other_than_demo,
                msg="Registering the same package to two locations should use the same "
                    "package instance in both places."
            )

            with self.assertRaises(
                ValueError,
                msg="Should not be able to register a new package to an existing domain"
            ):
                self.creator.register("demo", "pyiron_workflow.node_library.standard")

            with self.assertRaises(
                AttributeError,
                msg="Should not be able to register to existing fields"
            ):
                some_field = self.creator.dir()[0]
                self.creator.register(some_field, "static.demo_nodes")

        with self.subTest("Test semantic domain"):
            self.creator.register("some.path", "static.demo_nodes")
            self.assertIsInstance(self.creator.some.path, NodePackage)

            self.creator.register("some.deeper.path", "static.demo_nodes")
            self.assertIsInstance(self.creator.some.deeper.path, NodePackage)

            with self.assertRaises(
                ValueError,
                msg="Can't inject a branch on a package"
            ):
                self.creator.register("some.path.deeper", "static.demo_nodes")

        with self.subTest("Test failure cases"):
            n_initial_packages = len(self.creator._package_access)

            with self.assertRaises(
                NotANodePackage,
                msg="Mustn't allow importing from things that are not node packages"
            ):
                self.creator.register("not_even", "static.not_a_node_package")

            with self.assertRaises(
                NotANodePackage,
                msg="Must require a `nodes` property in the module"
            ):
                self.creator.register("forgetful", "static.forgetful_node_package")

            with self.assertRaises(
                NotANodePackage,
                msg="Must have only node classes in the iterable `nodes` property"
            ):
                self.creator.register("faulty", "static.faulty_node_package")

            self.assertEqual(
                n_initial_packages,
                len(self.creator._package_access),
                msg="Packages should not be getting added if exceptions are raised"
            )


if __name__ == '__main__':
    unittest.main()
