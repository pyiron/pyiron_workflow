import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.node_package import NodePackage, NotANodePackage


class TestNodePackage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_tests_in_python_path()
        cls.valid_identifier = "static.demo_nodes"

    def test_init(self):
        ok = NodePackage(self.valid_identifier)
        self.assertEqual(self.valid_identifier, ok.package_identifier)

        with self.assertRaises(NotANodePackage):
            NodePackage("not_even_a_module")

        with self.assertRaises(NotANodePackage):
            NodePackage("static.faulty_node_package")

        with self.assertRaises(NotANodePackage):
            NodePackage("static.not_a_node_packageZ")

    def test_nodes(self):
        package = NodePackage("static.demo_nodes")

        with self.subTest("Attribute access"):
            node = package.OptionallyAdd()
            self.assertIsInstance(node, package.OptionallyAdd)

        with self.subTest("Identifier information"):
            node = package.OptionallyAdd()
            self.assertEqual(node.package_identifier, package.package_identifier)


if __name__ == '__main__':
    unittest.main()
