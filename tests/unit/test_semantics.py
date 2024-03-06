import unittest
from pyiron_workflow.semantics import Semantics, HasSemantics, Parentmost


class TestSemantics(unittest.TestCase):
    def setUp(self):
        self.root = HasSemantics("root")
        self.child1 = HasSemantics("child1", semantic_parent=self.root)
        self.child2 = HasSemantics("child2", semantic_parent=self.child1)
        self.parentmost = Parentmost("top")

    def test_label_validity(self):
        with self.assertRaises(TypeError, msg="Label must be a string"):
            Semantics(self.root, 123)

    def test_label_delimiter(self):
        with self.assertRaises(ValueError, msg="Delimiter '/' not allowed"):
            Semantics(self.root, "invalid/label")

    def test_parent(self):
        self.assertEqual(self.child1.semantics.parent, self.root)
        self.assertEqual(self.root.semantics.parent, None)

        with self.assertRaises(TypeError, msg="Parentmost can't have parent"):
            self.parentmost.semantics.parent = self.root

    def test_path(self):
        self.assertEqual(self.root.semantics.path, "/root")
        self.assertEqual(self.child1.semantics.path, "/root/child1")
        self.assertEqual(self.child2.semantics.path, "/root/child1/child2")

    def test_root(self):
        self.assertEqual(self.root.semantics.root, self.root)
        self.assertEqual(self.child1.semantics.root, self.root)
        self.assertEqual(self.child2.semantics.root, self.root)


if __name__ == '__main__':
    unittest.main()