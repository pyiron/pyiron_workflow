import unittest
from pyiron_workflow.semantics import Semantic, SemanticParent, ParentMost


class SemanticRoot(SemanticParent, ParentMost):
    pass


class TestSemantics(unittest.TestCase):
    def setUp(self):
        self.root = SemanticRoot("root")
        self.child1 = Semantic("child1", parent=self.root)
        self.middle = SemanticParent("middle", parent=self.root)
        self.child2 = Semantic("child2", parent=self.middle)

    def test_label_validity(self):
        with self.assertRaises(TypeError, msg="Label must be a string"):
            Semantic(label=123)

    def test_label_delimiter(self):
        with self.assertRaises(
            ValueError,
            msg=f"Delimiter '{Semantic.semantic_delimiter}' not allowed"
        ):
            Semantic(f"invalid{Semantic.semantic_delimiter}label")

    def test_semantic_delimiter(self):
        self.assertEqual(
            "/",
            Semantic.semantic_delimiter,
            msg="This is just a hard-code to the current value, update it freely so "
                "the test passes; if it fails it's just a reminder that your change is "
                "not backwards compatible, and the next release number should reflect "
                "this."
        )

    def test_parent(self):
        self.assertEqual(self.child1.parent, self.root)
        self.assertEqual(self.root.parent, None)

        with self.assertRaises(
            TypeError,
            msg=f"{ParentMost.__name__} instances can't have parent"
        ):
            self.root.parent = SemanticParent(label="foo")

        with self.assertRaises(
            TypeError,
            msg=f"{ParentMost.__name__} instances can't be children"
        ):
            some_parent = SemanticParent(label="bar")
            some_parent.add_child(self.root)

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