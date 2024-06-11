import unittest
from pyiron_workflow.mixin.semantics import (
    Semantic, SemanticParent, ParentMost, CyclicPathError
)


class TestSemantics(unittest.TestCase):
    def setUp(self):
        self.root = ParentMost("root")
        self.child1 = Semantic("child1", parent=self.root)
        self.middle1 = SemanticParent("middle", parent=self.root)
        self.middle2 = SemanticParent("middle_sub", parent=self.middle1)
        self.child2 = Semantic("child2", parent=self.middle2)

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
        with self.subTest("Normal usage"):
            self.assertEqual(self.child1.parent, self.root)
            self.assertEqual(self.root.parent, None)

        with self.subTest(f"{ParentMost.__name__} exceptions"):
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

        with self.subTest("Cyclicity exceptions"):
            with self.assertRaises(CyclicPathError):
                self.middle1.parent = self.middle2

            with self.assertRaises(CyclicPathError):
                self.middle2.add_child(self.middle1)

    def test_path(self):
        self.assertEqual(self.root.semantic_path, "/root")
        self.assertEqual(self.child1.semantic_path, "/root/child1")
        self.assertEqual(self.middle1.semantic_path, "/root/middle")
        self.assertEqual(self.middle2.semantic_path, "/root/middle/middle_sub")
        self.assertEqual(self.child2.semantic_path, "/root/middle/middle_sub/child2")

    def test_root(self):
        self.assertEqual(self.root.semantic_root, self.root)
        self.assertEqual(self.child1.semantic_root, self.root)
        self.assertEqual(self.middle1.semantic_root, self.root)
        self.assertEqual(self.middle2.semantic_root, self.root)
        self.assertEqual(self.child2.semantic_root, self.root)


if __name__ == '__main__':
    unittest.main()
