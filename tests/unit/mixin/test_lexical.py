from __future__ import annotations

import unittest
from pathlib import Path

from pyiron_workflow.mixin.lexical import (
    CyclicPathError,
    Lexical,
    LexicalParent,
)


class ConcreteLexical(Lexical["ConcreteParent"]): ...


class ConcreteParent(LexicalParent[ConcreteLexical]):
    _label = "concrete_parent_default_label"

    @classmethod
    def child_type(cls) -> type[ConcreteLexical]:
        return ConcreteLexical


class ConcreteLexicalParent(ConcreteParent, ConcreteLexical):
    pass


class TestLexical(unittest.TestCase):
    def setUp(self):
        self.root = ConcreteLexicalParent(label="root")
        self.child1 = ConcreteLexical(label="child1", parent=self.root)
        self.middle1 = ConcreteLexicalParent(label="middle", parent=self.root)
        self.middle2 = ConcreteLexicalParent(label="middle_sub", parent=self.middle1)
        self.child2 = ConcreteLexical(label="child2", parent=self.middle2)

    def test_getattr(self):
        with self.assertRaises(AttributeError) as context:
            _ = self.middle1.Middle_sub
        self.assertIn(
            "Did you mean 'middle_sub' and not 'Middle_sub'",
            str(context.exception),
            msg="middle_sub must be suggested as it is close to Middle_sub",
        )
        with self.assertRaises(AttributeError) as context:
            _ = self.middle1.my_neighbor_stinks
        self.assertNotIn(
            "Did you mean",
            str(context.exception),
            msg="Nothings should be suggested for my_neighbor_stinks",
        )

    def test_label_validity(self):
        with self.assertRaises(TypeError, msg="Label must be a string"):
            ConcreteLexical(label=123)

    def test_label_delimiter(self):
        with self.assertRaises(
            ValueError,
            msg=f"Delimiter '{ConcreteLexical.lexical_delimiter}' not allowed",
        ):
            ConcreteLexical(label=f"invalid{ConcreteLexical.lexical_delimiter}label")

        non_lexical_parent = ConcreteParent()
        with self.assertRaises(
            ValueError,
            msg=f"Delimiter '{ConcreteLexical.lexical_delimiter}' not allowed",
        ):
            non_lexical_parent.label = f"contains_{non_lexical_parent.child_type().lexical_delimiter}_delimiter"

    def test_lexical_delimiter(self):
        self.assertEqual(
            "/",
            ConcreteLexical.lexical_delimiter,
            msg="This is just a hard-code to the current value, update it freely so "
            "the test passes; if it fails it's just a reminder that your change is "
            "not backwards compatible, and the next release number should reflect "
            "this.",
        )

    def test_parent(self):
        with self.subTest("Normal usage"):
            self.assertEqual(self.child1.parent, self.root)
            self.assertEqual(self.root.parent, None)

        with self.subTest("Cyclicity exceptions"):
            with self.assertRaises(CyclicPathError):
                self.middle1.parent = self.middle2

            with self.assertRaises(CyclicPathError):
                self.middle2.add_child(self.middle1)

    def test_path(self):
        self.assertEqual(self.root.lexical_path, "/root")
        self.assertEqual(self.child1.lexical_path, "/root/child1")
        self.assertEqual(self.middle1.lexical_path, "/root/middle")
        self.assertEqual(self.middle2.lexical_path, "/root/middle/middle_sub")
        self.assertEqual(self.child2.lexical_path, "/root/middle/middle_sub/child2")

    def test_root(self):
        self.assertEqual(self.root.lexical_root, self.root)
        self.assertEqual(self.child1.lexical_root, self.root)
        self.assertEqual(self.middle1.lexical_root, self.root)
        self.assertEqual(self.middle2.lexical_root, self.root)
        self.assertEqual(self.child2.lexical_root, self.root)

    def test_as_path(self):
        self.assertEqual(
            self.root.as_path(), Path.cwd() / self.root.label, msg="Default None root"
        )
        self.assertEqual(
            self.child1.as_path(root=".."),
            Path("..") / self.root.label / self.child1.label,
            msg="String root",
        )
        self.assertEqual(
            self.middle2.as_path(root=Path("..", "..")),
            (
                Path("..", "..")
                / self.root.label
                / self.middle1.label
                / self.middle2.label
            ),
            msg="Path root",
        )

    def test_detached_parent_path(self):
        orphan = ConcreteLexical(label="orphan")
        orphan.__setstate__(self.child2.__getstate__())
        self.assertIsNone(
            orphan.parent, msg="We still should not explicitly have a parent"
        )
        self.assertListEqual(
            orphan.detached_parent_path.split(orphan.lexical_delimiter),
            self.child2.lexical_path.split(orphan.lexical_delimiter)[:-1],
            msg="Despite not having a parent, the detached path should store lexical "
            "path info through the get/set state routine",
        )
        self.assertEqual(
            orphan.lexical_path,
            self.child2.lexical_path,
            msg="The detached path should carry through to lexical path in the "
            "absence of a parent",
        )
        orphan.label = "orphan"  # Re-set label after getting state
        orphan.parent = self.child2.parent
        self.assertIsNone(
            orphan.detached_parent_path,
            msg="Detached paths aren't necessary and shouldn't co-exist with the "
            "presence of a parent",
        )
        self.assertListEqual(
            orphan.lexical_path.split(orphan.lexical_delimiter)[:-1],
            self.child2.lexical_path.split(self.child2.lexical_delimiter)[:-1],
            msg="Sanity check -- except for the now-different labels, we should be "
            "recovering the usual lexical path on setting a parent.",
        )


if __name__ == "__main__":
    unittest.main()
