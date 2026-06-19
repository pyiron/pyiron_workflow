from __future__ import annotations

import types
import unittest
from dataclasses import dataclass, field

import flowrep as fr

from pyiron_workflow._wfms import lexical

# --------------------------------------------------------------------------- #
# Tiny structural stubs (satisfy `HasLexicalPath` / `Lexical` protocols).     #
# --------------------------------------------------------------------------- #


@dataclass
class _StubOwner:
    lexical_path: str = "root"


@dataclass
class _StubItem:
    label: str
    owner: _StubOwner | None = None
    lexical_path: str = field(init=False)

    def __post_init__(self) -> None:
        base = "<unowned>" if self.owner is None else self.owner.lexical_path
        self.lexical_path = f"{base}.{self.label}"


class _LexicalMapSubclass(lexical.LexicalMap[_StubItem, _StubOwner]):
    """Used to verify helpers preserve the concrete class type."""


class TestLexicalMap(unittest.TestCase):
    def setUp(self) -> None:
        self.owner = _StubOwner(lexical_path="owner")
        self.other = _StubOwner(lexical_path="other")
        self.foo = _StubItem(label="foo", owner=self.owner)
        self.bar = _StubItem(label="bar", owner=self.owner)
        self.map: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(
            self.owner, {"foo": self.foo, "bar": self.bar}
        )

    # ---- __slots__ contract ----------------------------------------------- #

    def test_slots_is_the_contract(self) -> None:
        self.assertEqual(
            lexical.LexicalMap.__slots__,
            ("_pwf_lexical_map__data", "_pwf_lexical_map__owner"),
        )

    # ---- __init__ --------------------------------------------------------- #

    def test_init_no_data(self) -> None:
        m: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(self.owner)
        self.assertEqual(len(m), 0)

    def test_init_empty_dict(self) -> None:
        m = lexical.LexicalMap(self.owner, {})
        self.assertEqual(len(m), 0)

    def test_init_with_data(self) -> None:
        m = lexical.LexicalMap(self.owner, {"foo": self.foo, "bar": self.bar})
        self.assertEqual(set(m), {"foo", "bar"})
        self.assertIs(m["foo"], self.foo)
        self.assertIs(m["bar"], self.bar)

    def test_init_keys_independent_of_labels(self) -> None:
        # The dict path trusts caller-chosen keys; they need not match
        # `value.label`.
        m = lexical.LexicalMap(
            self.owner, {"alias_foo": self.foo, "alias_bar": self.bar}
        )
        self.assertEqual(set(m), {"alias_foo", "alias_bar"})
        self.assertIs(m["alias_foo"], self.foo)
        self.assertIs(m["alias_bar"], self.bar)

    def test_init_copies_input_dict(self) -> None:
        src = {"foo": self.foo}
        m = lexical.LexicalMap(self.owner, src)
        src["bar"] = self.bar  # mutate source post-construction
        self.assertNotIn("bar", m)

    def test_init_accepts_non_dict_mapping(self) -> None:
        # Any Mapping is acceptable; it's coerced to dict internally.

        proxy = types.MappingProxyType({"foo": self.foo})
        m = lexical.LexicalMap(self.owner, proxy)
        self.assertIs(m["foo"], self.foo)

    def test_init_mismatched_owner_raises(self) -> None:
        bad = _StubItem(label="bad", owner=self.other)
        with self.assertRaisesRegex(
            ValueError,
            "Map owned by 'owner' cannot be initialized with items that have a different owner",
        ) as ctx:
            lexical.LexicalMap(self.owner, {"foo": self.foo, "bad": bad})
        msg = str(ctx.exception)
        self.assertIn(bad.lexical_path, msg)
        self.assertIn(self.other.lexical_path, msg)
        self.assertIn(self.owner.lexical_path, msg)

    def test_init_none_owner_passes(self) -> None:
        orphan = _StubItem(label="orphan", owner=None)
        map_ = lexical.LexicalMap(self.owner, {"orphan": orphan})
        self.assertIn(orphan.label, map_)

    # ---- __getitem__ ------------------------------------------------------ #

    def test_getitem_normal_label(self) -> None:
        self.assertIs(self.map["foo"], self.foo)
        self.assertIs(self.map["bar"], self.bar)

    def test_getitem_missing_label_raises(self) -> None:
        with self.assertRaises(KeyError):
            _ = self.map["does_not_exist"]

    def test_getitem_reserved_data_name_raises(self) -> None:
        with self.assertRaises(KeyError) as ctx:
            _ = self.map["_pwf_lexical_map__data"]
        self.assertIn("reserved", str(ctx.exception))
        self.assertIn("_pwf_lexical_map__data", str(ctx.exception))

    def test_getitem_reserved_owner_name_raises(self) -> None:
        with self.assertRaises(KeyError) as ctx:
            _ = self.map["_pwf_lexical_map__owner"]
        self.assertIn("reserved", str(ctx.exception))
        self.assertIn("_pwf_lexical_map__owner", str(ctx.exception))

    # ---- __iter__ / __len__ ---------------------------------------------- #

    def test_iter_matches_underlying_dict(self) -> None:
        self.assertEqual(list(iter(self.map)), ["foo", "bar"])

    def test_len_matches_underlying_dict(self) -> None:
        self.assertEqual(len(self.map), 2)
        empty: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(
            self.owner
        )
        self.assertEqual(len(empty), 0)

    # ---- __getattr__ ------------------------------------------------------ #

    def test_getattr_returns_item(self) -> None:
        self.assertIs(self.map.foo, self.foo)
        self.assertIs(self.map.bar, self.bar)

    def test_getattr_reserved_data_name_raises(self) -> None:
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map__data")

    def test_getattr_reserved_owner_name_raises(self) -> None:
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map__owner")

    def test_getattr_reserved_prefix_raises(self) -> None:
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map_anything")

    def test_getattr_missing_label_raises_attribute_error(self) -> None:
        with self.assertRaises(AttributeError) as ctx:
            _ = self.map.nonexistent
        msg = str(ctx.exception)
        self.assertIn("has no element 'nonexistent'", msg)
        self.assertIn("Available elements: dict_keys(['foo', 'bar'])", msg)
        self.assertIn(type(self.map).__name__, msg)


class TestCheckCoOwnership(unittest.TestCase):
    def setUp(self) -> None:
        self.owner = _StubOwner(lexical_path="owner")
        self.other = _StubOwner(lexical_path="other")
        self.foo = _StubItem(label="foo", owner=self.owner)

    def test_silent_on_co_owned(self) -> None:
        lexical.check_co_ownership(self.owner, [self.foo])  # no raise

    def test_silent_on_empty(self) -> None:
        lexical.check_co_ownership(self.owner, [])  # no raise

    def test_raises_on_mismatched(self) -> None:
        bad = _StubItem(label="bad", owner=self.other)
        with self.assertRaisesRegex(ValueError, "different owner") as ctx:
            lexical.check_co_ownership(self.owner, [self.foo, bad])
        msg = str(ctx.exception)
        self.assertIn(bad.lexical_path, msg)
        self.assertIn(self.other.lexical_path, msg)
        self.assertIn(self.owner.lexical_path, msg)

    def test_silent_on_none_owner(self) -> None:
        orphan = _StubItem(label="orphan", owner=None)
        lexical.check_co_ownership(self.owner, [orphan])


class TestLexicalPath(unittest.TestCase):
    def test_single_segment(self) -> None:
        p = lexical.LexicalPath("foo")
        self.assertEqual(p, "foo")

    def test_multi_segment_from_dotted_string(self) -> None:
        p = lexical.LexicalPath("a.b.c")
        self.assertEqual(p, "a.b.c")

    def test_flatten_mixed_paths_and_labels(self) -> None:
        p = lexical.LexicalPath("my_macro.add_0", "inputs", "x")
        self.assertEqual(p, "my_macro.add_0.inputs.x")

    def test_io_indicator_segments_allowed(self) -> None:
        for name in fr.schemas.RESERVED_NAMES:
            with self.subTest(name=name):
                p = lexical.LexicalPath(name)
                self.assertEqual(p, name)

    def test_empty_path_valid_and_falsy(self) -> None:
        p = lexical.LexicalPath()
        self.assertEqual(p, "")
        self.assertFalse(p)

    def test_empty_parts_skipped_on_concat(self) -> None:
        p = lexical.LexicalPath(lexical.LexicalPath(), "foo")
        self.assertEqual(p, "foo")

    def test_invalid_segments_raise(self) -> None:
        bad = ["a..b", "a.b.", ".a", "a b", "1abc"]
        for val in bad:
            with self.subTest(val=val), self.assertRaises(ValueError):
                lexical.LexicalPath(val)

    def test_keyword_segment_rejected(self) -> None:
        with self.assertRaises(ValueError):
            lexical.LexicalPath("for")

    def test_label_property(self) -> None:
        self.assertEqual(lexical.LexicalPath("a.b.c").label, "c")
        self.assertEqual(lexical.LexicalPath("solo").label, "solo")

    def test_parent_property(self) -> None:
        self.assertEqual(lexical.LexicalPath("a.b.c").parent, "a.b")
        self.assertFalse(lexical.LexicalPath("solo").parent)

    def test_segments_property(self) -> None:
        self.assertEqual(lexical.LexicalPath("a.b.c").segments, ("a", "b", "c"))
        self.assertEqual(lexical.LexicalPath().segments, ())

    def test_str_ops_preserved(self) -> None:
        p = lexical.LexicalPath("a.b.c")
        self.assertEqual(p.replace(".", "-"), "a-b-c")
        self.assertEqual(p, "a.b.c")
        self.assertIsInstance(p, str)
        self.assertIn("a.b.c", repr(p))


class TestLexicalPathHelpers(unittest.TestCase):
    def test_lexical_path_returns_lexical_path_instance(self) -> None:
        result = lexical.lexical_path("a", "b")
        self.assertIsInstance(result, lexical.LexicalPath)
        self.assertEqual(result, "a.b")

    def test_get_label_dotted_input(self) -> None:
        self.assertEqual(lexical.get_label("a.b.c"), "c")

    def test_get_label_single_input(self) -> None:
        self.assertEqual(lexical.get_label("a"), "a")


if __name__ == "__main__":
    unittest.main()
