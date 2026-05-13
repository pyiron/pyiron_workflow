from __future__ import annotations

import unittest
from dataclasses import dataclass, field

from pyiron_workflow._wfms import lexical

# --------------------------------------------------------------------------- #
# Tiny structural stubs (satisfy `HasLexicalPath` / `Lexical` protocols). #
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


class TestLexicalMap(unittest.TestCase):
    def setUp(self) -> None:
        self.owner = _StubOwner(lexical_path="owner")
        self.other = _StubOwner(lexical_path="other")
        self.foo = _StubItem(label="foo", owner=self.owner)
        self.bar = _StubItem(label="bar", owner=self.owner)
        self.map: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(
            self.owner, self.foo, self.bar
        )

    # ---- __slots__ contract ------------------------------------------------ #

    def test_slots_is_the_contract(self) -> None:
        self.assertEqual(
            lexical.LexicalMap.__slots__,
            ("_pwf_lexical_map__data", "_pwf_lexical_map__owner"),
        )

    # ---- __init__ co-ownership invariant ----------------------------------- #

    def test_init_co_owned_items_silent(self) -> None:
        # The fixture-built map already exercises this; an explicit assertion
        # documents intent.
        m = lexical.LexicalMap(self.owner, self.foo, self.bar)
        self.assertEqual(len(m), 2)

    def test_init_empty_is_silent(self) -> None:
        m: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(self.owner)
        self.assertEqual(len(m), 0)

    def test_init_mismatched_owner_raises(self) -> None:
        bad = _StubItem(label="bad", owner=self.other)
        with self.assertRaisesRegex(
            ValueError,
            "Map owned by 'owner' cannot be initialized with items that have a different owner",
        ) as ctx:
            lexical.LexicalMap(self.owner, self.foo, bad)
        msg = str(ctx.exception)
        self.assertIn("owner", msg)
        # Offending dict {item.lexical_path: owner.lexical_path} appears in msg.
        self.assertIn(bad.lexical_path, msg)
        self.assertIn(self.other.lexical_path, msg)
        # Owner of the map is also referenced.
        self.assertIn(self.owner.lexical_path, msg)

    def test_init_none_owner_raises(self) -> None:
        orphan = _StubItem(label="orphan", owner=None)
        with self.assertRaisesRegex(
            ValueError,
            "Map owned by 'owner' cannot be initialized with items that have a different owner",
        ) as ctx:
            lexical.LexicalMap(self.owner, orphan)
        msg = str(ctx.exception)
        self.assertIn(orphan.lexical_path, msg)
        # The None branch must surface as `None` in the offending dict.
        self.assertIn("None", msg)

    # ---- __getitem__ ------------------------------------------------------- #

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

    # ---- __iter__ / __len__ ----------------------------------------------- #

    def test_iter_matches_underlying_dict(self) -> None:
        self.assertEqual(list(iter(self.map)), ["foo", "bar"])

    def test_len_matches_underlying_dict(self) -> None:
        self.assertEqual(len(self.map), 2)
        empty: lexical.LexicalMap[_StubItem, _StubOwner] = lexical.LexicalMap(
            self.owner
        )
        self.assertEqual(len(empty), 0)

    # ---- __getattr__ ------------------------------------------------------- #

    def test_getattr_returns_item(self) -> None:
        self.assertIs(self.map.foo, self.foo)
        self.assertIs(self.map.bar, self.bar)

    def test_getattr_reserved_data_name_raises(self) -> None:
        # Note: `__getattribute__` finds the real slot first; we exercise the
        # `__getattr__` guard by going through it directly.
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map__data")

    def test_getattr_reserved_owner_name_raises(self) -> None:
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map__owner")

    def test_getattr_reserved_prefix_raises(self) -> None:
        # Any name with the guarded prefix short-circuits.
        with self.assertRaises(AttributeError):
            self.map.__getattr__("_pwf_lexical_map_anything")

    def test_getattr_missing_label_raises_attribute_error(self) -> None:
        with self.assertRaises(AttributeError) as ctx:
            _ = self.map.nonexistent
        msg = str(ctx.exception)
        self.assertIn("has no attribute", msg)
        self.assertIn("nonexistent", msg)
        self.assertIn(type(self.map).__name__, msg)


if __name__ == "__main__":
    unittest.main()
