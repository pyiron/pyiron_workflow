from __future__ import annotations

import unittest
from typing import Annotated, Union

import semantikon

from pyiron_workflow._wfms import annotation


class TestAnnotationToTypeHint(unittest.TestCase):
    def test_none_returns_none(self) -> None:
        self.assertIsNone(annotation.annotation_to_type_hint(None))

    def test_plain_type_returned_unchanged(self) -> None:
        self.assertIs(annotation.annotation_to_type_hint(int), int)

    def test_single_metadata_annotated_is_stripped(self) -> None:
        self.assertIs(
            annotation.annotation_to_type_hint(Annotated[int, "meta"]),
            int,
        )

    def test_multi_metadata_annotated_is_stripped(self) -> None:
        self.assertIs(
            annotation.annotation_to_type_hint(Annotated[int, "m1", "m2"]),
            int,
        )

    def test_generic_alias_returned_unchanged(self) -> None:
        hint = annotation.annotation_to_type_hint(list[int])
        self.assertEqual(hint, list[int])


class TestUnwrapAnnotated(unittest.TestCase):
    def test_plain_type_returned_unchanged(self) -> None:
        self.assertIs(annotation._unwrap_annotated(int), int)

    def test_annotated_with_multiple_metadata_is_stripped(self) -> None:
        self.assertIs(
            annotation._unwrap_annotated(Annotated[str, "x", "y"]),
            str,
        )

    def test_union_returned_unchanged(self) -> None:
        # Keep the legacy `typing.Union` form deliberately: the test plan
        # exercises this exact alias to confirm non-`Annotated` generic
        # aliases are returned unchanged.
        union_hint = Union[int, str]  # noqa: UP007
        self.assertEqual(annotation._unwrap_annotated(union_hint), union_hint)


class TestAnnotationToTypeMetadata(unittest.TestCase):
    def test_none_returns_none(self) -> None:
        self.assertIsNone(annotation.annotation_to_type_metadata(None))

    def test_plain_type_propagates_semantikon_error(self) -> None:
        # semantikon.parse_metadata expects an Annotated alias; this wrapper
        # does not guard against bare types, so the underlying AttributeError
        # from semantikon should surface unchanged. This asserts the
        # passthrough contract without mocking the collaborator.
        with self.assertRaises(AttributeError):
            annotation.annotation_to_type_metadata(int)

    def test_annotated_with_semantikon_metadata(self) -> None:
        payload = {"units": "meter", "label": "length"}
        result = annotation.annotation_to_type_metadata(Annotated[int, payload])
        self.assertIsInstance(result, semantikon.TypeMetadata)
        # Structural assertions: at least one field round-trips from payload.
        self.assertEqual(result.units, "meter")
        self.assertEqual(result.label, "length")


if __name__ == "__main__":
    unittest.main()
