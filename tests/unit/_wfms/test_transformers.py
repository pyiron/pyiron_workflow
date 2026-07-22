"""Unit tests for :mod:`pyiron_workflow._wfms.transformers`."""

from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import atomic, transformers
from tests.unit._wfms import _fixtures


class TestTransform1toN(unittest.TestCase):
    def test_input_label_class_constant(self) -> None:
        self.assertEqual(transformers.Transform1toN.input_label, "items")

    def test_output_label_zero(self) -> None:
        self.assertEqual(transformers.Transform1toN.output_label(0), "output_0")

    def test_output_label_seven(self) -> None:
        self.assertEqual(transformers.Transform1toN.output_label(7), "output_7")

    def test_iterable_to_outputs_returns_tuple(self) -> None:
        self.assertEqual(
            transformers.Transform1toN.iterable_to_outputs([1, 2, 3]),
            (1, 2, 3),
        )

    def test_recipe_inputs(self) -> None:
        recipe = transformers.Transform1toN(3).recipe
        self.assertEqual(recipe.inputs, ["items"])

    def test_recipe_outputs(self) -> None:
        recipe = transformers.Transform1toN(3).recipe
        self.assertEqual(recipe.outputs, ["output_0", "output_1", "output_2"])

    def test_recipe_unpack_mode(self) -> None:
        recipe = transformers.Transform1toN(3).recipe
        self.assertEqual(recipe.unpack_mode, fr.schemas.UnpackMode.TUPLE)

    def test_recipe_restricted_input_kinds(self) -> None:
        recipe = transformers.Transform1toN(3).recipe
        self.assertEqual(
            recipe.reference.restricted_input_kinds["items"],
            fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
        )

    def test_node_returns_atomic_with_label_and_recipe(self) -> None:
        transformer = transformers.Transform1toN(3)
        node = transformer.node("lbl")
        self.assertIsInstance(node, atomic.Atomic)
        self.assertEqual(node.label, "lbl")
        self.assertEqual(node.recipe.inputs, ["items"])
        self.assertEqual(node.recipe.outputs, ["output_0", "output_1", "output_2"])


class TestTransformNto1(unittest.TestCase):
    def test_output_label_class_constant(self) -> None:
        self.assertEqual(transformers.TransformNto1.output_label, "output_0")

    def test_input_label_zero(self) -> None:
        self.assertEqual(transformers.TransformNto1.input_label(0), "item_0")

    def test_input_label_two(self) -> None:
        self.assertEqual(transformers.TransformNto1.input_label(2), "item_2")

    def test_inputs_to_list_returns_list(self) -> None:
        self.assertEqual(
            transformers.TransformNto1.inputs_to_list(1, 2, 3),
            [1, 2, 3],
        )

    def test_recipe_inputs(self) -> None:
        recipe = transformers.TransformNto1(3).recipe
        self.assertEqual(recipe.inputs, ["item_0", "item_1", "item_2"])

    def test_recipe_outputs(self) -> None:
        recipe = transformers.TransformNto1(3).recipe
        self.assertEqual(recipe.outputs, ["output_0"])

    def test_recipe_unpack_mode(self) -> None:
        recipe = transformers.TransformNto1(3).recipe
        self.assertEqual(recipe.unpack_mode, fr.schemas.UnpackMode.TUPLE)

    def test_recipe_restricted_input_kinds(self) -> None:
        recipe = transformers.TransformNto1(3).recipe
        for i in range(3):
            with self.subTest(i=i):
                self.assertEqual(
                    recipe.reference.restricted_input_kinds[f"item_{i}"],
                    fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                )

    def test_node_returns_atomic_with_label(self) -> None:
        transformer = transformers.TransformNto1(3)
        node = transformer.node("lbl")
        self.assertIsInstance(node, atomic.Atomic)
        self.assertEqual(node.label, "lbl")


class TestAutoencoderRoundTrip(unittest.TestCase):
    def test_values_round_trip_through_compress_then_expand(self) -> None:
        node = _fixtures.autoencoder_node()
        run = node.run(a=1, b=20, c=300)
        self.assertEqual(run.outputs.x, 1)
        self.assertEqual(run.outputs.y, 20)
        self.assertEqual(run.outputs.z, 300)


if __name__ == "__main__":
    unittest.main()
