from __future__ import annotations

import dataclasses
import unittest

import flowrep as fr

from pyiron_workflow._wfms import decorators
from tests.unit._wfms import _fixtures


class TestDataclassDecorator(unittest.TestCase):
    def test_attaches_both_tools(self) -> None:
        self.assertIsInstance(
            _fixtures.PlainPoint.pwf_inputs2dc, decorators.Inputs2Dataclass
        )
        self.assertIsInstance(
            _fixtures.PlainPoint.pwf_dc2outputs, decorators.Dataclass2Outputs
        )

    def test_still_a_dataclass(self) -> None:
        self.assertTrue(dataclasses.is_dataclass(_fixtures.PlainPoint))
        self.assertEqual(_fixtures.PlainPoint(1.0, 2.0).x, 1.0)

    def test_parameterized_call_form(self) -> None:
        # @wfms.dataclass(frozen=True, kw_only=True) returns a usable dataclass
        self.assertTrue(dataclasses.is_dataclass(_fixtures.FrozenKw))
        inst = _fixtures.FrozenKw(nova=1.1)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            inst.nova = 2.2  # type: ignore[misc]

    def test_unannotated_attribute_is_not_a_field(self) -> None:
        field_names = {f.name for f in dataclasses.fields(_fixtures.FrozenKw)}
        self.assertNotIn("not_a_field", field_names)
        self.assertNotIn(
            "not_a_field", list(_fixtures.FrozenKw.pwf_inputs2dc.node().inputs)
        )

    def test_node_labels_default_and_explicit(self) -> None:
        self.assertEqual(
            _fixtures.PlainPoint.pwf_inputs2dc.node().label, "Inputs2PlainPoint"
        )
        self.assertEqual(
            _fixtures.PlainPoint.pwf_dc2outputs.node().label, "Dataclass2PlainPoint"
        )
        self.assertEqual(
            _fixtures.PlainPoint.pwf_inputs2dc.node("custom").label, "custom"
        )


class TestInputs2Dataclass(unittest.TestCase):
    def test_ports(self) -> None:
        node = _fixtures.PlainPoint.pwf_inputs2dc.node()
        self.assertEqual(list(node.inputs), ["x", "y"])
        self.assertEqual(list(node.outputs), ["dataclass"])

    def test_run_constructs_instance(self) -> None:
        run = _fixtures.PlainPoint.pwf_inputs2dc.run(x=1.0, y=2.0)
        self.assertEqual(run.outputs["dataclass"].value, _fixtures.PlainPoint(1.0, 2.0))

    def test_inputs_with_defaults(self) -> None:
        ref = _fixtures.WithDefaults.pwf_inputs2dc.recipe.reference
        self.assertEqual(ref.inputs_with_defaults, ["b", "c"])

    def test_default_factory_resolves_on_run(self) -> None:
        run = _fixtures.WithDefaults.pwf_inputs2dc.run(a=1)
        self.assertEqual(run.outputs["dataclass"].value, _fixtures.WithDefaults(a=1))

    def test_kw_only_recorded_as_restricted(self) -> None:
        ref = _fixtures.FrozenKw.pwf_inputs2dc.recipe.reference
        self.assertEqual(set(ref.restricted_input_kinds), {"nova", "foo"})
        self.assertTrue(
            all(
                kind is fr.schemas.RestrictedParamKind.KEYWORD_ONLY
                for kind in ref.restricted_input_kinds.values()
            )
        )


if __name__ == "__main__":
    unittest.main()
