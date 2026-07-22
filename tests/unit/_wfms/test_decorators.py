from __future__ import annotations

import dataclasses
import unittest

import flowrep as fr

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import decorators, validation
from tests.unit._wfms import _fixtures


class _ReservedFieldCarrier:
    """Plain class whose field name collides with the reserved 'dataclass' port."""

    dataclass: int = 0


def _make_local_dataclass():
    @wfms.dataclass
    class Local:
        a: int

    return Local


def _make_local_atomic():
    @wfms.atomic
    def inner(x):
        return x

    return inner


class _NoVersionCarrier:
    a: int = 0


class _ForbidLambdaCarrier:
    a: int = 0


def _nounpack_target(x):
    return x, x  # with UnpackMode.NONE this stays a single output port


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
        self.assertEqual(run.outputs.dataclass, _fixtures.PlainPoint(1.0, 2.0))

    def test_inputs_with_defaults(self) -> None:
        ref = _fixtures.WithDefaults.pwf_inputs2dc.recipe.reference
        self.assertEqual(ref.inputs_with_defaults, ["b", "c"])

    def test_default_factory_resolves_on_run(self) -> None:
        run = _fixtures.WithDefaults.pwf_inputs2dc.run(a=1)
        self.assertEqual(run.outputs.dataclass, _fixtures.WithDefaults(a=1))

    def test_kw_only_recorded_as_restricted(self) -> None:
        ref = _fixtures.FrozenKw.pwf_inputs2dc.recipe.reference
        self.assertEqual(set(ref.restricted_input_kinds), {"nova", "foo"})
        self.assertTrue(
            all(
                kind is fr.schemas.RestrictedParamKind.KEYWORD_ONLY
                for kind in ref.restricted_input_kinds.values()
            )
        )


class TestDataclass2Outputs(unittest.TestCase):
    def test_string_annotation_resolves(self):
        node = _fixtures.WithInitVar.pwf_dc2outputs.node()
        self.assertEqual(node.outputs["a"].type_hint, int)

    def test_plain_ports(self) -> None:
        node = _fixtures.PlainPoint.pwf_dc2outputs.node()
        self.assertEqual(list(node.inputs), ["dataclass"])
        self.assertEqual(list(node.outputs), ["x", "y"])

    def test_plain_run_unpacks_fields(self) -> None:
        run = _fixtures.PlainPoint.pwf_dc2outputs.run(
            dataclass=_fixtures.PlainPoint(1.0, 2.0)
        )
        self.assertEqual(run.outputs.x, 1.0)
        self.assertEqual(run.outputs.y, 2.0)

    def test_frozen_read_back(self) -> None:
        run = _fixtures.FrozenKw.pwf_dc2outputs.run(
            dataclass=_fixtures.FrozenKw(nova=1.1)
        )
        self.assertEqual(run.outputs.nova, 1.1)
        self.assertEqual(run.outputs.foo, 42)

    def test_init_false_field_is_output_not_input(self) -> None:
        # init=False -> real field (output) but not a constructor param (no input)
        self.assertEqual(
            list(_fixtures.WithInitFalse.pwf_dc2outputs.node().outputs), ["a", "c"]
        )
        self.assertEqual(
            list(_fixtures.WithInitFalse.pwf_inputs2dc.node().inputs), ["a"]
        )
        run = _fixtures.WithInitFalse.pwf_dc2outputs.run(
            dataclass=_fixtures.WithInitFalse(a=1)
        )
        self.assertEqual(run.outputs.c, 7)

    def test_init_var_is_input_not_output(self) -> None:
        # InitVar -> constructor param (input) but not a real field (no output)
        self.assertEqual(
            list(_fixtures.WithInitVar.pwf_dc2outputs.node().outputs), ["a", "b"]
        )
        self.assertEqual(
            list(_fixtures.WithInitVar.pwf_inputs2dc.node().inputs), ["a", "d", "b"]
        )

    def test_round_trip(self) -> None:
        wf = wfms.Workflow("roundtrip")
        wf.to_values = _fixtures.PlainPoint.pwf_dc2outputs.node()
        wf.to_dc = _fixtures.PlainPoint.pwf_inputs2dc.node()
        wf.create_input_for(wf.to_values.inputs.dataclass)
        wf.create_output_from(wf.to_dc)
        for k in wf.to_values.outputs:
            wf.connect(wf.to_values.outputs[k], wf.to_dc.inputs[k])
        result = wf.run(dataclass=_fixtures.PlainPoint(1.0, 2.0)).outputs.dataclass
        self.assertEqual(result, _fixtures.PlainPoint(1.0, 2.0))


class TestDataclassGuards(unittest.TestCase):
    def test_reserved_field_name_raises(self) -> None:
        with self.assertRaises(ValueError):
            wfms.dataclass(_ReservedFieldCarrier)

    def test_local_dataclass_rejected(self) -> None:
        with self.assertRaises(ImportError):
            _make_local_dataclass()

    def test_require_version_raises_without_version(self) -> None:
        with self.assertRaises(ValueError):
            wfms.dataclass(require_version=True)(_NoVersionCarrier)

    def test_forbid_lambda_forwarded(self) -> None:
        # forbid_lambda is irrelevant for a class but must be accepted and
        # forwarded to both tools without error.
        dcls = wfms.dataclass(forbid_lambda=True)(_ForbidLambdaCarrier)
        self.assertIsInstance(dcls.pwf_inputs2dc, decorators.Inputs2Dataclass)
        self.assertIsInstance(dcls.pwf_dc2outputs, decorators.Dataclass2Outputs)


class TestAtomicWorkflowDecorators(unittest.TestCase):
    def test_bare_atomic_attaches_tool(self) -> None:
        self.assertIsInstance(_fixtures.wfms_add.pwf, decorators.DecoratedAtomic)

    def test_atomic_default_and_explicit_label(self) -> None:
        self.assertEqual(_fixtures.wfms_add.pwf.node().label, "wfms_add")
        self.assertEqual(_fixtures.wfms_add.pwf.node("custom").label, "custom")

    def test_atomic_run(self) -> None:
        run = _fixtures.wfms_add.pwf.run(x=1, y=2)
        (only,) = run.outputs.keys()
        self.assertEqual(run.outputs[only], 3)

    def test_string_dispatch_relabels_output(self) -> None:
        self.assertIsInstance(
            _fixtures.wfms_add_relabelled.pwf, decorators.DecoratedAtomic
        )
        self.assertEqual(
            list(_fixtures.wfms_add_relabelled.pwf.node().outputs), ["relabelled_sum"]
        )

    def test_workflow_attaches_macro_tool(self) -> None:
        self.assertIsInstance(_fixtures.wfms_macro.pwf, decorators.DecoratedMacro)
        self.assertEqual(_fixtures.wfms_macro.pwf.node().label, "wfms_macro")

    def test_macro_validate_returns_report(self) -> None:
        report = _fixtures.wfms_macro.pwf.validate()
        self.assertIsInstance(report, validation.CombinedValidationReport)

    def test_invalid_dispatch_raises_type_error(self) -> None:
        with self.assertRaises(TypeError):
            wfms.atomic(123)

    def test_keyword_param_form_attaches_tool(self) -> None:
        decorated = wfms.atomic(unpack_mode=fr.schemas.UnpackMode.NONE)(
            _nounpack_target
        )
        self.assertIsInstance(decorated.pwf, decorators.DecoratedAtomic)
        self.assertEqual(len(list(decorated.pwf.node().outputs)), 1)

    def test_local_function_rejected(self) -> None:
        with self.assertRaises(ImportError):
            _make_local_atomic()


if __name__ == "__main__":
    unittest.main()
