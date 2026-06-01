"""
Unit tests for :mod:`pyiron_workflow._wfms.validation`.

These tests construct small :class:`Workflow` instances directly and invoke
`validate_edge` against them in isolation (i.e. not through
:meth:`Workflow.add_edge`, which now wraps the validator).
"""

from __future__ import annotations

import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import validation, workflow
from tests.unit._wfms import _fixtures


class TestValidateEdge(unittest.TestCase):
    """Direct tests of `validate_edge` — does not flow through `add_edge`."""

    # ---------- helpers ----------------------------------------------------

    def _sibling_workflow(self, src_factory, tgt_factory):
        wf = _fixtures.build_workflow(
            node_specs={"src": src_factory, "tgt": tgt_factory},
            label="wf",
        )
        edge = wfms.EdgeTuple(
            frs.SourceHandle(node="src", port="output_0"),
            frs.TargetHandle(node="tgt", port="x"),
        )
        return wf, edge

    def _parent_workflow_with_input_hint(self, child_factory, hint):
        wf = _fixtures.build_workflow(
            inputs=["p"],
            node_specs={"child": child_factory},
            label="wf",
        )
        wf.add_port_hint(wf.inputs["p"], hint)
        edge = wfms.EdgeTuple(
            frs.InputSource(port="p"),
            frs.TargetHandle(node="child", port="x"),
        )
        return wf, edge

    def _parent_workflow_with_output_hint(self, child_factory, hint):
        wf = _fixtures.build_workflow(
            outputs=["p"],
            node_specs={"child": child_factory},
            label="wf",
        )
        wf.add_port_hint(wf.outputs["p"], hint)
        edge = wfms.EdgeTuple(
            frs.SourceHandle(node="child", port="output_0"),
            frs.OutputTarget(port="p"),
        )
        return wf, edge

    # ---------- hint combinations on sibling edges -------------------------

    def test_sibling_no_hint_no_hint(self):
        wf, edge = self._sibling_workflow(
            _fixtures.atomic_add_node, _fixtures.atomic_add_node
        )
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_source_hint_only(self):
        wf, edge = self._sibling_workflow(
            _fixtures.typed_int_node, _fixtures.atomic_add_node
        )
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_target_hint_only(self):
        wf, edge = self._sibling_workflow(
            _fixtures.atomic_add_node, _fixtures.typed_int_node
        )
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_both_hinted_ok(self):
        wf, edge = self._sibling_workflow(
            _fixtures.typed_int_node, _fixtures.typed_int_node
        )
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_both_hinted_fail(self):
        wf, edge = self._sibling_workflow(
            _fixtures.typed_float_node, _fixtures.typed_int_node
        )
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        msg = str(ctx.exception)
        self.assertIn("wf", msg)
        self.assertIn("float", msg)
        self.assertIn("int", msg)

    # ---------- input edges (parent → child) -------------------------------

    def test_input_edge_both_hinted_ok(self):
        wf, edge = self._parent_workflow_with_input_hint(_fixtures.typed_int_node, int)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_input_edge_both_hinted_fail(self):
        wf, edge = self._parent_workflow_with_input_hint(
            _fixtures.typed_int_node, float
        )
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        self.assertIn("float", str(ctx.exception))
        self.assertIn("int", str(ctx.exception))

    # ---------- output edges (child → parent) ------------------------------

    def test_output_edge_both_hinted_ok(self):
        wf, edge = self._parent_workflow_with_output_hint(_fixtures.typed_int_node, int)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_output_edge_both_hinted_fail(self):
        wf, edge = self._parent_workflow_with_output_hint(
            _fixtures.typed_int_node, float
        )
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        self.assertIn("int", str(ctx.exception))
        self.assertIn("float", str(ctx.exception))

    # ---------- strict mode -------------------------------------------------

    def test_strict_rejects_unfulfilled_request(self):
        # target hinted (int), source unhinted (add)
        wf, edge = self._sibling_workflow(
            _fixtures.atomic_add_node, _fixtures.typed_int_node
        )
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf, strict=True)
        self.assertIn("strict", str(ctx.exception))

    def test_default_allows_unfulfilled_request(self):
        wf, edge = self._sibling_workflow(
            _fixtures.atomic_add_node, _fixtures.typed_int_node
        )
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_strict_allows_both_unhinted(self):
        wf, edge = self._sibling_workflow(
            _fixtures.atomic_add_node, _fixtures.atomic_add_node
        )
        self.assertIs(validation.validate_edge(edge, wf, strict=True), edge)

    def test_strict_allows_source_only_hinted(self):
        # source hinted (int), target unhinted (add) -> target imposes nothing
        wf, edge = self._sibling_workflow(
            _fixtures.typed_int_node, _fixtures.atomic_add_node
        )
        self.assertIs(validation.validate_edge(edge, wf, strict=True), edge)

    def test_strict_still_rejects_mismatch(self):
        wf, edge = self._sibling_workflow(
            _fixtures.typed_float_node, _fixtures.typed_int_node
        )
        with self.assertRaises(TypeError):
            validation.validate_edge(edge, wf, strict=True)


class TestTypeValidationReport(unittest.TestCase):
    """Direct construction of reports — exercises `valid`/`complete` logic."""

    def _edge(self, src="a", tgt="b") -> wfms.EdgeTuple:
        return wfms.EdgeTuple(
            frs.SourceHandle(node=src, port="output_0"),
            frs.TargetHandle(node=tgt, port="x"),
        )

    def test_clean_report_is_valid_and_complete(self):
        r = validation.TypeValidationReport("g", [], [], {})
        self.assertTrue(r.valid)
        self.assertTrue(r.complete)

    def test_invalid_edge_makes_invalid_not_incomplete(self):
        r = validation.TypeValidationReport("g", [self._edge()], [], {})
        self.assertFalse(r.valid)
        self.assertTrue(r.complete)

    def test_unfulfilled_edge_makes_incomplete_not_invalid(self):
        r = validation.TypeValidationReport("g", [], [self._edge()], {})
        self.assertTrue(r.valid)
        self.assertFalse(r.complete)

    def test_notparseable_child_is_complete_false_valid_true(self):
        r = validation.TypeValidationReport(
            "g", [], [], {"c": validation.NotParseable()}
        )
        self.assertTrue(r.valid)
        self.assertFalse(r.complete)

    def test_validity_recurses_into_subreports(self):
        bad_child = validation.TypeValidationReport("g.c", [self._edge()], [], {})
        r = validation.TypeValidationReport("g", [], [], {"c": bad_child})
        self.assertFalse(r.valid)

    def test_completeness_recurses_into_subreports(self):
        incomplete_child = validation.TypeValidationReport(
            "g.c", [], [self._edge()], {}
        )
        r = validation.TypeValidationReport("g", [], [], {"c": incomplete_child})
        self.assertTrue(r.valid)
        self.assertFalse(r.complete)

    def test_notparseable_class_attributes(self):
        np = validation.NotParseable()
        self.assertTrue(np.valid)
        self.assertFalse(np.complete)


class TestValidateTypes(unittest.TestCase):
    """End-to-end `validate_types` over small graphs."""

    def _sibling_wf(self, src_factory, tgt_factory, *, type_validate=True):
        wf = _fixtures.build_workflow(
            node_specs={"src": src_factory, "tgt": tgt_factory}, label="wf"
        )
        edge = wfms.EdgeTuple(
            frs.SourceHandle(node="src", port="output_0"),
            frs.TargetHandle(node="tgt", port="x"),
        )
        wf.add_edge(edge, type_validate=type_validate)
        return wf

    def test_clean_hinted_dag_valid_and_complete(self):
        wf = self._sibling_wf(_fixtures.typed_int_node, _fixtures.typed_int_node)
        report = validation.validate_types(wf)
        self.assertTrue(report.valid)
        self.assertTrue(report.complete)

    def test_mismatch_is_invalid(self):
        wf = self._sibling_wf(
            _fixtures.typed_float_node, _fixtures.typed_int_node, type_validate=False
        )
        report = validation.validate_types(wf)
        self.assertFalse(report.valid)
        self.assertEqual(len(report.invalid_edges), 1)

    def test_both_unhinted_is_valid_and_complete(self):
        wf = self._sibling_wf(_fixtures.atomic_add_node, _fixtures.atomic_add_node)
        report = validation.validate_types(wf)
        self.assertTrue(report.valid)
        self.assertTrue(report.complete)

    def test_unfulfilled_request_is_incomplete_not_invalid(self):
        wf = self._sibling_wf(_fixtures.atomic_add_node, _fixtures.typed_int_node)
        report = validation.validate_types(wf)
        self.assertTrue(report.valid)
        self.assertFalse(report.complete)
        self.assertEqual(len(report.unfulfilled_edges), 1)

    def test_source_only_hinted_is_valid_and_complete(self):
        wf = self._sibling_wf(_fixtures.typed_int_node, _fixtures.atomic_add_node)
        report = validation.validate_types(wf)
        self.assertTrue(report.valid)
        self.assertTrue(report.complete)

    def test_atomic_is_trivially_valid_and_complete(self):
        report = validation.validate_types(_fixtures.atomic_add_node())
        self.assertTrue(report.valid)
        self.assertTrue(report.complete)
        self.assertEqual(report.subreports, {})

    def test_bad_target_type_raises(self):
        with self.assertRaises(TypeError):
            validation.validate_types(object())  # type: ignore[arg-type]

    def test_recipe_input_builds_macro(self):
        report = validation.validate_types(_fixtures.macro.flowrep_recipe)
        self.assertIsInstance(report, validation.TypeValidationReport)
        self.assertEqual(report.name, "from_recipe")

    def test_flow_control_child_is_notparseable(self):
        wf = workflow.Workflow("wf")
        wf.add_node(_fixtures.foreach_node("fe"))
        report = validation.validate_types(wf)
        self.assertIsInstance(report.subreports["fe"], validation.NotParseable)
        self.assertTrue(report.valid)  # no actual type error
        self.assertFalse(report.complete)  # but something was unparseable

    def test_nested_failure_drives_top_level_invalid(self):
        inner = self._sibling_wf(
            _fixtures.typed_float_node, _fixtures.typed_int_node, type_validate=False
        )
        outer = workflow.Workflow("outer")
        outer.add_node(inner)
        report = validation.validate_types(outer)
        self.assertFalse(report.valid)
        self.assertIsInstance(report.subreports["wf"], validation.TypeValidationReport)

    def test_text_clean_report_is_ok(self):
        report = validation.validate_types(
            self._sibling_wf(_fixtures.typed_int_node, _fixtures.typed_int_node)
        )
        self.assertIn("valid=True", report.text)
        self.assertIn("complete=True", report.text)
        self.assertTrue(report.text.rstrip().endswith("OK"))

    def test_text_lists_both_edge_buckets(self):
        invalid = self._sibling_wf(
            _fixtures.typed_float_node, _fixtures.typed_int_node, type_validate=False
        )
        report = validation.validate_types(invalid)
        self.assertIn("invalid edges:", report.text)

        unfulfilled = self._sibling_wf(
            _fixtures.atomic_add_node, _fixtures.typed_int_node
        )
        self.assertIn("unfulfilled edges:", validation.validate_types(unfulfilled).text)

    def test_text_nested_has_no_double_indent(self):
        outer = workflow.Workflow("outer")
        outer.add_node(
            self._sibling_wf(
                _fixtures.typed_float_node,
                _fixtures.typed_int_node,
                type_validate=False,
            )
        )
        report = validation.validate_types(outer)
        # The child header is indented exactly one tab deeper than the parent's.
        self.assertIn("\n\tType validation for 'outer.wf'", report.text)

    def test_text_renders_notparseable_child(self):
        wf = workflow.Workflow("wf")
        wf.add_node(_fixtures.foreach_node("fe"))
        report = validation.validate_types(wf)
        # `repr` round-trips through `text`; the NotParseable child is listed.
        self.assertEqual(repr(report), report.text)
        self.assertIn("fe: <NOT PARSEABLE>", report.text)


if __name__ == "__main__":
    unittest.main()
