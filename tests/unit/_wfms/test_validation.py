from __future__ import annotations

import unittest

import flowrep as fr
from flowrep.api import schemas as frs

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import validation
from tests.unit._wfms import _fixtures


@fr.atomic
def typed_int(x: int) -> int:
    return x + 0


@fr.atomic
def typed_float(x: float) -> float:
    return x + 0.0


def _typed_int_node(label: str = "typed_int"):
    return wfms.function2node(typed_int, label)


def _typed_float_node(label: str = "typed_float"):
    return wfms.function2node(typed_float, label)


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
        wf, edge = self._sibling_workflow(_typed_int_node, _fixtures.atomic_add_node)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_target_hint_only(self):
        wf, edge = self._sibling_workflow(_fixtures.atomic_add_node, _typed_int_node)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_both_hinted_ok(self):
        wf, edge = self._sibling_workflow(_typed_int_node, _typed_int_node)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_sibling_both_hinted_fail(self):
        wf, edge = self._sibling_workflow(_typed_float_node, _typed_int_node)
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        msg = str(ctx.exception)
        self.assertIn("wf", msg)
        self.assertIn("float", msg)
        self.assertIn("int", msg)

    # ---------- input edges (parent → child) -------------------------------

    def test_input_edge_both_hinted_ok(self):
        wf, edge = self._parent_workflow_with_input_hint(_typed_int_node, int)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_input_edge_both_hinted_fail(self):
        wf, edge = self._parent_workflow_with_input_hint(_typed_int_node, float)
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        self.assertIn("float", str(ctx.exception))
        self.assertIn("int", str(ctx.exception))

    # ---------- output edges (child → parent) ------------------------------

    def test_output_edge_both_hinted_ok(self):
        wf, edge = self._parent_workflow_with_output_hint(_typed_int_node, int)
        self.assertIs(validation.validate_edge(edge, wf), edge)

    def test_output_edge_both_hinted_fail(self):
        wf, edge = self._parent_workflow_with_output_hint(_typed_int_node, float)
        with self.assertRaises(TypeError) as ctx:
            validation.validate_edge(edge, wf)
        self.assertIn("int", str(ctx.exception))
        self.assertIn("float", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
