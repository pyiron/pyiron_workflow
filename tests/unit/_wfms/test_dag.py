"""
Covers the testable surface of ``dag.py``:

* ``MutablePortMap`` (owner-check insert, ``__setattr__`` delegation, ``__delitem__``).
* ``MutableNodeMap`` (reparenting insert, conflict raise, ``__setattr__``,
  ``__delitem__``).
* ``Workflow.__init__`` and ``Workflow.undo_limit`` (the only non-stub surface).
* ``Macro`` end-to-end via fixtures (children, edges identity, ``run``).
* ``evaluate_dag_by_layer`` smoke (via a macro run).
* ``topo_sort_nodes`` for empty / single-layer / linear chain / order-determinism.
* ``gather_target_inputs`` for input-edge, sibling-edge, and port-omitted paths.
* ``populate_outputs`` for both ``SourceHandle`` and ``InputSource`` sources.
"""

from __future__ import annotations

import collections
import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import dag, datatypes
from tests.unit._wfms import _fixtures


class TestMutablePortMap(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = wfms.Workflow("wf")
        self.other = wfms.Workflow("other")
        self.port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )

    def test_setitem_matching_owner_succeeds(self) -> None:
        self.wf.inputs["x"] = self.port
        self.assertIs(self.wf.inputs["x"], self.port)

    def test_setitem_mismatched_owner_raises(self) -> None:
        foreign = datatypes.InputPort(
            label="y", owner=self.other, type_hint=None, type_metadata=None
        )
        with self.assertRaisesRegex(
            ValueError,
            "Port 'y' already has owner 'other' and cannot be assigned to a port map with owner 'wf'",
            msg="Readable references to both owners should appear",
        ):
            self.wf.inputs["y"] = foreign

    def test_setattr_delegates_to_setitem(self) -> None:
        # Attribute-style assignment runs through the same owner check.
        self.wf.inputs.x = self.port
        self.assertIs(self.wf.inputs["x"], self.port)

        foreign = datatypes.InputPort(
            label="y", owner=self.other, type_hint=None, type_metadata=None
        )
        with self.assertRaisesRegex(ValueError, "Port 'y' already has owner 'other'"):
            self.wf.inputs.y = foreign

    def test_delitem_removes_entry(self) -> None:
        self.wf.inputs["x"] = self.port
        self.assertIn("x", self.wf.inputs)
        del self.wf.inputs["x"]
        self.assertNotIn("x", self.wf.inputs)


class TestMutableNodeMap(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = wfms.Workflow("wf")
        self.other = wfms.Workflow("other")

    def test_setitem_unowned_node_reparents(self) -> None:
        node = _fixtures.atomic_add_node()
        self.assertIsNone(node.owner)
        self.wf.nodes["adder"] = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_setitem_foreign_owned_raises(self) -> None:
        node = _fixtures.atomic_add_node()
        self.other.nodes["adder"] = node
        self.assertIs(node.owner, self.other)
        with self.assertRaisesRegex(
            ValueError, "ode 'adder' already has owner 'other'"
        ):
            self.wf.nodes["adder"] = node

    def test_setattr_delegates_to_setitem(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes.adder = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_delitem_detaches_node(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes["adder"] = node
        del self.wf.nodes["adder"]
        self.assertIsNone(node.owner)
        self.assertNotIn("adder", self.wf.nodes)


class TestWorkflowInit(unittest.TestCase):
    def test_empty_workflow_has_empty_maps(self) -> None:
        wf = wfms.Workflow("wf")
        self.assertEqual(len(wf.inputs), 0)
        self.assertEqual(len(wf.outputs), 0)
        self.assertEqual(len(wf.nodes), 0)

    def test_custom_undo_limit(self) -> None:
        explicit_limit = 3

        with self.subTest("Baseline"):
            wf = wfms.Workflow("wf")
            self.assertIsInstance(wf.undo_stack, collections.deque)
            self.assertIsInstance(wf.redo_stack, collections.deque)
            self.assertNotEqual(wf.undo_stack.maxlen, explicit_limit)
            self.assertNotEqual(wf.redo_stack.maxlen, explicit_limit)

        with self.subTest("Explicit limit set"):
            wf = wfms.Workflow("wf", undo_limit=explicit_limit)
            self.assertEqual(wf.undo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.redo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.undo_limit, explicit_limit)


class TestWorkflowUndoLimit(unittest.TestCase):
    def test_setter_updates_both_stacks(self) -> None:
        wf = wfms.Workflow("wf", undo_limit=5)
        wf.undo_limit = 12
        self.assertEqual(wf.undo_limit, 12)
        self.assertEqual(wf.undo_stack.maxlen, 12)
        self.assertEqual(wf.redo_stack.maxlen, 12)


class TestMacro(unittest.TestCase):
    """End-to-end exercise of ``Macro`` via the ``macro`` fixture."""

    def test_run_produces_expected_outputs(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.status, wfms.RunStatus.FINISHED)
        # Macro outputs come from inner ``add`` (a) and ``sub`` (s).
        self.assertEqual(run.outputs["a"].value, 3)
        self.assertEqual(run.outputs["s"].value, 0)

    def test_run_records_one_step_per_child(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        labels = [step.label for step in run.steps]
        self.assertEqual(sorted(labels), ["add_0", "sub_0"])
        self.assertEqual(len(run.steps), 2)

    def test_edges_are_pass_throughs_to_recipe(self) -> None:
        n = _fixtures.macro_node()
        # Identity (the same object as the underlying recipe).
        self.assertIs(n.input_edges, n._recipe.input_edges)
        self.assertIs(n.edges, n._recipe.edges)
        self.assertIs(n.output_edges, n._recipe.output_edges)

    def test_nodes_property_returns_constructed_node_map(self) -> None:
        n = _fixtures.macro_node()
        self.assertIsInstance(n.nodes, datatypes.NodeMap)
        self.assertEqual(set(n.nodes), {"add_0", "sub_0"})

    def test_function_metadata_consistent_with_reference(self) -> None:
        # The fixture-decorated macro has no ``_semantikon_metadata`` attribute,
        # so the captured value is ``None``. The attribute exists in both cases.
        n = _fixtures.macro_node()
        self.assertTrue(hasattr(n, "_function_metadata"))
        self.assertEqual(n._function_metadata, n.function_metadata)
        self.assertIsNone(n.function_metadata)

    def test_result_type_classmethod(self) -> None:
        self.assertIs(dag.Macro._result_type(), frs.LiveWorkflow)


class TestEvaluateDagByLayer(unittest.TestCase):
    def test_children_results_attached_to_run(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        # Every child has a result on the parent's run.result.nodes
        # and a successfully finished sub-run.
        self.assertEqual(set(run.result.nodes), {"add_0", "sub_0"})
        for step in run.steps:
            self.assertEqual(step.run.status, wfms.RunStatus.FINISHED)


class TestTopoSortNodes(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(dag.topo_sort_nodes({}, {}), [])

    def test_single_layer_no_edges_sorted_by_label(self) -> None:
        # ``topo_sort_nodes`` only consumes ``nodes`` as an iterable of labels,
        # so a plain ``dict`` is a valid stand-in for ``NodeMap``.
        nodes = {"c": None, "a": None, "b": None}
        self.assertEqual(dag.topo_sort_nodes(nodes, {}), [["a", "b", "c"]])

    def test_linear_chain(self) -> None:
        nodes = {"a": None, "b": None, "c": None}
        edges = {
            frs.TargetHandle(node="b", port="x"): frs.SourceHandle(
                node="a", port="out"
            ),
            frs.TargetHandle(node="c", port="x"): frs.SourceHandle(
                node="b", port="out"
            ),
        }
        self.assertEqual(dag.topo_sort_nodes(nodes, edges), [["a"], ["b"], ["c"]])

    def test_layered_deterministic_across_insertion_orders(self) -> None:
        edges = {
            frs.TargetHandle(node="b", port="x"): frs.SourceHandle(
                node="a", port="out"
            ),
            frs.TargetHandle(node="c", port="x"): frs.SourceHandle(
                node="b", port="out"
            ),
        }
        forward = {"a": None, "b": None, "c": None}
        reverse = {"c": None, "b": None, "a": None}
        self.assertEqual(
            dag.topo_sort_nodes(forward, edges),
            dag.topo_sort_nodes(reverse, edges),
        )


class TestGatherTargetInputs(unittest.TestCase):
    def test_input_edge_path(self) -> None:
        # ``add_0`` in the ``macro`` fixture takes both x and y from parent inputs.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs(n.nodes["add_0"], run.result)
        self.assertEqual(inputs, {"x": 1, "y": 2})

    def test_sibling_edge_path(self) -> None:
        # ``sub_0`` reads ``x`` from sibling ``add_0`` output and ``y`` from
        # parent input ``z``.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs(n.nodes["sub_0"], run.result)
        self.assertEqual(inputs, {"x": 3, "y": 3})

    def test_port_omitted_when_no_edge(self) -> None:
        # The macro node itself has inputs (x, y, z) but none of them appear as
        # ``TargetHandle`` targets in the result's edges — so all ports omitted.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(dag.gather_target_inputs(n, run.result), {})


class TestPopulateOutputs(unittest.TestCase):
    def test_source_handle_path(self) -> None:
        # ``macro`` output ``s`` is fed by ``sub_0.output_0`` (a SourceHandle).
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.outputs["s"].value, 0)
        self.assertEqual(run.outputs["a"].value, 3)

    def test_input_source_path(self) -> None:
        # ``passthrough`` output ``x`` is fed directly by parent input ``x``
        # (an InputSource). ``s`` exercises the SourceHandle branch as a sanity
        # check that both branches coexist in one run.
        n = _fixtures.passthrough_node()
        run = n.run(x=42, y=5)
        self.assertEqual(run.outputs["x"].value, 42)
        self.assertEqual(run.outputs["s"].value, 47)


if __name__ == "__main__":
    unittest.main()
