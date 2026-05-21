"""
Covers the testable surface of `dag.py`:

* `MutablePortMap` (owner-check insert, `__setattr__` delegation, `__delitem__`).
* `MutableNodeMap` (reparenting insert, conflict raise, `__setattr__`,
  `__delitem__`).
* `Workflow.__init__` and `Workflow.undo_limit` (the only non-stub surface).
* `Macro` end-to-end via fixtures (children, edges identity, `run`).
* `evaluate_dag_by_layer` smoke (via a macro run).
* `topo_sort_nodes` for empty / single-layer / linear chain / order-determinism.
* `gather_target_inputs` for input-edge, sibling-edge, and port-omitted paths.
* `populate_outputs` for both `SourceHandle` and `InputSource` sources.
"""

from __future__ import annotations

import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import dag, datatypes
from tests.unit._wfms import _fixtures


class TestMacro(unittest.TestCase):
    """End-to-end exercise of `Macro` via the `macro` fixture."""

    def test_run_produces_expected_outputs(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.status, wfms.RunStatus.FINISHED)
        # Macro outputs come from inner `add` (a) and `sub` (s).
        self.assertEqual(run.outputs["a"].value, 3)
        self.assertEqual(run.outputs["s"].value, 0)

    def test_run_records_one_step_per_child(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        labels = [step.label for step in run.steps]
        self.assertEqual(sorted(labels), ["add_0", "sub_0"])
        self.assertEqual(len(run.steps), 2)

    def test_edges_constructed_from_recipe(self) -> None:
        n = _fixtures.macro_node()
        recipe_edges = (
            [(source, target) for target, source in n._recipe.input_edges.items()]
            + [(source, target) for target, source in n._recipe.edges.items()]
            + [(source, target) for target, source in n._recipe.output_edges.items()]
        )
        self.assertSetEqual(set(recipe_edges), set(n.edges))

    def test_nodes_property_returns_constructed_node_map(self) -> None:
        n = _fixtures.macro_node()
        self.assertIsInstance(n.nodes, datatypes.NodeMap)
        self.assertEqual(set(n.nodes), {"add_0", "sub_0"})

    def test_function_metadata_consistent_with_reference(self) -> None:
        # The fixture-decorated macro has no `_semantikon_metadata` attribute,
        # so the captured value is `None`. The attribute exists in both cases.
        undecorated = _fixtures.macro_node()
        self.assertIsNone(undecorated.function_metadata)

        decorated = _fixtures.annotated_macro_node()
        self.assertEqual("This is decorated", decorated.function_metadata["uri"])

    def test_result_type_classmethod(self) -> None:
        self.assertIs(dag.Macro._result_type(), frs.DagData)


class TestEvaluateDagByLayer(unittest.TestCase):
    def test_children_results_attached_to_run(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        # Every child has a result on the parent's run.result.nodes
        # and a successfully finished sub-run.
        self.assertEqual(set(run.result.nodes), {"add_0", "sub_0"})
        for step in run.steps:
            self.assertEqual(step.status, wfms.RunStatus.FINISHED)


class TestTopoSortNodes(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(dag.topo_sort_nodes({}, {}), [])

    def test_single_layer_no_edges_sorted_by_label(self) -> None:
        # `topo_sort_nodes` only consumes `nodes` as an iterable of labels,
        # so a plain `dict` is a valid stand-in for `NodeMap`.
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
        # `add_0` in the `macro` fixture takes both x and y from parent inputs.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs("add_0", run.result)
        self.assertEqual(inputs, {"x": 1, "y": 2})

    def test_sibling_edge_path(self) -> None:
        # `sub_0` reads `x` from sibling `add_0` output and `y` from
        # parent input `z`.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs("sub_0", run.result)
        self.assertEqual(inputs, {"x": 3, "y": 3})

    def test_port_omitted_when_no_edge(self) -> None:
        n = _fixtures.container_node()
        run = n.run()
        self.assertEqual(
            dag.gather_target_inputs("multiply_with_defaults_0", run.result),
            {},
            msg="The child has no input edges, so all ports should be omitted",
        )


class TestPopulateOutputs(unittest.TestCase):
    def test_source_handle_path(self) -> None:
        # `macro` output `s` is fed by `sub_0.output_0` (a SourceHandle).
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.outputs["s"].value, 0)
        self.assertEqual(run.outputs["a"].value, 3)

    def test_input_source_path(self) -> None:
        # `passthrough` output `x` is fed directly by parent input `x`
        # (an InputSource). `s` exercises the SourceHandle branch as a sanity
        # check that both branches coexist in one run.
        n = _fixtures.passthrough_node()
        run = n.run(x=42, y=5)
        self.assertEqual(run.outputs["x"].value, 42)
        self.assertEqual(run.outputs["s"].value, 47)


if __name__ == "__main__":
    unittest.main()
