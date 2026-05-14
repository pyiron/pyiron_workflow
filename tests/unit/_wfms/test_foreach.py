from __future__ import annotations

import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution, transformers
from pyiron_workflow._wfms.flowcontrollers import foreach
from tests.unit._wfms import _fixtures


def _atomic_body_recipe() -> frs.AtomicNode:
    """The `add(x, y)` atomic recipe — handy as a 2-input body."""
    return _fixtures.add.flowrep_recipe


def _macro_body_recipe() -> frs.WorkflowNode:
    """The `macro(x, y, z)` workflow recipe — handy as a 3-input body."""
    return _fixtures.macro.flowrep_recipe


def _build_nested_only_recipe() -> frs.ForEachNode:
    """body=add(x, y); `x` nested, `y` broadcast."""
    body = frs.LabeledNode(label="body", node=_atomic_body_recipe())
    return frs.ForEachNode(
        inputs=["xs", "y"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="xs"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
        },
        output_edges={
            frs.OutputTarget(port="sums"): frs.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=["x"],
        zipped_ports=[],
    )


def _build_zipped_only_recipe() -> frs.ForEachNode:
    """body=add(x, y); both zipped."""
    body = frs.LabeledNode(label="body", node=_atomic_body_recipe())
    return frs.ForEachNode(
        inputs=["xs", "ys"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="xs"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="ys"),
        },
        output_edges={
            frs.OutputTarget(port="sums"): frs.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=[],
        zipped_ports=["x", "y"],
    )


def _build_mixed_recipe() -> frs.ForEachNode:
    """body=macro(x, y, z); `x` nested, `y` and `z` zipped."""
    body = frs.LabeledNode(label="body", node=_macro_body_recipe())
    return frs.ForEachNode(
        inputs=["xs", "ys", "ws"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="xs"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="ys"),
            frs.TargetHandle(node="body", port="z"): frs.InputSource(port="ws"),
        },
        output_edges={
            frs.OutputTarget(port="sums"): frs.SourceHandle(node="body", port="s"),
        },
        nested_ports=["x"],
        zipped_ports=["y", "z"],
    )


def _make_broadcast_only_recipe() -> frs.ForEachNode:
    """body=add(x, y); both broadcast.

    The recipe-level validators forbid a ForEach with no iterated ports, so
    `model_construct` is used to bypass them — `_build_runtime_dag` doesn't
    care, it just reads fields.
    """
    body = frs.LabeledNode(label="body", node=_atomic_body_recipe())
    return frs.ForEachNode.model_construct(
        inputs=["x", "y"],
        outputs=["out"],
        body_node=body,
        input_edges={
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
        },
        output_edges={
            frs.OutputTarget(port="out"): frs.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=[],
        zipped_ports=[],
    )


def _prepare_run(
    fe: foreach.ForEach, inputs: dict[str, object]
) -> execution.Run[frs.LiveForEach]:
    """Build a Run with seeded input port values, ready for _build_runtime_dag."""
    live = fe.generate_flowrep_live_node()
    for name, val in inputs.items():
        live.input_ports[name].value = val
    return execution.Run(result=live, status=execution.RunStatus.PENDING)


class TestForEachProspectiveAndRetrospective(unittest.TestCase):
    """Pre-run vs post-run views of the ForEach in the `for_wf` fixture."""

    def setUp(self) -> None:
        self.node = _fixtures.for_wf_node()
        self.fe = self.node.nodes.for_each_0

    # ---- pre-run: prospective populated, retrospective empty ---------------- #

    def test_prospective_input_edges_matches_recipe(self) -> None:
        self.assertEqual(self.fe.prospective_input_edges, self.fe.recipe.input_edges)
        self.assertGreater(len(self.fe.prospective_input_edges), 0)

    def test_prospective_edges_is_empty(self) -> None:
        self.assertEqual(self.fe.prospective_edges, {})

    def test_prospective_output_edges_matches_recipe(self) -> None:
        self.assertGreater(len(self.fe.prospective_output_edges), 0)
        self.assertTrue(
            all(len(v) == 1 for v in self.fe.prospective_output_edges.values())
        )
        flattened_prospects = {
            k: v[0] for k, v in self.fe.prospective_output_edges.items()
        }
        self.assertDictEqual(flattened_prospects, self.fe.recipe.output_edges)

    def test_prospective_nodes_has_single_body(self) -> None:
        self.assertEqual(len(self.fe.prospective_nodes), 1)
        body_label = self.fe.recipe.body_node.label
        self.assertIn(body_label, self.fe.prospective_nodes)

    def test_pre_run_retrospective_views_empty(self) -> None:
        self.assertEqual(self.fe.input_edges, {})
        self.assertEqual(self.fe.edges, {})
        self.assertEqual(self.fe.output_edges, {})
        self.assertEqual(len(self.fe.nodes), 0)

    # ---- post-run: prospective unchanged, retrospective populated ---------- #

    def test_post_run_views(self) -> None:
        prospective_input_before = self.fe.prospective_input_edges
        prospective_edges_before = self.fe.prospective_edges
        prospective_output_before = self.fe.prospective_output_edges
        prospective_nodes_before = list(self.fe.prospective_nodes)

        self.node.run(xs=[1, 2], ys=[10, 20], z=1000)

        # prospective unchanged
        self.assertEqual(self.fe.prospective_input_edges, prospective_input_before)
        self.assertEqual(self.fe.prospective_edges, prospective_edges_before)
        self.assertEqual(self.fe.prospective_output_edges, prospective_output_before)
        self.assertEqual(list(self.fe.prospective_nodes), prospective_nodes_before)

        # retrospective populated
        self.assertGreater(len(self.fe.input_edges), 0)
        self.assertGreater(len(self.fe.edges), 0)
        self.assertGreater(len(self.fe.output_edges), 0)

        body_count = sum(1 for label in self.fe.nodes if label.startswith("body_"))
        self.assertEqual(body_count, 4)  # 2 (xs) x 2 (ys)


class TestBuildRuntimeDagNestedOnly(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_nested_only_recipe()
        self.fe = foreach.ForEach("fe", self.recipe)
        self.dag_run = _prepare_run(self.fe, {"xs": [1, 2], "y": 100})
        (
            self.nodes,
            self.input_edges,
            self.edges,
            self.output_edges,
        ) = self.fe._build_runtime_dag(self.dag_run)

    def test_total_body_count(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 2)

    def test_scatter_node_present(self) -> None:
        self.assertIn("scatter_xs", self.nodes)

    def test_aggregator_node_present(self) -> None:
        self.assertIn("aggregate_sums", self.nodes)

    def test_input_edge_to_scatter(self) -> None:
        scatter_input = self.input_edges[
            frs.TargetHandle(
                node="scatter_xs",
                port=transformers.Transform1toN.input_label,
            )
        ]
        self.assertEqual(scatter_input, frs.InputSource(port="xs"))

    def test_scatter_to_bodies_indexed_correctly(self) -> None:
        for i in range(2):
            src = self.edges[frs.TargetHandle(node=f"body_{i}", port="x")]
            self.assertEqual(
                src,
                frs.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(i),
                ),
            )

    def test_broadcast_y_to_each_body(self) -> None:
        for i in range(2):
            src = self.input_edges[frs.TargetHandle(node=f"body_{i}", port="y")]
            self.assertEqual(src, frs.InputSource(port="y"))

    def test_bodies_to_aggregator(self) -> None:
        for i in range(2):
            src = self.edges[
                frs.TargetHandle(
                    node="aggregate_sums",
                    port=transformers.TransformNto1.input_label(i),
                )
            ]
            self.assertEqual(src, frs.SourceHandle(node=f"body_{i}", port="output_0"))

    def test_output_edge_from_aggregator(self) -> None:
        src = self.output_edges[frs.OutputTarget(port="sums")]
        self.assertEqual(
            src,
            frs.SourceHandle(
                node="aggregate_sums",
                port=transformers.TransformNto1.output_label,
            ),
        )


class TestBuildRuntimeDagZippedOnly(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_zipped_only_recipe()
        self.fe = foreach.ForEach("fe", self.recipe)
        self.dag_run = _prepare_run(self.fe, {"xs": [1, 2, 3], "ys": [10, 20, 30]})
        (
            self.nodes,
            self.input_edges,
            self.edges,
            self.output_edges,
        ) = self.fe._build_runtime_dag(self.dag_run)

    def test_total_steps_is_zipped_width(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 3)

    def test_two_scatter_nodes(self) -> None:
        self.assertIn("scatter_xs", self.nodes)
        self.assertIn("scatter_ys", self.nodes)

    def test_zipped_indices_pair_per_body(self) -> None:
        for i in range(3):
            x_src = self.edges[frs.TargetHandle(node=f"body_{i}", port="x")]
            y_src = self.edges[frs.TargetHandle(node=f"body_{i}", port="y")]
            self.assertEqual(
                x_src,
                frs.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(i % 3),
                ),
            )
            self.assertEqual(
                y_src,
                frs.SourceHandle(
                    node="scatter_ys",
                    port=transformers.Transform1toN.output_label(i % 3),
                ),
            )


class TestBuildRuntimeDagMixed(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_mixed_recipe()
        self.fe = foreach.ForEach("fe", self.recipe)
        self.dag_run = _prepare_run(
            self.fe,
            {"xs": [1, 2], "ys": [10, 20, 30], "ws": [100, 200, 300]},
        )
        (
            self.nodes,
            self.input_edges,
            self.edges,
            self.output_edges,
        ) = self.fe._build_runtime_dag(self.dag_run)

    def test_total_steps_equals_product(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 6)

    def test_every_nested_zipped_pair_present_exactly_once(self) -> None:
        nested_length = 2  # len(xs)
        zipped_width = 3  # len(ys) == len(ws)
        # nested stride from spec: _nested_strides(6, {'xs': 2}) == {'xs': 3}
        observed_pairs: set[tuple[int, int]] = set()
        for i in range(nested_length * zipped_width):
            x_src = self.edges[frs.TargetHandle(node=f"body_{i}", port="x")]
            y_src = self.edges[frs.TargetHandle(node=f"body_{i}", port="y")]
            z_src = self.edges[frs.TargetHandle(node=f"body_{i}", port="z")]

            nested_idx = (i // 3) % nested_length
            zipped_idx = i % zipped_width

            self.assertEqual(
                x_src,
                frs.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(nested_idx),
                ),
            )
            self.assertEqual(
                y_src,
                frs.SourceHandle(
                    node="scatter_ys",
                    port=transformers.Transform1toN.output_label(zipped_idx),
                ),
            )
            self.assertEqual(
                z_src,
                frs.SourceHandle(
                    node="scatter_ws",
                    port=transformers.Transform1toN.output_label(zipped_idx),
                ),
            )
            observed_pairs.add((nested_idx, zipped_idx))

        expected_pairs = {
            (n, z) for n in range(nested_length) for z in range(zipped_width)
        }
        self.assertEqual(observed_pairs, expected_pairs)


def _build_broadcast_seed_recipe() -> frs.ForEachNode:
    """
    A valid ForEach recipe with the same input-port labels (`x`, `y`)
    as the broadcast-only recipe we'll swap in later.
    """
    body = frs.LabeledNode(label="body", node=_atomic_body_recipe())
    return frs.ForEachNode(
        inputs=["x", "y"],
        outputs=["out"],
        body_node=body,
        input_edges={
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
        },
        output_edges={
            frs.OutputTarget(port="out"): frs.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=["x"],
        zipped_ports=[],
    )


class TestBuildRuntimeDagBroadcastOnly(unittest.TestCase):
    """
    Broadcast-only path; recipe is built via `model_construct` to
    bypass the ForEach-recipe validator that requires at least one iterated
    port.
    """

    def setUp(self) -> None:
        # Build with a valid seed recipe whose inputs (x, y) match the
        # broadcast-only recipe; then swap recipes on the live.
        seed_recipe = _build_broadcast_seed_recipe()
        self.fe = foreach.ForEach("fe", seed_recipe)
        self.dag_run = _prepare_run(self.fe, {"x": 42, "y": 100})

        self.dag_run.result.recipe = _make_broadcast_only_recipe()

        (
            self.nodes,
            self.input_edges,
            self.edges,
            self.output_edges,
        ) = self.fe._build_runtime_dag(self.dag_run)

    def test_single_body_copy(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(body_labels, ["body_0"])

    def test_no_scatter_nodes(self) -> None:
        scatters = [label for label in self.nodes if label.startswith("scatter_")]
        self.assertEqual(scatters, [])

    def test_all_parent_inputs_broadcast_to_body(self) -> None:
        x_src = self.input_edges[frs.TargetHandle(node="body_0", port="x")]
        y_src = self.input_edges[frs.TargetHandle(node="body_0", port="y")]
        self.assertEqual(x_src, frs.InputSource(port="x"))
        self.assertEqual(y_src, frs.InputSource(port="y"))

    def test_no_sibling_scatter_edges_to_body(self) -> None:
        # No scatter -> body edges should exist; the only sibling edges in
        # `edges` are body -> aggregator.
        for target, source in self.edges.items():
            self.assertFalse(
                target.node.startswith("body_"),
                msg=f"Unexpected scatter->body sibling edge: {target} <- {source}",
            )

    def test_body_to_aggregator(self) -> None:
        src = self.edges[
            frs.TargetHandle(
                node="aggregate_out",
                port=transformers.TransformNto1.input_label(0),
            )
        ]
        self.assertEqual(src, frs.SourceHandle(node="body_0", port="output_0"))


class TestValidateZippedLengths(unittest.TestCase):
    def test_empty_returns_empty(self) -> None:
        self.assertEqual(foreach.ForEach._validate_zipped_lengths({}), {})

    def test_equal_lengths_returned_unchanged(self) -> None:
        length_map = {"a": 3, "b": 3}
        self.assertEqual(
            foreach.ForEach._validate_zipped_lengths(length_map), length_map
        )

    def test_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Expected all zipped ports to have the same length"
        ) as ctx:
            foreach.ForEach._validate_zipped_lengths({"a": 3, "b": 4})
        msg = str(ctx.exception)
        self.assertIn("'a'", msg)
        self.assertIn("'b'", msg)
        self.assertIn("3", msg)
        self.assertIn("4", msg)


class TestCalculateTotalSteps(unittest.TestCase):
    def test_both_empty(self) -> None:
        self.assertEqual(foreach.ForEach._calculate_total_steps({}, {}), 1)

    def test_nested_only(self) -> None:
        self.assertEqual(
            foreach.ForEach._calculate_total_steps({"a": 2, "b": 3}, {}), 6
        )

    def test_zipped_only(self) -> None:
        self.assertEqual(
            foreach.ForEach._calculate_total_steps({}, {"a": 4, "b": 4}), 4
        )

    def test_mixed(self) -> None:
        self.assertEqual(foreach.ForEach._calculate_total_steps({"a": 2}, {"b": 3}), 6)


class TestNestedStrides(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(foreach.ForEach._nested_strides(1, {}), {})

    def test_single_nested(self) -> None:
        self.assertEqual(foreach.ForEach._nested_strides(3, {"a": 3}), {"a": 1})

    def test_two_nested_leftmost_widest(self) -> None:
        self.assertEqual(
            foreach.ForEach._nested_strides(6, {"a": 2, "b": 3}),
            {"a": 3, "b": 1},
        )

    def test_mixed_with_zipped_width(self) -> None:
        # total = 2 * 3 (nested) * 4 (zipped) = 24
        self.assertEqual(
            foreach.ForEach._nested_strides(24, {"a": 2, "b": 3}),
            {"a": 12, "b": 4},
        )


class TestLabelHelpers(unittest.TestCase):
    def test_scatter_label(self) -> None:
        self.assertEqual(foreach.ForEach._scatter_label("xs"), "scatter_xs")

    def test_body_label(self) -> None:
        self.assertEqual(foreach.ForEach._body_label("body", 3), "body_3")

    def test_aggregate_label(self) -> None:
        self.assertEqual(foreach.ForEach._aggregate_label("sums"), "aggregate_sums")


class TestBodyToParentLabelMap(unittest.TestCase):
    def test_filters_by_body_label_and_port(self) -> None:
        edges: frs.InputEdges = {
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="xs"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="ys"),
            # Unrelated entries that must be filtered out:
            frs.TargetHandle(node="body", port="ignored"): frs.InputSource(port="z"),
            frs.TargetHandle(node="other", port="x"): frs.InputSource(port="xs"),
        }
        result = foreach.ForEach._body_to_parent_label_map(edges, "body", ["x", "y"])
        self.assertEqual(result, {"x": "xs", "y": "ys"})

    def test_empty_inputs(self) -> None:
        self.assertEqual(
            foreach.ForEach._body_to_parent_label_map({}, "body", ["x"]), {}
        )


class TestCapturedOutputLabelMap(unittest.TestCase):
    def test_includes_only_source_handle_entries_from_body(self) -> None:
        edges: frs.OutputEdges = {
            frs.OutputTarget(port="sums"): frs.SourceHandle(node="body", port="s"),
            frs.OutputTarget(port="passed"): frs.InputSource(port="xs"),
            frs.OutputTarget(port="other"): frs.SourceHandle(
                node="elsewhere", port="o"
            ),
        }
        result = foreach.ForEach._captured_output_label_map(edges, "body")
        self.assertEqual(result, {"sums": "s"})


class TestTransferLabelMap(unittest.TestCase):
    def test_only_input_source_entries_returned(self) -> None:
        edges: frs.OutputEdges = {
            frs.OutputTarget(port="sums"): frs.SourceHandle(node="body", port="s"),
            frs.OutputTarget(port="x_used"): frs.InputSource(port="xs"),
            frs.OutputTarget(port="y_used"): frs.InputSource(port="ys"),
        }
        result = foreach.ForEach._transfer_label_map(edges)
        self.assertEqual(result, {"x_used": "xs", "y_used": "ys"})

    def test_empty(self) -> None:
        self.assertEqual(foreach.ForEach._transfer_label_map({}), {})


if __name__ == "__main__":
    unittest.main()
