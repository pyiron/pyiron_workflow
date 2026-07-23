from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import execution, transformers
from pyiron_workflow._wfms.flowcontrollers import forflow
from tests.unit._wfms import _fixtures


def _atomic_body_recipe() -> fr.schemas.AtomicRecipe:
    """The `add(x, y)` atomic recipe — handy as a 2-input body."""
    return _fixtures.add.flowrep_recipe


def _macro_body_recipe() -> fr.schemas.WorkflowRecipe:
    """The `macro(x, y, z)` workflow recipe — handy as a 3-input body."""
    return _fixtures.macro.flowrep_recipe


def _build_nested_only_recipe() -> fr.schemas.ForEachRecipe:
    """body=add(x, y); `x` nested, `y` broadcast."""
    body = fr.schemas.LabeledRecipe(label="body", recipe=_atomic_body_recipe())
    return fr.schemas.ForEachRecipe(
        inputs=["xs", "y"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=["x"],
        zipped_ports=[],
    )


def _build_zipped_only_recipe() -> fr.schemas.ForEachRecipe:
    """body=add(x, y); both zipped."""
    body = fr.schemas.LabeledRecipe(label="body", recipe=_atomic_body_recipe())
    return fr.schemas.ForEachRecipe(
        inputs=["xs", "ys"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="ys"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=[],
        zipped_ports=["x", "y"],
    )


def _build_mixed_recipe() -> fr.schemas.ForEachRecipe:
    """body=macro(x, y, z); `x` nested, `y` and `z` zipped."""
    body = fr.schemas.LabeledRecipe(label="body", recipe=_macro_body_recipe())
    return fr.schemas.ForEachRecipe(
        inputs=["xs", "ys", "ws"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="ys"
            ),
            fr.schemas.TargetHandle(node="body", port="z"): fr.schemas.InputSource(
                port="ws"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="s"
            ),
        },
        nested_ports=["x"],
        zipped_ports=["y", "z"],
    )


def _make_broadcast_only_recipe() -> fr.schemas.ForEachRecipe:
    """body=add(x, y); both broadcast.

    The recipe-level validators forbid a ForEach with no iterated ports, so
    `model_construct` is used to bypass them — `_build_runtime_dag` doesn't
    care, it just reads fields.
    """
    body = fr.schemas.LabeledRecipe(label="body", recipe=_atomic_body_recipe())
    return fr.schemas.ForEachRecipe.model_construct(
        inputs=["x", "y"],
        outputs=["out"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="out"): fr.schemas.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=[],
        zipped_ports=[],
    )


def _prepare_run(
    fe: forflow.ForEach, inputs: dict[str, object]
) -> execution.Run[fr.schemas.ForEachData]:
    """Build a Run with seeded input port values, ready for _build_runtime_dag."""
    live = fe.generate_flowrep_live_node()
    for name, val in inputs.items():
        live.input_ports[name].value = val
    return execution.Run(
        lexical_path=fe.lexical_path, result=live, status=execution.RunStatus.PENDING
    )


class TestBuildRuntimeDagNestedOnly(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_nested_only_recipe()
        self.fe = forflow.ForEach(self.recipe, "fe")
        self.dag_run = _prepare_run(self.fe, {"xs": [1, 2], "y": 100})
        self.nodes = self.fe._build_runtime_dag(self.dag_run)

    def test_total_body_count(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 2)

    def test_scatter_node_present(self) -> None:
        self.assertIn("scatter_xs", self.nodes)

    def test_aggregator_node_present(self) -> None:
        self.assertIn("aggregate_sums", self.nodes)

    def test_input_edge_to_scatter(self) -> None:
        scatter_input = self.dag_run.result.input_edges[
            fr.schemas.TargetHandle(
                node="scatter_xs",
                port=transformers.Transform1toN.input_label,
            )
        ]
        self.assertEqual(scatter_input, fr.schemas.InputSource(port="xs"))

    def test_scatter_to_bodies_indexed_correctly(self) -> None:
        for i in range(2):
            src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="x")
            ]
            self.assertEqual(
                src,
                fr.schemas.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(i),
                ),
            )

    def test_broadcast_y_to_each_body(self) -> None:
        for i in range(2):
            src = self.dag_run.result.input_edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="y")
            ]
            self.assertEqual(src, fr.schemas.InputSource(port="y"))

    def test_bodies_to_aggregator(self) -> None:
        for i in range(2):
            src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(
                    node="aggregate_sums",
                    port=transformers.TransformNto1.input_label(i),
                )
            ]
            self.assertEqual(
                src, fr.schemas.SourceHandle(node=f"body_{i}", port="output_0")
            )

    def test_output_edge_from_aggregator(self) -> None:
        src = self.dag_run.result.output_edges[fr.schemas.OutputTarget(port="sums")]
        self.assertEqual(
            src,
            fr.schemas.SourceHandle(
                node="aggregate_sums",
                port=transformers.TransformNto1.output_label,
            ),
        )


class TestBuildRuntimeDagZippedOnly(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_zipped_only_recipe()
        self.fe = forflow.ForEach(self.recipe, "fe")
        self.dag_run = _prepare_run(self.fe, {"xs": [1, 2, 3], "ys": [10, 20, 30]})
        self.nodes = self.fe._build_runtime_dag(self.dag_run)

    def test_total_steps_is_zipped_width(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 3)

    def test_two_scatter_nodes(self) -> None:
        self.assertIn("scatter_xs", self.nodes)
        self.assertIn("scatter_ys", self.nodes)

    def test_zipped_indices_pair_per_body(self) -> None:
        for i in range(3):
            x_src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="x")
            ]
            y_src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="y")
            ]
            self.assertEqual(
                x_src,
                fr.schemas.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(i % 3),
                ),
            )
            self.assertEqual(
                y_src,
                fr.schemas.SourceHandle(
                    node="scatter_ys",
                    port=transformers.Transform1toN.output_label(i % 3),
                ),
            )


class TestBuildRuntimeDagMixed(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _build_mixed_recipe()
        self.fe = forflow.ForEach(self.recipe, "fe")
        self.dag_run = _prepare_run(
            self.fe,
            {"xs": [1, 2], "ys": [10, 20, 30], "ws": [100, 200, 300]},
        )
        self.nodes = self.fe._build_runtime_dag(self.dag_run)

    def test_total_steps_equals_product(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(len(body_labels), 6)

    def test_every_nested_zipped_pair_present_exactly_once(self) -> None:
        nested_length = 2  # len(xs)
        zipped_width = 3  # len(ys) == len(ws)
        # nested stride from spec: _nested_strides(6, {'xs': 2}) == {'xs': 3}
        observed_pairs: set[tuple[int, int]] = set()
        for i in range(nested_length * zipped_width):
            x_src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="x")
            ]
            y_src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="y")
            ]
            z_src = self.dag_run.result.edges[
                fr.schemas.TargetHandle(node=f"body_{i}", port="z")
            ]

            nested_idx = (i // 3) % nested_length
            zipped_idx = i % zipped_width

            self.assertEqual(
                x_src,
                fr.schemas.SourceHandle(
                    node="scatter_xs",
                    port=transformers.Transform1toN.output_label(nested_idx),
                ),
            )
            self.assertEqual(
                y_src,
                fr.schemas.SourceHandle(
                    node="scatter_ys",
                    port=transformers.Transform1toN.output_label(zipped_idx),
                ),
            )
            self.assertEqual(
                z_src,
                fr.schemas.SourceHandle(
                    node="scatter_ws",
                    port=transformers.Transform1toN.output_label(zipped_idx),
                ),
            )
            observed_pairs.add((nested_idx, zipped_idx))

        expected_pairs = {
            (n, z) for n in range(nested_length) for z in range(zipped_width)
        }
        self.assertEqual(observed_pairs, expected_pairs)


def _build_broadcast_seed_recipe() -> fr.schemas.ForEachRecipe:
    """
    A valid ForEach recipe with the same input-port labels (`x`, `y`)
    as the broadcast-only recipe we'll swap in later.
    """
    body = fr.schemas.LabeledRecipe(label="body", recipe=_atomic_body_recipe())
    return fr.schemas.ForEachRecipe(
        inputs=["x", "y"],
        outputs=["out"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="out"): fr.schemas.SourceHandle(
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
        self.fe = forflow.ForEach(seed_recipe, "fe")
        self.dag_run = _prepare_run(self.fe, {"x": 42, "y": 100})

        self.dag_run.result.recipe = _make_broadcast_only_recipe()
        self.nodes = self.fe._build_runtime_dag(self.dag_run)

    def test_single_body_copy(self) -> None:
        body_labels = [label for label in self.nodes if label.startswith("body_")]
        self.assertEqual(body_labels, ["body_0"])

    def test_no_scatter_nodes(self) -> None:
        scatters = [label for label in self.nodes if label.startswith("scatter_")]
        self.assertEqual(scatters, [])

    def test_all_parent_inputs_broadcast_to_body(self) -> None:
        x_src = self.dag_run.result.input_edges[
            fr.schemas.TargetHandle(node="body_0", port="x")
        ]
        y_src = self.dag_run.result.input_edges[
            fr.schemas.TargetHandle(node="body_0", port="y")
        ]
        self.assertEqual(x_src, fr.schemas.InputSource(port="x"))
        self.assertEqual(y_src, fr.schemas.InputSource(port="y"))

    def test_no_sibling_scatter_edges_to_body(self) -> None:
        # No scatter -> body edges should exist; the only sibling edges in
        # `edges` are body -> aggregator.
        for target, source in self.dag_run.result.edges.items():
            self.assertFalse(
                target.node.startswith("body_"),
                msg=f"Unexpected scatter->body sibling edge: {target} <- {source}",
            )

    def test_body_to_aggregator(self) -> None:
        src = self.dag_run.result.edges[
            fr.schemas.TargetHandle(
                node="aggregate_out",
                port=transformers.TransformNto1.input_label(0),
            )
        ]
        self.assertEqual(src, fr.schemas.SourceHandle(node="body_0", port="output_0"))


class TestValidateZippedLengths(unittest.TestCase):
    def test_empty_returns_empty(self) -> None:
        self.assertEqual(forflow.ForEach._validate_zipped_lengths({}), {})

    def test_equal_lengths_returned_unchanged(self) -> None:
        length_map = {"a": 3, "b": 3}
        self.assertEqual(
            forflow.ForEach._validate_zipped_lengths(length_map), length_map
        )

    def test_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Expected all zipped ports to have the same length"
        ) as ctx:
            forflow.ForEach._validate_zipped_lengths({"a": 3, "b": 4})
        msg = str(ctx.exception)
        self.assertIn("'a'", msg)
        self.assertIn("'b'", msg)
        self.assertIn("3", msg)
        self.assertIn("4", msg)


class TestCalculateTotalSteps(unittest.TestCase):
    def test_both_empty(self) -> None:
        self.assertEqual(forflow.ForEach._calculate_total_steps({}, {}), 1)

    def test_nested_only(self) -> None:
        self.assertEqual(
            forflow.ForEach._calculate_total_steps({"a": 2, "b": 3}, {}), 6
        )

    def test_zipped_only(self) -> None:
        self.assertEqual(
            forflow.ForEach._calculate_total_steps({}, {"a": 4, "b": 4}), 4
        )

    def test_mixed(self) -> None:
        self.assertEqual(forflow.ForEach._calculate_total_steps({"a": 2}, {"b": 3}), 6)


class TestNestedStrides(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(forflow.ForEach._nested_strides(1, {}), {})

    def test_single_nested(self) -> None:
        self.assertEqual(forflow.ForEach._nested_strides(3, {"a": 3}), {"a": 1})

    def test_two_nested_leftmost_widest(self) -> None:
        self.assertEqual(
            forflow.ForEach._nested_strides(6, {"a": 2, "b": 3}),
            {"a": 3, "b": 1},
        )

    def test_mixed_with_zipped_width(self) -> None:
        # total = 2 * 3 (nested) * 4 (zipped) = 24
        self.assertEqual(
            forflow.ForEach._nested_strides(24, {"a": 2, "b": 3}),
            {"a": 12, "b": 4},
        )


class TestLabelHelpers(unittest.TestCase):
    def test_scatter_label(self) -> None:
        self.assertEqual(forflow.ForEach._scatter_label("xs"), "scatter_xs")

    def test_body_label(self) -> None:
        self.assertEqual(forflow.ForEach._body_label("body", 3), "body_3")

    def test_aggregate_label(self) -> None:
        self.assertEqual(forflow.ForEach._aggregate_label("sums"), "aggregate_sums")


class TestBodyToParentLabelMap(unittest.TestCase):
    def test_filters_by_body_label_and_port(self) -> None:
        edges: fr.schemas.InputEdges = {
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="ys"
            ),
            # Unrelated entries that must be filtered out:
            fr.schemas.TargetHandle(
                node="body", port="ignored"
            ): fr.schemas.InputSource(port="z"),
            fr.schemas.TargetHandle(node="other", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
        }
        result = forflow.ForEach._body_to_parent_label_map(edges, "body", ["x", "y"])
        self.assertEqual(result, {"x": "xs", "y": "ys"})

    def test_empty_inputs(self) -> None:
        self.assertEqual(
            forflow.ForEach._body_to_parent_label_map({}, "body", ["x"]), {}
        )


class TestCapturedOutputLabelMap(unittest.TestCase):
    def test_includes_only_source_handle_entries_from_body(self) -> None:
        edges: fr.schemas.OutputEdges = {
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="s"
            ),
            fr.schemas.OutputTarget(port="passed"): fr.schemas.InputSource(port="xs"),
            fr.schemas.OutputTarget(port="other"): fr.schemas.SourceHandle(
                node="elsewhere", port="o"
            ),
        }
        result = forflow.ForEach._captured_output_label_map(edges, "body")
        self.assertEqual(result, {"sums": "s"})


class TestTransferLabelMap(unittest.TestCase):
    def test_only_input_source_entries_returned(self) -> None:
        edges: fr.schemas.OutputEdges = {
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="s"
            ),
            fr.schemas.OutputTarget(port="x_used"): fr.schemas.InputSource(port="xs"),
            fr.schemas.OutputTarget(port="y_used"): fr.schemas.InputSource(port="ys"),
        }
        result = forflow.ForEach._transfer_label_map(edges)
        self.assertEqual(result, {"x_used": "xs", "y_used": "ys"})

    def test_empty(self) -> None:
        self.assertEqual(forflow.ForEach._transfer_label_map({}), {})


if __name__ == "__main__":
    unittest.main()
