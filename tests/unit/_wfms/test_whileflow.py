from __future__ import annotations

import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.flowcontrollers import whileflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _non_looping_recipe() -> frs.WhileNode:
    """
    WhileNode with inputs=["n", "step"] but outputs=["n"] only.

    `step` is never a while-loop output, so it must always be sourced from the
    parent input, never from a previous body iteration.  The body subtracts
    `step` from `n`.
    """
    sub_recipe = _fixtures.sub.flowrep_recipe  # sub(x, y) → output_0
    body = frs.WorkflowNode(
        inputs=["n", "step"],
        outputs=["n"],
        nodes={"sub_0": sub_recipe},
        input_edges={
            frs.TargetHandle(node="sub_0", port="x"): frs.InputSource(port="n"),
            frs.TargetHandle(node="sub_0", port="y"): frs.InputSource(port="step"),
        },
        edges={},
        output_edges={
            frs.OutputTarget(port="n"): frs.SourceHandle(node="sub_0", port="output_0"),
        },
    )
    return frs.WhileNode(
        inputs=["n", "step"],
        outputs=["n"],
        case=frs.ConditionalCase(
            condition=frs.LabeledNode(
                label="condition", node=_fixtures.is_positive.flowrep_recipe
            ),
            body=frs.LabeledNode(label="body", node=body),
        ),
        input_edges={
            frs.TargetHandle(node="condition", port="n"): frs.InputSource(port="n"),
            frs.TargetHandle(node="body", port="n"): frs.InputSource(port="n"),
            frs.TargetHandle(node="body", port="step"): frs.InputSource(port="step"),
        },
        output_edges={
            frs.OutputTarget(port="n"): frs.SourceHandle(node="body", port="n"),
        },
    )


# --------------------------------------------------------------------------- #
# Prospective + retrospective surface                                         #
# --------------------------------------------------------------------------- #


class TestWhileProspectiveAndRetrospective(unittest.TestCase):
    """Pre-run vs post-run views of the While in the `while_countdown` fixture."""

    def setUp(self) -> None:
        self.node = _fixtures.while_countdown_node()
        self.whl = self.node.nodes.while_0

    def test_prospective_input_edges_matches_recipe(self) -> None:
        self.assertEqual(self.whl.prospective_input_edges, self.whl.recipe.input_edges)
        self.assertGreater(len(self.whl.prospective_input_edges), 0)

    def test_prospective_edges_is_non_empty_and_matches_recipe(self) -> None:
        recipe = self.whl.recipe
        expected = {**recipe.body_body_edges, **recipe.body_condition_edges}
        self.assertEqual(self.whl.prospective_edges, expected)
        self.assertGreater(len(self.whl.prospective_edges), 0)

    def test_prospective_edges_contains_body_to_body_loop_edge(self) -> None:
        self.assertIn(
            frs.TargetHandle(node="body", port="n"),
            self.whl.prospective_edges,
        )
        self.assertEqual(
            self.whl.prospective_edges[frs.TargetHandle(node="body", port="n")],
            frs.SourceHandle(node="body", port="n"),
        )

    def test_prospective_output_edges_matches_recipe(self) -> None:
        self.assertEqual(
            self.whl.prospective_output_edges, self.whl.recipe.output_edges
        )
        self.assertGreater(len(self.whl.prospective_output_edges), 0)

    def test_prospective_nodes_has_condition_and_body(self) -> None:
        self.assertEqual(set(self.whl.prospective_nodes), {"condition", "body"})

    def test_pre_run_retrospective_views_empty(self) -> None:
        self.assertEqual(self.whl.input_edges, {})
        self.assertEqual(self.whl.edges, {})
        self.assertEqual(self.whl.output_edges, {})
        self.assertEqual(len(self.whl.nodes), 0)

    def test_post_run_views(self) -> None:
        prospective_input_before = self.whl.prospective_input_edges
        prospective_edges_before = self.whl.prospective_edges
        prospective_output_before = self.whl.prospective_output_edges
        prospective_nodes_before = list(self.whl.prospective_nodes)

        self.node.run(n=3)

        # prospective unchanged
        self.assertEqual(self.whl.prospective_input_edges, prospective_input_before)
        self.assertEqual(self.whl.prospective_edges, prospective_edges_before)
        self.assertEqual(self.whl.prospective_output_edges, prospective_output_before)
        self.assertEqual(list(self.whl.prospective_nodes), prospective_nodes_before)

        # retrospective populated: condition_0..3 + body_0..2
        self.assertGreater(len(self.whl.input_edges), 0)
        self.assertGreater(len(self.whl.edges), 0)
        self.assertGreater(len(self.whl.output_edges), 0)
        self.assertEqual(
            set(self.whl.nodes),
            {
                "condition_0",
                "body_0",
                "condition_1",
                "body_1",
                "condition_2",
                "body_2",
                "condition_3",
            },
        )


# --------------------------------------------------------------------------- #
# evaluate — zero iterations                                                  #
# --------------------------------------------------------------------------- #


class TestEvaluateZeroIterations(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.while_recipe()
        self.whl = whileflow.While("whl", self.recipe)
        self.run = self.whl.run(n=0)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_sourced_from_input(self) -> None:
        self.assertEqual(self.run.outputs["n"].value, 0)

    def test_only_condition_ran(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["condition_0"])

    def test_output_edge_is_input_source_fallback(self) -> None:
        self.assertEqual(
            self.whl.output_edges[frs.OutputTarget(port="n")],
            frs.InputSource(port="n"),
        )


# --------------------------------------------------------------------------- #
# evaluate — single iteration                                                 #
# --------------------------------------------------------------------------- #


class TestEvaluateSingleIteration(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.while_recipe()
        self.whl = whileflow.While("whl", self.recipe)
        self.run = self.whl.run(n=1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_value(self) -> None:
        self.assertEqual(self.run.outputs["n"].value, 0)

    def test_steps_order(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["condition_0", "body_0", "condition_1"])

    def test_output_edge_points_to_last_body(self) -> None:
        self.assertEqual(
            self.whl.output_edges[frs.OutputTarget(port="n")],
            frs.SourceHandle(node="body_0", port="n"),
        )


# --------------------------------------------------------------------------- #
# evaluate — multiple iterations                                              #
# --------------------------------------------------------------------------- #


class TestEvaluateMultipleIterations(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.while_recipe()
        self.whl = whileflow.While("whl", self.recipe)
        self.run = self.whl.run(n=3)

    def test_output_value(self) -> None:
        self.assertEqual(self.run.outputs["n"].value, 0)

    def test_step_count_and_order(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(
            labels,
            [
                "condition_0",
                "body_0",
                "condition_1",
                "body_1",
                "condition_2",
                "body_2",
                "condition_3",
            ],
        )

    def test_retrospective_nodes_keyset(self) -> None:
        self.assertEqual(
            set(self.whl.nodes),
            {
                "condition_0",
                "body_0",
                "condition_1",
                "body_1",
                "condition_2",
                "body_2",
                "condition_3",
            },
        )

    def test_body_sibling_edges_present(self) -> None:
        # body_1 gets n from body_0
        self.assertEqual(
            self.whl.edges[frs.TargetHandle(node="body_1", port="n")],
            frs.SourceHandle(node="body_0", port="n"),
        )
        # body_2 gets n from body_1
        self.assertEqual(
            self.whl.edges[frs.TargetHandle(node="body_2", port="n")],
            frs.SourceHandle(node="body_1", port="n"),
        )

    def test_condition_sibling_edges_present(self) -> None:
        # condition_1 gets n from body_0
        self.assertEqual(
            self.whl.edges[frs.TargetHandle(node="condition_1", port="n")],
            frs.SourceHandle(node="body_0", port="n"),
        )
        # condition_3 gets n from body_2
        self.assertEqual(
            self.whl.edges[frs.TargetHandle(node="condition_3", port="n")],
            frs.SourceHandle(node="body_2", port="n"),
        )


# --------------------------------------------------------------------------- #
# evaluate — macro-wrapped while                                              #
# --------------------------------------------------------------------------- #


class TestMacroWrappedWhile(unittest.TestCase):
    def setUp(self) -> None:
        self.node = _fixtures.while_countdown_node()
        self.run = self.node.run(n=3)
        self.whl = self.node.nodes.while_0

    def test_output_matches_python_countdown(self) -> None:
        self.assertEqual(self.run.outputs["n"].value, _fixtures.while_countdown(n=3))

    def test_inner_while_steps_are_wired(self) -> None:
        inner_steps = self.whl.current_run.steps
        labels = [step.label for step in inner_steps]
        self.assertEqual(
            labels,
            [
                "condition_0",
                "body_0",
                "condition_1",
                "body_1",
                "condition_2",
                "body_2",
                "condition_3",
            ],
        )


# --------------------------------------------------------------------------- #
# Internal helper: _stage_child_edges                                         #
# --------------------------------------------------------------------------- #


class TestStageChildEdges(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.while_recipe()
        self.whl = whileflow.While("whl", self.recipe)
        self.result = self.whl.generate_flowrep_live_node()

    def test_first_call_routes_all_to_input_edges(self) -> None:
        whileflow.While._stage_child_edges(
            "condition_0", "condition", self.recipe, self.result, None
        )
        self.assertIn(
            frs.TargetHandle(node="condition_0", port="n"), self.result.input_edges
        )
        self.assertEqual(self.result.edges, {})

    def test_subsequent_call_routes_looping_port_to_sibling_edge(self) -> None:
        whileflow.While._stage_child_edges(
            "condition_1", "condition", self.recipe, self.result, "body_0"
        )
        self.assertIn(frs.TargetHandle(node="condition_1", port="n"), self.result.edges)
        self.assertEqual(
            self.result.edges[frs.TargetHandle(node="condition_1", port="n")],
            frs.SourceHandle(node="body_0", port="n"),
        )
        self.assertNotIn(
            frs.TargetHandle(node="condition_1", port="n"), self.result.input_edges
        )

    def test_non_looping_port_always_from_input_edges(self) -> None:
        recipe = _non_looping_recipe()
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()

        # Second call (last_body_label set): "step" is not a while output,
        # so it must still land in input_edges.
        whileflow.While._stage_child_edges("body_1", "body", recipe, result, "body_0")
        self.assertIn(frs.TargetHandle(node="body_1", port="step"), result.input_edges)
        self.assertEqual(
            result.input_edges[frs.TargetHandle(node="body_1", port="step")],
            frs.InputSource(port="step"),
        )
        # "n" is a while output so it routes via sibling edge
        self.assertIn(frs.TargetHandle(node="body_1", port="n"), result.edges)


# --------------------------------------------------------------------------- #
# Internal helper: _stage_final_output_edges                                  #
# --------------------------------------------------------------------------- #


class TestStageFinalOutputEdges(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.while_recipe()
        self.whl = whileflow.While("whl", self.recipe)

    def test_no_iterations_produces_input_source_fallback(self) -> None:
        result = self.whl.generate_flowrep_live_node()
        whileflow.While._stage_final_output_edges(result, self.recipe, None)
        self.assertEqual(
            result.output_edges[frs.OutputTarget(port="n")],
            frs.InputSource(port="n"),
        )

    def test_iterations_produce_source_handle_from_last_body(self) -> None:
        result = self.whl.generate_flowrep_live_node()
        whileflow.While._stage_final_output_edges(result, self.recipe, "body_2")
        self.assertEqual(
            result.output_edges[frs.OutputTarget(port="n")],
            frs.SourceHandle(node="body_2", port="n"),
        )


# --------------------------------------------------------------------------- #
# Internal helper: _condition_value                                           #
# --------------------------------------------------------------------------- #


class TestConditionValue(unittest.TestCase):
    def _make_result_with_cond(
        self, cond_label: str, output_val: object
    ) -> frs.LiveWhile:
        recipe = _fixtures.while_recipe()
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()
        cond_live = frs.LiveAtomic.from_recipe(recipe.case.condition.node)
        cond_live.output_ports["output_0"].value = output_val
        result.nodes[cond_label] = cond_live
        return result, recipe

    def test_truthy_value_returns_true(self) -> None:
        result, recipe = self._make_result_with_cond("condition_0", 7)
        self.assertTrue(
            whileflow.While._condition_value("condition_0", recipe.case, result)
        )

    def test_falsy_value_returns_false(self) -> None:
        result, recipe = self._make_result_with_cond("condition_0", 0)
        self.assertFalse(
            whileflow.While._condition_value("condition_0", recipe.case, result)
        )

    def test_explicit_condition_output_label(self) -> None:
        cond_recipe = _fixtures.add.flowrep_recipe
        body_recipe = _fixtures.identity.flowrep_recipe
        case = frs.ConditionalCase(
            condition=frs.LabeledNode(label="condition", node=cond_recipe),
            body=frs.LabeledNode(label="body", node=body_recipe),
            condition_output="output_0",
        )
        recipe = frs.WhileNode(
            inputs=["x", "y"],
            outputs=["x"],
            case=case,
            input_edges={
                frs.TargetHandle(node="condition", port="x"): frs.InputSource(port="x"),
                frs.TargetHandle(node="condition", port="y"): frs.InputSource(port="y"),
                frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            },
            output_edges={
                frs.OutputTarget(port="x"): frs.SourceHandle(
                    node="body", port="x"  # identity outputs "x" (named return)
                ),
            },
        )
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()
        cond_live = frs.LiveAtomic.from_recipe(cond_recipe)
        cond_live.output_ports["output_0"].value = 0  # falsy via explicit label
        result.nodes["condition_0"] = cond_live
        self.assertFalse(whileflow.While._condition_value("condition_0", case, result))


# --------------------------------------------------------------------------- #
# Non-looping inputs                                                          #
# --------------------------------------------------------------------------- #


class TestNonLoopingInputs(unittest.TestCase):
    """
    `step` is a while input but not a while output; it must always be sourced
    from the parent input port, not from a previous body iteration.
    """

    def setUp(self) -> None:
        self.recipe = _non_looping_recipe()
        self.whl = whileflow.While("whl", self.recipe)
        # n=3, step=1 → 3-1=2, 2-1=1, 1-1=0 → terminates
        self.run = self.whl.run(n=3, step=1)

    def test_output_value(self) -> None:
        self.assertEqual(self.run.outputs["n"].value, 0)

    def test_step_always_sourced_from_input(self) -> None:
        for key, val in self.whl.input_edges.items():
            if key.port == "step":
                self.assertEqual(val, frs.InputSource(port="step"))
        # No sibling edge for "step"
        for key in self.whl.edges:
            self.assertNotEqual(key.port, "step")

    def test_terminates_correctly(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)


# --------------------------------------------------------------------------- #
# Internal helper: _indexed_label                                             #
# --------------------------------------------------------------------------- #


class TestIndexedLabel(unittest.TestCase):
    def test_body_index_zero(self) -> None:
        self.assertEqual(whileflow.While._indexed_label("body", 0), "body_0")

    def test_condition_index_five(self) -> None:
        self.assertEqual(whileflow.While._indexed_label("condition", 5), "condition_5")

    def test_arbitrary_prefix_and_index(self) -> None:
        self.assertEqual(whileflow.While._indexed_label("my_node", 42), "my_node_42")


if __name__ == "__main__":
    unittest.main()
