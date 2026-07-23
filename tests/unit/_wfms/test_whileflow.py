from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.flowcontrollers import whileflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _non_looping_recipe() -> fr.schemas.WhileRecipe:
    """
    WhileNode with inputs=["n", "step"] but outputs=["n"] only.

    `step` is never a while-loop output, so it must always be sourced from the
    parent input, never from a previous body iteration.  The body subtracts
    `step` from `n`.
    """
    sub_recipe = _fixtures.sub.flowrep_recipe  # sub(x, y) → output_0
    body = fr.schemas.WorkflowRecipe(
        inputs=["n", "step"],
        outputs=["n"],
        nodes={"sub_0": sub_recipe},
        input_edges={
            fr.schemas.TargetHandle(node="sub_0", port="x"): fr.schemas.InputSource(
                port="n"
            ),
            fr.schemas.TargetHandle(node="sub_0", port="y"): fr.schemas.InputSource(
                port="step"
            ),
        },
        edges={},
        output_edges={
            fr.schemas.OutputTarget(port="n"): fr.schemas.SourceHandle(
                node="sub_0", port="output_0"
            ),
        },
    )
    return fr.schemas.WhileRecipe(
        inputs=["n", "step"],
        outputs=["n"],
        case=fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(
                label="condition", recipe=_fixtures.is_positive.flowrep_recipe
            ),
            body=fr.schemas.LabeledRecipe(label="body", recipe=body),
        ),
        input_edges={
            fr.schemas.TargetHandle(node="condition", port="n"): fr.schemas.InputSource(
                port="n"
            ),
            fr.schemas.TargetHandle(node="body", port="n"): fr.schemas.InputSource(
                port="n"
            ),
            fr.schemas.TargetHandle(node="body", port="step"): fr.schemas.InputSource(
                port="step"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="n"): fr.schemas.SourceHandle(
                node="body", port="n"
            ),
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
        self.assertEqual(self.run.outputs.n, 0)

    def test_only_condition_ran(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["condition_0"])

    def test_output_edge_is_input_source_fallback(self) -> None:
        self.assertEqual(
            self.run.result.output_edges[fr.schemas.OutputTarget(port="n")],
            fr.schemas.InputSource(port="n"),
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
        self.assertEqual(self.run.outputs.n, 0)

    def test_steps_order(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["condition_0", "body_0", "condition_1"])

    def test_output_edge_points_to_last_body(self) -> None:
        self.assertEqual(
            self.run.result.output_edges[fr.schemas.OutputTarget(port="n")],
            fr.schemas.SourceHandle(node="body_0", port="n"),
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
        self.assertEqual(self.run.outputs.n, 0)

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
            set(self.run.result.nodes),
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
            self.run.result.edges[fr.schemas.TargetHandle(node="body_1", port="n")],
            fr.schemas.SourceHandle(node="body_0", port="n"),
        )
        # body_2 gets n from body_1
        self.assertEqual(
            self.run.result.edges[fr.schemas.TargetHandle(node="body_2", port="n")],
            fr.schemas.SourceHandle(node="body_1", port="n"),
        )

    def test_condition_sibling_edges_present(self) -> None:
        # condition_1 gets n from body_0
        self.assertEqual(
            self.run.result.edges[
                fr.schemas.TargetHandle(node="condition_1", port="n")
            ],
            fr.schemas.SourceHandle(node="body_0", port="n"),
        )
        # condition_3 gets n from body_2
        self.assertEqual(
            self.run.result.edges[
                fr.schemas.TargetHandle(node="condition_3", port="n")
            ],
            fr.schemas.SourceHandle(node="body_2", port="n"),
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
        self.assertEqual(self.run.outputs.n, _fixtures.while_countdown(n=3))

    def test_inner_while_steps_are_wired(self) -> None:
        inner_steps = self.run.steps[0].steps
        self.assertEqual(
            inner_steps.labels,
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
            fr.schemas.TargetHandle(node="condition_0", port="n"),
            self.result.input_edges,
        )
        self.assertEqual(self.result.edges, {})

    def test_subsequent_call_routes_looping_port_to_sibling_edge(self) -> None:
        whileflow.While._stage_child_edges(
            "condition_1", "condition", self.recipe, self.result, "body_0"
        )
        self.assertIn(
            fr.schemas.TargetHandle(node="condition_1", port="n"), self.result.edges
        )
        self.assertEqual(
            self.result.edges[fr.schemas.TargetHandle(node="condition_1", port="n")],
            fr.schemas.SourceHandle(node="body_0", port="n"),
        )
        self.assertNotIn(
            fr.schemas.TargetHandle(node="condition_1", port="n"),
            self.result.input_edges,
        )

    def test_non_looping_port_always_from_input_edges(self) -> None:
        recipe = _non_looping_recipe()
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()

        # Second call (last_body_label set): "step" is not a while output,
        # so it must still land in input_edges.
        whileflow.While._stage_child_edges("body_1", "body", recipe, result, "body_0")
        self.assertIn(
            fr.schemas.TargetHandle(node="body_1", port="step"), result.input_edges
        )
        self.assertEqual(
            result.input_edges[fr.schemas.TargetHandle(node="body_1", port="step")],
            fr.schemas.InputSource(port="step"),
        )
        # "n" is a while output so it routes via sibling edge
        self.assertIn(fr.schemas.TargetHandle(node="body_1", port="n"), result.edges)


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
            result.output_edges[fr.schemas.OutputTarget(port="n")],
            fr.schemas.InputSource(port="n"),
        )

    def test_iterations_produce_source_handle_from_last_body(self) -> None:
        result = self.whl.generate_flowrep_live_node()
        whileflow.While._stage_final_output_edges(result, self.recipe, "body_2")
        self.assertEqual(
            result.output_edges[fr.schemas.OutputTarget(port="n")],
            fr.schemas.SourceHandle(node="body_2", port="n"),
        )


# --------------------------------------------------------------------------- #
# Internal helper: _condition_value                                           #
# --------------------------------------------------------------------------- #


class TestConditionValue(unittest.TestCase):
    def _make_result_with_cond(
        self, cond_label: str, output_val: object
    ) -> fr.schemas.WhileData:
        recipe = _fixtures.while_recipe()
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()
        cond_live = fr.schemas.AtomicData.from_recipe(recipe.case.condition.recipe)
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
        case = fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(label="condition", recipe=cond_recipe),
            body=fr.schemas.LabeledRecipe(label="body", recipe=body_recipe),
            condition_output="output_0",
        )
        recipe = fr.schemas.WhileRecipe(
            inputs=["x", "y"],
            outputs=["x"],
            case=case,
            input_edges={
                fr.schemas.TargetHandle(
                    node="condition", port="x"
                ): fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(
                    node="condition", port="y"
                ): fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                    port="x"
                ),
            },
            output_edges={
                fr.schemas.OutputTarget(port="x"): fr.schemas.SourceHandle(
                    node="body", port="x"  # identity outputs "x" (named return)
                ),
            },
        )
        whl = whileflow.While("whl", recipe)
        result = whl.generate_flowrep_live_node()
        cond_live = fr.schemas.AtomicData.from_recipe(cond_recipe)
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
        self.assertEqual(self.run.outputs.n, 0)

    def test_step_always_sourced_from_input(self) -> None:
        for key, val in self.run.result.input_edges.items():
            if key.port == "step":
                self.assertEqual(val, fr.schemas.InputSource(port="step"))
        # No sibling edge for "step"
        for key in self.run.result.edges:
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
