from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import constructors, execution
from pyiron_workflow._wfms.flowcontrollers import ifflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _no_else_recipe() -> fr.schemas.IfRecipe:
    """`if add(x, y): add(x, y) else: <nothing>` — single case, no `else`."""
    add_recipe = _fixtures.add.flowrep_recipe
    return fr.schemas.IfRecipe(
        inputs=["x", "y"],
        outputs=["out"],
        cases=[
            fr.schemas.ConditionalCase(
                condition=fr.schemas.LabeledRecipe(label="cond", recipe=add_recipe),
                body=fr.schemas.LabeledRecipe(label="body", recipe=add_recipe),
            )
        ],
        input_edges={
            fr.schemas.TargetHandle(node="cond", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="cond", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="out"): [
                fr.schemas.SourceHandle(node="body", port="output_0")
            ],
        },
    )


def _two_case_recipe(with_else: bool) -> fr.schemas.IfRecipe:
    """Two-case If: `is_positive(x) → identity(x); is_negative(x) → negate(x)`.

    With `with_else=True` the else branch returns `identity(x)` (so `x=0`
    routes there). With `with_else=False` `x=0` leaves the output as
    `NOT_DATA`.
    """
    pos_recipe = _fixtures.is_positive.flowrep_recipe
    neg_recipe = _fixtures.is_negative.flowrep_recipe
    identity_recipe = _fixtures.identity.flowrep_recipe
    negate_recipe = _fixtures.negate.flowrep_recipe

    cases = [
        fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(label="cond_pos", recipe=pos_recipe),
            body=fr.schemas.LabeledRecipe(label="body_pos", recipe=identity_recipe),
        ),
        fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(label="cond_neg", recipe=neg_recipe),
            body=fr.schemas.LabeledRecipe(label="body_neg", recipe=negate_recipe),
        ),
    ]
    input_edges: dict[fr.schemas.TargetHandle, fr.schemas.InputSource] = {
        fr.schemas.TargetHandle(node="cond_pos", port="n"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="cond_neg", port="n"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="body_pos", port="x"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="body_neg", port="x"): fr.schemas.InputSource(
            port="x"
        ),
    }
    # `identity` returns the input variable; the parsed recipe names that
    # output port after the return-name (`x`). `negate` uses a unary op
    # and falls back to `output_0`.
    prospective_output_edges = {
        fr.schemas.OutputTarget(port="out"): [
            fr.schemas.SourceHandle(node="body_pos", port="x"),
            fr.schemas.SourceHandle(node="body_neg", port="output_0"),
        ],
    }
    else_case = None
    if with_else:
        input_edges[fr.schemas.TargetHandle(node="else_body", port="x")] = (
            fr.schemas.InputSource(port="x")
        )
        prospective_output_edges[fr.schemas.OutputTarget(port="out")].append(
            fr.schemas.SourceHandle(node="else_body", port="x")
        )
        else_case = fr.schemas.LabeledRecipe(label="else_body", recipe=identity_recipe)

    return fr.schemas.IfRecipe(
        inputs=["x"],
        outputs=["out"],
        cases=cases,
        else_case=else_case,
        input_edges=input_edges,
        prospective_output_edges=prospective_output_edges,
    )


def _macro_with_no_else_and_downstream() -> fr.schemas.WorkflowRecipe:
    """
    Macro: `if add(x, y): add(x, y)` then a downstream `identity` sibling.

    The macro's exported output is wired from the `IfNode`'s `out` port
    (which stays `NOT_DATA` when no case fires), not from the downstream
    sibling — so :func:`populate_outputs` does not hit the skipped node.
    """
    identity_recipe = _fixtures.identity.flowrep_recipe
    return fr.schemas.WorkflowRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        nodes={"if_0": _no_else_recipe(), "downstream": identity_recipe},
        input_edges={
            fr.schemas.TargetHandle(node="if_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="if_0", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        edges={
            fr.schemas.TargetHandle(
                node="downstream", port="x"
            ): fr.schemas.SourceHandle(node="if_0", port="out"),
        },
        output_edges={
            fr.schemas.OutputTarget(port="z"): fr.schemas.SourceHandle(
                node="if_0", port="out"
            ),
        },
    )


# --------------------------------------------------------------------------- #
# evaluate — single case                                                      #
# --------------------------------------------------------------------------- #


class TestEvaluateSingleCaseTrue(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.if_recipe()
        self.ifn = ifflow.If("ifn", self.recipe)
        # cond and body both wrap `add`; with x=1, y=2 the condition returns
        # 3 (truthy) and the body returns 3.
        self.run = self.ifn.run(x=1, y=2)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_body_output_routed_to_parent(self) -> None:
        self.assertEqual(self.run.outputs.out, 3)

    def test_steps_in_run_order(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["cond", "body"])

    def test_retrospective_nodes_only_what_ran(self) -> None:
        self.assertEqual(set(self.ifn.nodes), {"cond", "body"})


class TestEvaluateSingleCaseFalseNoElse(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _no_else_recipe()
        self.ifn = ifflow.If("ifn", self.recipe)
        # x=1, y=-1 → add → 0 (falsy); no else → output stays NOT_DATA.
        self.run = self.ifn.run(x=1, y=-1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_stays_not_data(self) -> None:
        self.assertIsInstance(self.run.outputs.out, fr.schemas.NotData)

    def test_only_condition_ran(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["cond"])

    def test_no_output_edges_recorded(self) -> None:
        self.assertEqual(self.run.result.output_edges, {})


class TestEvaluateSingleCaseFalseWithElse(unittest.TestCase):
    def setUp(self) -> None:
        self.node = _fixtures.if_abs_node()
        # x=-5 → is_positive=False → else fires → negate(-5)=5.
        self.run = self.node.run(x=-5)
        self.ifn = self.node.nodes.if_0
        self.if_step = self.run.steps[0]

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_else_body_output_routed(self) -> None:
        self.assertEqual(self.run.outputs.y, 5)

    def test_steps_visit_condition_then_else_body(self) -> None:
        labels = [step.label for step in self.if_step.steps]
        self.assertEqual(labels, ["condition_0", "else_body"])

    def test_retrospective_excludes_body_0(self) -> None:
        self.assertEqual(
            set(self.run.result.nodes["if_0"].nodes), {"condition_0", "else_body"}
        )


# --------------------------------------------------------------------------- #
# evaluate — multiple cases                                                   #
# --------------------------------------------------------------------------- #


class TestEvaluateMultipleCases(unittest.TestCase):
    def test_first_true_wins_and_short_circuits(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=5: is_positive(5) → True, identity(5) → 5. cond_neg must not run.
        run = ifn.run(x=5)
        self.assertEqual(run.outputs.out, 5)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "body_pos"])

    def test_second_case_fires_when_first_false(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=-5: cond_pos False, cond_neg True, negate(-5)=5.
        run = ifn.run(x=-5)
        self.assertEqual(run.outputs.out, 5)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg", "body_neg"])

    def test_all_false_falls_to_else(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=0: both predicates False, else returns identity(0)=0.
        run = ifn.run(x=0)
        self.assertEqual(run.outputs.out, 0)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg", "else_body"])

    def test_all_false_no_else_leaves_not_data(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=False))
        run = ifn.run(x=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertIsInstance(run.outputs.out, fr.schemas.NotData)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg"])
        self.assertEqual(run.result.output_edges, {})


# --------------------------------------------------------------------------- #
# Macro wrapping an If — downstream skip when no case fires                   #
# --------------------------------------------------------------------------- #


class TestMacroDownstreamOfFalsyIfIsSkipped(unittest.TestCase):
    """
    A sibling consuming an unfired `IfNode`'s output is skipped.

    The behaviour under test lives in `dag.evaluate_dag_by_layer`: when a
    node's gathered inputs contain `NOT_DATA` (because an upstream
    conditional did not fire), the node is silently skipped instead of being
    executed against the sentinel.
    """

    def setUp(self) -> None:
        self.recipe = _macro_with_no_else_and_downstream()
        self.macro = constructors.recipe2node(self.recipe, "macro")
        # cond = add(1, -1) = 0 (falsy); no else → if_0.out stays NOT_DATA.
        self.run = self.macro.run(x=1, y=-1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_macro_output_is_not_data(self) -> None:
        self.assertIsInstance(self.run.outputs.z, fr.schemas.NotData)

    def test_downstream_step_not_recorded(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["if_0"])
        substep_labels = self.run.steps[0].steps.labels
        self.assertEqual(substep_labels, ["cond"])


# --------------------------------------------------------------------------- #
# Internal helper unit tests                                                  #
# --------------------------------------------------------------------------- #


class TestStageNodeInputEdges(unittest.TestCase):
    """`_stage_node_input_edges` filters by target node."""

    def setUp(self) -> None:
        self.recipe = _fixtures.if_recipe()
        self.ifn = ifflow.If("ifn", self.recipe)
        self.live = self.ifn.generate_flowrep_live_node()

    def test_only_matching_target_node_edges_copied(self) -> None:
        ifflow.If._stage_node_input_edges("cond", self.live, self.recipe)
        self.assertEqual(
            set(self.live.input_edges),
            {
                fr.schemas.TargetHandle(node="cond", port="x"),
                fr.schemas.TargetHandle(node="cond", port="y"),
            },
        )

    def test_subsequent_call_extends_in_place(self) -> None:
        ifflow.If._stage_node_input_edges("cond", self.live, self.recipe)
        ifflow.If._stage_node_input_edges("body", self.live, self.recipe)
        self.assertEqual(len(self.live.input_edges), 4)
        self.assertIn(
            fr.schemas.TargetHandle(node="body", port="x"), self.live.input_edges
        )


class TestStageBodyOutputEdges(unittest.TestCase):
    """`_stage_body_output_edges` picks the unique SourceHandle for the body."""

    def setUp(self) -> None:
        self.recipe = _two_case_recipe(with_else=True)
        self.ifn = ifflow.If("ifn", self.recipe)
        self.live = self.ifn.generate_flowrep_live_node()

    def test_picks_body_pos_handle(self) -> None:
        ifflow.If._stage_body_output_edges("body_pos", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges,
            {
                fr.schemas.OutputTarget(port="out"): fr.schemas.SourceHandle(
                    node="body_pos", port="x"
                ),
            },
        )

    def test_picks_else_body_handle(self) -> None:
        ifflow.If._stage_body_output_edges("else_body", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges[fr.schemas.OutputTarget(port="out")],
            fr.schemas.SourceHandle(node="else_body", port="x"),
        )

    def test_skips_outputs_without_matching_body(self) -> None:
        unrelated_body_recipe = fr.schemas.IfRecipe(
            inputs=["x"],
            outputs=["out"],
            cases=[
                fr.schemas.ConditionalCase(
                    condition=fr.schemas.LabeledRecipe(
                        label="cond", recipe=_fixtures.is_positive.flowrep_recipe
                    ),
                    body=fr.schemas.LabeledRecipe(
                        label="body_a", recipe=_fixtures.identity.flowrep_recipe
                    ),
                ),
                fr.schemas.ConditionalCase(
                    condition=fr.schemas.LabeledRecipe(
                        label="cond2", recipe=_fixtures.is_negative.flowrep_recipe
                    ),
                    body=fr.schemas.LabeledRecipe(
                        label="body_b", recipe=_fixtures.identity.flowrep_recipe
                    ),
                ),
            ],
            input_edges={
                fr.schemas.TargetHandle(node="cond", port="n"): fr.schemas.InputSource(
                    port="x"
                ),
                fr.schemas.TargetHandle(node="cond2", port="n"): fr.schemas.InputSource(
                    port="x"
                ),
                fr.schemas.TargetHandle(
                    node="body_a", port="x"
                ): fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(
                    node="body_b", port="x"
                ): fr.schemas.InputSource(port="x"),
            },
            prospective_output_edges={
                fr.schemas.OutputTarget(port="out"): [
                    fr.schemas.SourceHandle(node="body_a", port="x"),
                    fr.schemas.SourceHandle(node="body_b", port="x"),
                ],
            },
        )
        ifn = ifflow.If("ifn", unrelated_body_recipe)
        live = ifn.generate_flowrep_live_node()
        ifflow.If._stage_body_output_edges("body_a", live, unrelated_body_recipe)
        self.assertEqual(
            live.output_edges[fr.schemas.OutputTarget(port="out")],
            fr.schemas.SourceHandle(node="body_a", port="x"),
        )


class TestConditionValue(unittest.TestCase):
    def test_falls_back_to_sole_output_label(self) -> None:
        recipe = _fixtures.if_recipe()
        ifn = ifflow.If("ifn", recipe)
        live = ifn.generate_flowrep_live_node()
        case = recipe.cases[0]
        cond_live = fr.schemas.AtomicData.from_recipe(case.condition.recipe)
        cond_live.output_ports["output_0"].value = 7  # truthy
        live.nodes[case.condition.label] = cond_live
        self.assertTrue(ifflow.If._condition_value(case, live))

    def test_uses_condition_output_when_set(self) -> None:
        cond_recipe = _fixtures.add.flowrep_recipe
        body_recipe = _fixtures.identity.flowrep_recipe
        case = fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(label="cond", recipe=cond_recipe),
            body=fr.schemas.LabeledRecipe(label="body", recipe=body_recipe),
            condition_output="output_0",
        )
        recipe = fr.schemas.IfRecipe(
            inputs=["x", "y"],
            outputs=["out"],
            cases=[case],
            input_edges={
                fr.schemas.TargetHandle(node="cond", port="x"): fr.schemas.InputSource(
                    port="x"
                ),
                fr.schemas.TargetHandle(node="cond", port="y"): fr.schemas.InputSource(
                    port="y"
                ),
                fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                    port="x"
                ),
            },
            prospective_output_edges={
                fr.schemas.OutputTarget(port="out"): [
                    fr.schemas.SourceHandle(node="body", port="x")
                ],
            },
        )
        ifn = ifflow.If("ifn", recipe)
        live = ifn.generate_flowrep_live_node()
        cond_live = fr.schemas.AtomicData.from_recipe(cond_recipe)
        cond_live.output_ports["output_0"].value = 0  # falsy via explicit label
        live.nodes["cond"] = cond_live
        self.assertFalse(ifflow.If._condition_value(case, live))


if __name__ == "__main__":
    unittest.main()
