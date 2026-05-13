from __future__ import annotations

import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import constructors, execution
from pyiron_workflow._wfms.flowcontrollers import ifflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _no_else_recipe() -> frs.IfNode:
    """`if add(x, y): add(x, y) else: <nothing>` — single case, no `else`."""
    add_recipe = _fixtures.add.flowrep_recipe
    return frs.IfNode(
        inputs=["x", "y"],
        outputs=["out"],
        cases=[
            frs.ConditionalCase(
                condition=frs.LabeledNode(label="cond", node=add_recipe),
                body=frs.LabeledNode(label="body", node=add_recipe),
            )
        ],
        input_edges={
            frs.TargetHandle(node="cond", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="cond", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="out"): [
                frs.SourceHandle(node="body", port="output_0")
            ],
        },
    )


def _two_case_recipe(with_else: bool) -> frs.IfNode:
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
        frs.ConditionalCase(
            condition=frs.LabeledNode(label="cond_pos", node=pos_recipe),
            body=frs.LabeledNode(label="body_pos", node=identity_recipe),
        ),
        frs.ConditionalCase(
            condition=frs.LabeledNode(label="cond_neg", node=neg_recipe),
            body=frs.LabeledNode(label="body_neg", node=negate_recipe),
        ),
    ]
    input_edges: dict[frs.TargetHandle, frs.InputSource] = {
        frs.TargetHandle(node="cond_pos", port="n"): frs.InputSource(port="x"),
        frs.TargetHandle(node="cond_neg", port="n"): frs.InputSource(port="x"),
        frs.TargetHandle(node="body_pos", port="x"): frs.InputSource(port="x"),
        frs.TargetHandle(node="body_neg", port="x"): frs.InputSource(port="x"),
    }
    # `identity` returns the input variable; the parsed recipe names that
    # output port after the return-name (`x`). `negate` uses a unary op
    # and falls back to `output_0`.
    prospective_output_edges = {
        frs.OutputTarget(port="out"): [
            frs.SourceHandle(node="body_pos", port="x"),
            frs.SourceHandle(node="body_neg", port="output_0"),
        ],
    }
    else_case = None
    if with_else:
        input_edges[frs.TargetHandle(node="else_body", port="x")] = frs.InputSource(
            port="x"
        )
        prospective_output_edges[frs.OutputTarget(port="out")].append(
            frs.SourceHandle(node="else_body", port="x")
        )
        else_case = frs.LabeledNode(label="else_body", node=identity_recipe)

    return frs.IfNode(
        inputs=["x"],
        outputs=["out"],
        cases=cases,
        else_case=else_case,
        input_edges=input_edges,
        prospective_output_edges=prospective_output_edges,
    )


def _macro_with_no_else_and_downstream() -> frs.WorkflowNode:
    """
    Macro: `if add(x, y): add(x, y)` then a downstream `identity` sibling.

    The macro's exported output is wired from the `IfNode`'s `out` port
    (which stays `NOT_DATA` when no case fires), not from the downstream
    sibling — so :func:`populate_outputs` does not hit the skipped node.
    """
    identity_recipe = _fixtures.identity.flowrep_recipe
    return frs.WorkflowNode(
        inputs=["x", "y"],
        outputs=["z"],
        nodes={"if_0": _no_else_recipe(), "downstream": identity_recipe},
        input_edges={
            frs.TargetHandle(node="if_0", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="if_0", port="y"): frs.InputSource(port="y"),
        },
        edges={
            frs.TargetHandle(node="downstream", port="x"): frs.SourceHandle(
                node="if_0", port="out"
            ),
        },
        output_edges={
            frs.OutputTarget(port="z"): frs.SourceHandle(node="if_0", port="out"),
        },
    )


# --------------------------------------------------------------------------- #
# Prospective + retrospective surface                                         #
# --------------------------------------------------------------------------- #


class TestIfProspectiveAndRetrospective(unittest.TestCase):
    """Pre-run vs post-run views of the If in the `if_abs` fixture."""

    def setUp(self) -> None:
        self.node = _fixtures.if_abs_node()
        self.ifn = self.node.nodes.if_0

    def test_prospective_input_edges_matches_recipe(self) -> None:
        self.assertEqual(self.ifn.prospective_input_edges, self.ifn.recipe.input_edges)
        self.assertGreater(len(self.ifn.prospective_input_edges), 0)

    def test_prospective_edges_is_empty(self) -> None:
        self.assertEqual(self.ifn.prospective_edges, {})

    def test_prospective_output_edges_matches_recipe(self) -> None:
        self.assertEqual(
            self.ifn.prospective_output_edges,
            self.ifn.recipe.prospective_output_edges,
        )
        self.assertGreater(len(self.ifn.prospective_output_edges), 0)

    def test_prospective_nodes_has_all_case_and_else_nodes(self) -> None:
        # 1 case → cond + body, plus the else body.
        self.assertEqual(
            set(self.ifn.prospective_nodes), {"condition_0", "body_0", "else_body"}
        )

    def test_pre_run_retrospective_views_empty(self) -> None:
        self.assertEqual(self.ifn.input_edges, {})
        self.assertEqual(self.ifn.edges, {})
        self.assertEqual(self.ifn.output_edges, {})
        self.assertEqual(len(self.ifn.nodes), 0)

    def test_post_run_views(self) -> None:
        prospective_input_before = self.ifn.prospective_input_edges
        prospective_edges_before = self.ifn.prospective_edges
        prospective_output_before = self.ifn.prospective_output_edges
        prospective_nodes_before = list(self.ifn.prospective_nodes)

        self.node.run(x=5)

        self.assertEqual(self.ifn.prospective_input_edges, prospective_input_before)
        self.assertEqual(self.ifn.prospective_edges, prospective_edges_before)
        self.assertEqual(self.ifn.prospective_output_edges, prospective_output_before)
        self.assertEqual(list(self.ifn.prospective_nodes), prospective_nodes_before)

        self.assertGreater(len(self.ifn.input_edges), 0)
        self.assertEqual(self.ifn.edges, {})
        self.assertGreater(len(self.ifn.output_edges), 0)
        # condition + body ran (else_body did not)
        self.assertEqual(set(self.ifn.nodes), {"condition_0", "body_0"})


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
        self.assertEqual(self.run.outputs["out"].value, 3)

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
        self.assertIsInstance(self.run.outputs["out"].value, frs.NotData)

    def test_only_condition_ran(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["cond"])

    def test_no_output_edges_recorded(self) -> None:
        self.assertEqual(self.ifn.output_edges, {})


class TestEvaluateSingleCaseFalseWithElse(unittest.TestCase):
    def setUp(self) -> None:
        self.node = _fixtures.if_abs_node()
        # x=-5 → is_positive=False → else fires → negate(-5)=5.
        self.run = self.node.run(x=-5)
        self.ifn = self.node.nodes.if_0

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_else_body_output_routed(self) -> None:
        self.assertEqual(self.run.outputs["y"].value, 5)

    def test_steps_visit_condition_then_else_body(self) -> None:
        labels = [step.label for step in self.ifn.current_run.steps]
        self.assertEqual(labels, ["condition_0", "else_body"])

    def test_retrospective_excludes_body_0(self) -> None:
        self.assertEqual(set(self.ifn.nodes), {"condition_0", "else_body"})


# --------------------------------------------------------------------------- #
# evaluate — multiple cases                                                   #
# --------------------------------------------------------------------------- #


class TestEvaluateMultipleCases(unittest.TestCase):
    def test_first_true_wins_and_short_circuits(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=5: is_positive(5) → True, identity(5) → 5. cond_neg must not run.
        run = ifn.run(x=5)
        self.assertEqual(run.outputs["out"].value, 5)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "body_pos"])

    def test_second_case_fires_when_first_false(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=-5: cond_pos False, cond_neg True, negate(-5)=5.
        run = ifn.run(x=-5)
        self.assertEqual(run.outputs["out"].value, 5)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg", "body_neg"])

    def test_all_false_falls_to_else(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=True))
        # x=0: both predicates False, else returns identity(0)=0.
        run = ifn.run(x=0)
        self.assertEqual(run.outputs["out"].value, 0)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg", "else_body"])

    def test_all_false_no_else_leaves_not_data(self) -> None:
        ifn = ifflow.If("ifn", _two_case_recipe(with_else=False))
        run = ifn.run(x=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertIsInstance(run.outputs["out"].value, frs.NotData)
        labels = [step.label for step in run.steps]
        self.assertEqual(labels, ["cond_pos", "cond_neg"])
        self.assertEqual(ifn.output_edges, {})


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
        self.macro = constructors.recipe2static("macro", self.recipe)
        # cond = add(1, -1) = 0 (falsy); no else → if_0.out stays NOT_DATA.
        self.run = self.macro.run(x=1, y=-1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_macro_output_is_not_data(self) -> None:
        self.assertIsInstance(self.run.outputs["z"].value, frs.NotData)

    def test_downstream_step_not_recorded(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["if_0"])
        substep_labels = [step.label for step in self.run.steps[0].run.steps]
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
                frs.TargetHandle(node="cond", port="x"),
                frs.TargetHandle(node="cond", port="y"),
            },
        )

    def test_subsequent_call_extends_in_place(self) -> None:
        ifflow.If._stage_node_input_edges("cond", self.live, self.recipe)
        ifflow.If._stage_node_input_edges("body", self.live, self.recipe)
        self.assertEqual(len(self.live.input_edges), 4)
        self.assertIn(frs.TargetHandle(node="body", port="x"), self.live.input_edges)


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
                frs.OutputTarget(port="out"): frs.SourceHandle(
                    node="body_pos", port="x"
                ),
            },
        )

    def test_picks_else_body_handle(self) -> None:
        ifflow.If._stage_body_output_edges("else_body", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges[frs.OutputTarget(port="out")],
            frs.SourceHandle(node="else_body", port="x"),
        )

    def test_skips_outputs_without_matching_body(self) -> None:
        unrelated_body_recipe = frs.IfNode(
            inputs=["x"],
            outputs=["out"],
            cases=[
                frs.ConditionalCase(
                    condition=frs.LabeledNode(
                        label="cond", node=_fixtures.is_positive.flowrep_recipe
                    ),
                    body=frs.LabeledNode(
                        label="body_a", node=_fixtures.identity.flowrep_recipe
                    ),
                ),
                frs.ConditionalCase(
                    condition=frs.LabeledNode(
                        label="cond2", node=_fixtures.is_negative.flowrep_recipe
                    ),
                    body=frs.LabeledNode(
                        label="body_b", node=_fixtures.identity.flowrep_recipe
                    ),
                ),
            ],
            input_edges={
                frs.TargetHandle(node="cond", port="n"): frs.InputSource(port="x"),
                frs.TargetHandle(node="cond2", port="n"): frs.InputSource(port="x"),
                frs.TargetHandle(node="body_a", port="x"): frs.InputSource(port="x"),
                frs.TargetHandle(node="body_b", port="x"): frs.InputSource(port="x"),
            },
            prospective_output_edges={
                frs.OutputTarget(port="out"): [
                    frs.SourceHandle(node="body_a", port="x"),
                    frs.SourceHandle(node="body_b", port="x"),
                ],
            },
        )
        ifn = ifflow.If("ifn", unrelated_body_recipe)
        live = ifn.generate_flowrep_live_node()
        ifflow.If._stage_body_output_edges("body_a", live, unrelated_body_recipe)
        self.assertEqual(
            live.output_edges[frs.OutputTarget(port="out")],
            frs.SourceHandle(node="body_a", port="x"),
        )


class TestConditionValue(unittest.TestCase):
    def test_falls_back_to_sole_output_label(self) -> None:
        recipe = _fixtures.if_recipe()
        ifn = ifflow.If("ifn", recipe)
        live = ifn.generate_flowrep_live_node()
        case = recipe.cases[0]
        cond_live = frs.LiveAtomic.from_recipe(case.condition.node)
        cond_live.output_ports["output_0"].value = 7  # truthy
        live.nodes[case.condition.label] = cond_live
        self.assertTrue(ifflow.If._condition_value(case, live))

    def test_uses_condition_output_when_set(self) -> None:
        cond_recipe = _fixtures.add.flowrep_recipe
        body_recipe = _fixtures.identity.flowrep_recipe
        case = frs.ConditionalCase(
            condition=frs.LabeledNode(label="cond", node=cond_recipe),
            body=frs.LabeledNode(label="body", node=body_recipe),
            condition_output="output_0",
        )
        recipe = frs.IfNode(
            inputs=["x", "y"],
            outputs=["out"],
            cases=[case],
            input_edges={
                frs.TargetHandle(node="cond", port="x"): frs.InputSource(port="x"),
                frs.TargetHandle(node="cond", port="y"): frs.InputSource(port="y"),
                frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            },
            prospective_output_edges={
                frs.OutputTarget(port="out"): [frs.SourceHandle(node="body", port="x")],
            },
        )
        ifn = ifflow.If("ifn", recipe)
        live = ifn.generate_flowrep_live_node()
        cond_live = frs.LiveAtomic.from_recipe(cond_recipe)
        cond_live.output_ports["output_0"].value = 0  # falsy via explicit label
        live.nodes["cond"] = cond_live
        self.assertFalse(ifflow.If._condition_value(case, live))


if __name__ == "__main__":
    unittest.main()
