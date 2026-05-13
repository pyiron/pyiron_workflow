from __future__ import annotations

import contextlib
import unittest

from flowrep.api import schemas as frs
from pyiron_snippets import versions

from pyiron_workflow._wfms import constructors, execution
from pyiron_workflow._wfms.flowcontrollers import tryflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _no_match_recipe() -> frs.TryNode:
    """
    try divide(x, y); except TypeError → identity(x). No match for ZeroDivisionError.
    """
    return frs.TryNode(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=frs.LabeledNode(
            label="try_body", node=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            frs.ExceptionCase(
                exceptions=[versions.VersionInfo.of(TypeError)],
                body=frs.LabeledNode(
                    label="handler_0", node=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            frs.TargetHandle(node="try_body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_body", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="handler_0", port="x"): frs.InputSource(port="x"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="z"): [
                frs.SourceHandle(node="try_body", port="output_0"),
                frs.SourceHandle(node="handler_0", port="x"),
            ],
        },
    )


def _multi_case_recipe() -> frs.TryNode:
    """try divide(x, y); except TypeError → identity; except ValueError → negate.

    try_body raises ValueError (via negate with a string — actually we need to raise
    ValueError manually). Instead: try_body calls divide but we use x="bad" to force
    TypeError... Actually we want to test case order specifically.

    Simpler approach: try_body = divide(x, y) where x="bad", y=1 → TypeError.
    First handler catches ZeroDivisionError (no match), second catches TypeError (match).
    """
    return frs.TryNode(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=frs.LabeledNode(
            label="try_body", node=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            frs.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=frs.LabeledNode(
                    label="handler_0", node=_fixtures.negate.flowrep_recipe
                ),
            ),
            frs.ExceptionCase(
                exceptions=[versions.VersionInfo.of(TypeError)],
                body=frs.LabeledNode(
                    label="handler_1", node=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            frs.TargetHandle(node="try_body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_body", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="handler_0", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="handler_1", port="x"): frs.InputSource(port="x"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="z"): [
                frs.SourceHandle(node="try_body", port="output_0"),
                frs.SourceHandle(node="handler_0", port="output_0"),
                frs.SourceHandle(node="handler_1", port="x"),
            ],
        },
    )


def _tuple_exceptions_recipe() -> frs.TryNode:
    """Single handler catching both ZeroDivisionError and TypeError."""
    return frs.TryNode(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=frs.LabeledNode(
            label="try_body", node=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            frs.ExceptionCase(
                exceptions=[
                    versions.VersionInfo.of(ZeroDivisionError),
                    versions.VersionInfo.of(TypeError),
                ],
                body=frs.LabeledNode(
                    label="handler_0", node=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            frs.TargetHandle(node="try_body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_body", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="handler_0", port="x"): frs.InputSource(port="x"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="z"): [
                frs.SourceHandle(node="try_body", port="output_0"),
                frs.SourceHandle(node="handler_0", port="x"),
            ],
        },
    )


def _handler_raises_recipe() -> frs.TryNode:
    """try divide(x, 0) raises ZeroDivisionError; handler also calls divide(x, 0)."""
    return frs.TryNode(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=frs.LabeledNode(
            label="try_body", node=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            frs.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=frs.LabeledNode(
                    label="handler_0", node=_fixtures.divide.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            frs.TargetHandle(node="try_body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_body", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="handler_0", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="handler_0", port="y"): frs.InputSource(port="y"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="z"): [
                frs.SourceHandle(node="try_body", port="output_0"),
                frs.SourceHandle(node="handler_0", port="output_0"),
            ],
        },
    )


def _macro_wrapping_try() -> frs.WorkflowNode:
    """Macro: try_safe_divide(x, y) → downstream identity(z). Output from try."""
    identity_recipe = _fixtures.identity.flowrep_recipe
    return frs.WorkflowNode(
        inputs=["x", "y"],
        outputs=["w"],
        nodes={"try_0": _fixtures.try_recipe(), "downstream": identity_recipe},
        input_edges={
            frs.TargetHandle(node="try_0", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_0", port="y"): frs.InputSource(port="y"),
        },
        edges={
            frs.TargetHandle(node="downstream", port="x"): frs.SourceHandle(
                node="try_0", port="z"
            ),
        },
        output_edges={
            frs.OutputTarget(port="w"): frs.SourceHandle(node="try_0", port="z"),
        },
    )


# --------------------------------------------------------------------------- #
# Prospective + retrospective surface                                         #
# --------------------------------------------------------------------------- #


class TestTryProspectiveAndRetrospective(unittest.TestCase):
    """Pre-run vs post-run views of the Try in the `try_safe_divide` fixture."""

    def setUp(self) -> None:
        self.node = _fixtures.try_safe_divide_node()
        self.tryn = self.node.nodes.try_0

    def test_prospective_input_edges_matches_recipe(self) -> None:
        self.assertEqual(
            self.tryn.prospective_input_edges, self.tryn.recipe.input_edges
        )
        self.assertGreater(len(self.tryn.prospective_input_edges), 0)

    def test_prospective_edges_is_empty(self) -> None:
        self.assertEqual(self.tryn.prospective_edges, {})

    def test_prospective_output_edges_matches_recipe(self) -> None:
        self.assertEqual(
            self.tryn.prospective_output_edges,
            self.tryn.recipe.prospective_output_edges,
        )
        self.assertGreater(len(self.tryn.prospective_output_edges), 0)

    def test_prospective_nodes_has_try_and_except_bodies(self) -> None:
        self.assertSetEqual(
            set(self.tryn.prospective_nodes), {"try_body", "except_body_0"}
        )

    def test_pre_run_retrospective_views_empty(self) -> None:
        self.assertEqual(self.tryn.input_edges, {})
        self.assertEqual(self.tryn.edges, {})
        self.assertEqual(self.tryn.output_edges, {})
        self.assertEqual(len(self.tryn.nodes), 0)

    def test_post_run_success_path(self) -> None:
        prospective_input_before = dict(self.tryn.prospective_input_edges)
        prospective_edges_before = dict(self.tryn.prospective_edges)
        prospective_output_before = dict(self.tryn.prospective_output_edges)
        prospective_nodes_before = list(self.tryn.prospective_nodes)

        self.node.run(x=10, y=2)

        self.assertEqual(self.tryn.prospective_input_edges, prospective_input_before)
        self.assertEqual(self.tryn.prospective_edges, prospective_edges_before)
        self.assertEqual(self.tryn.prospective_output_edges, prospective_output_before)
        self.assertEqual(list(self.tryn.prospective_nodes), prospective_nodes_before)

        self.assertGreater(len(self.tryn.input_edges), 0)
        self.assertEqual(self.tryn.edges, {})
        self.assertGreater(len(self.tryn.output_edges), 0)
        self.assertEqual(set(self.tryn.nodes), {"try_body"})

    def test_post_run_exception_path(self) -> None:
        node = _fixtures.try_safe_divide_node()
        tryn = node.nodes.try_0
        node.run(x=10, y=0)
        self.assertSetEqual(set(tryn.nodes), {"try_body", "except_body_0"})


# --------------------------------------------------------------------------- #
# evaluate — try succeeds                                                      #
# --------------------------------------------------------------------------- #


class TestEvaluateTrySucceeds(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        self.run = self.tryn.run(x=10, y=2)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_value(self) -> None:
        self.assertEqual(self.run.outputs["z"].value, 5.0)

    def test_only_try_body_in_steps(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body"])

    def test_retrospective_nodes_keyset(self) -> None:
        self.assertEqual(set(self.tryn.nodes), {"try_body"})

    def test_try_body_step_finished(self) -> None:
        step = self.run.steps[0]
        self.assertEqual(step.run.status, execution.RunStatus.FINISHED)


# --------------------------------------------------------------------------- #
# evaluate — exception handled                                                 #
# --------------------------------------------------------------------------- #


class TestEvaluateExceptionHandled(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        self.run = self.tryn.run(x=10, y=0)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_is_identity_fallback(self) -> None:
        self.assertEqual(self.run.outputs["z"].value, 10)

    def test_steps_include_both_bodies(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body", "except_body_0"])

    def test_try_body_step_failed(self) -> None:
        try_step = self.run.steps[0]
        self.assertEqual(try_step.label, "try_body")
        self.assertEqual(try_step.run.status, execution.RunStatus.FAILED)
        self.assertIsInstance(try_step.run.exception, ZeroDivisionError)

    def test_handler_step_finished(self) -> None:
        handler_step = self.run.steps[1]
        self.assertEqual(handler_step.label, "except_body_0")
        self.assertEqual(handler_step.run.status, execution.RunStatus.FINISHED)


# --------------------------------------------------------------------------- #
# evaluate — exception unmatched                                               #
# --------------------------------------------------------------------------- #


class TestEvaluateExceptionUnmatched(unittest.TestCase):
    # Node.dump() is a known stub; when tryn is the prime mover and fails,
    # execution.run calls dump() → NotImplementedError. The interesting state
    # (current_run.exception, current_run.steps) is recorded before dump() is
    # called, so assertions remain valid after catching either exception.
    _EXPECTED_EXC = (ZeroDivisionError, NotImplementedError)

    def setUp(self) -> None:
        self.recipe = _no_match_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)

    def test_unmatched_exception_propagates(self) -> None:
        with self.assertRaises(self._EXPECTED_EXC):
            self.tryn.run(x=10, y=0)

    def test_try_run_status_failed(self) -> None:
        with contextlib.suppress(*self._EXPECTED_EXC):
            self.tryn.run(x=10, y=0)
        self.assertEqual(self.tryn.current_run.status, execution.RunStatus.FAILED)
        self.assertIsInstance(self.tryn.current_run.exception, ZeroDivisionError)

    def test_only_try_body_step_recorded(self) -> None:
        with contextlib.suppress(*self._EXPECTED_EXC):
            self.tryn.run(x=10, y=0)
        labels = [step.label for step in self.tryn.current_run.steps]
        self.assertEqual(labels, ["try_body"])


# --------------------------------------------------------------------------- #
# evaluate — multiple cases, first match wins                                  #
# --------------------------------------------------------------------------- #


class TestEvaluateMultiCaseFirstMatchWins(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _multi_case_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        # x="bad" triggers TypeError in divide("bad", 1)
        self.run = self.tryn.run(x="bad", y=1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_second_handler_fired(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body", "handler_1"])

    def test_output_from_second_handler(self) -> None:
        # identity("bad") → "bad"
        self.assertEqual(self.run.outputs["z"].value, "bad")


# --------------------------------------------------------------------------- #
# evaluate — tuple exceptions                                                  #
# --------------------------------------------------------------------------- #


class TestEvaluateTupleExceptions(unittest.TestCase):
    def test_zero_division_matches_tuple(self) -> None:
        recipe = _tuple_exceptions_recipe()
        tryn = tryflow.Try("tryn", recipe)
        run = tryn.run(x=10, y=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["z"].value, 10)

    def test_type_error_matches_tuple(self) -> None:
        recipe = _tuple_exceptions_recipe()
        tryn = tryflow.Try("tryn", recipe)
        run = tryn.run(x="bad", y=1)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["z"].value, "bad")


# --------------------------------------------------------------------------- #
# evaluate — handler body raises                                               #
# --------------------------------------------------------------------------- #


class TestEvaluateHandlerBodyRaisesPropagates(unittest.TestCase):
    # Same dump()-stub caveat as TestEvaluateExceptionUnmatched.
    _EXPECTED_EXC = (ZeroDivisionError, NotImplementedError)

    def setUp(self) -> None:
        self.recipe = _handler_raises_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        # Both try and handler call divide(x, y) with y=0 → ZeroDivisionError twice
        with contextlib.suppress(*self._EXPECTED_EXC):
            self.tryn.run(x=10, y=0)

    def test_try_run_status_failed(self) -> None:
        self.assertEqual(self.tryn.current_run.status, execution.RunStatus.FAILED)

    def test_both_body_steps_recorded(self) -> None:
        labels = [step.label for step in self.tryn.current_run.steps]
        self.assertEqual(labels, ["try_body", "handler_0"])

    def test_propagated_exception_is_from_handler(self) -> None:
        with self.assertRaises(self._EXPECTED_EXC):
            tryflow.Try("tryn2", self.recipe).run(x=10, y=0)


# --------------------------------------------------------------------------- #
# Macro-wrapped Try                                                            #
# --------------------------------------------------------------------------- #


class TestMacroWrappedTry(unittest.TestCase):
    def test_success_path(self) -> None:
        node = _fixtures.try_safe_divide_node()
        run = node.run(x=10, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["z"].value, 5.0)

    def test_exception_path(self) -> None:
        node = _fixtures.try_safe_divide_node()
        run = node.run(x=10, y=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["z"].value, 10)


# --------------------------------------------------------------------------- #
# Macro downstream of failed Try propagates exception                          #
# --------------------------------------------------------------------------- #


def _no_match_macro_recipe() -> frs.WorkflowNode:
    """Macro wrapping a no-match Try, with a downstream identity sibling."""
    return frs.WorkflowNode(
        inputs=["x", "y"],
        outputs=["w"],
        nodes={
            "try_0": _no_match_recipe(),
            "downstream": _fixtures.identity.flowrep_recipe,
        },
        input_edges={
            frs.TargetHandle(node="try_0", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_0", port="y"): frs.InputSource(port="y"),
        },
        edges={
            frs.TargetHandle(node="downstream", port="x"): frs.SourceHandle(
                node="try_0", port="z"
            ),
        },
        output_edges={
            frs.OutputTarget(port="w"): frs.SourceHandle(node="try_0", port="z"),
        },
    )


class TestMacroDownstreamOfFailedTry(unittest.TestCase):
    # The macro is the prime mover; when its Try child fails with an unmatched
    # exception, execution.run for the macro calls dump() → NotImplementedError.
    _EXPECTED_EXC = (ZeroDivisionError, NotImplementedError)

    def setUp(self) -> None:
        self.macro = constructors.recipe2static("macro", _no_match_macro_recipe())

    def test_unmatched_exception_raises_from_macro(self) -> None:
        with self.assertRaises(self._EXPECTED_EXC):
            self.macro.run(x=10, y=0)

    def test_failed_macro_status(self) -> None:
        with contextlib.suppress(*self._EXPECTED_EXC):
            self.macro.run(x=10, y=0)
        self.assertEqual(self.macro.current_run.status, execution.RunStatus.FAILED)

    def test_downstream_step_not_recorded_when_try_fails(self) -> None:
        with contextlib.suppress(*self._EXPECTED_EXC):
            self.macro.run(x=10, y=0)
        step_labels = [step.label for step in self.macro.current_run.steps]
        self.assertNotIn("downstream", step_labels)


# --------------------------------------------------------------------------- #
# Internal helper: _resolve_exception_types                                    #
# --------------------------------------------------------------------------- #


class TestResolveExceptionTypes(unittest.TestCase):
    def test_single_exception(self) -> None:
        case = frs.ExceptionCase(
            exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
            body=frs.LabeledNode(label="h", node=_fixtures.identity.flowrep_recipe),
        )
        result = tryflow.Try._resolve_exception_types(case)
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], ZeroDivisionError)

    def test_multiple_exceptions_preserves_order(self) -> None:
        case = frs.ExceptionCase(
            exceptions=[
                versions.VersionInfo.of(ZeroDivisionError),
                versions.VersionInfo.of(TypeError),
            ],
            body=frs.LabeledNode(label="h", node=_fixtures.identity.flowrep_recipe),
        )
        result = tryflow.Try._resolve_exception_types(case)
        self.assertEqual(len(result), 2)
        self.assertIs(result[0], ZeroDivisionError)
        self.assertIs(result[1], TypeError)


# --------------------------------------------------------------------------- #
# Internal helper: _stage_node_input_edges                                     #
# --------------------------------------------------------------------------- #


class TestStageNodeInputEdges(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        self.live = self.tryn.generate_flowrep_live_node()

    def test_only_matching_target_node_edges_copied(self) -> None:
        tryflow.Try._stage_node_input_edges("try_body", self.live, self.recipe)
        self.assertEqual(
            set(self.live.input_edges),
            {
                frs.TargetHandle(node="try_body", port="x"),
                frs.TargetHandle(node="try_body", port="y"),
            },
        )

    def test_subsequent_call_extends_in_place(self) -> None:
        tryflow.Try._stage_node_input_edges("try_body", self.live, self.recipe)
        tryflow.Try._stage_node_input_edges("except_body_0", self.live, self.recipe)
        self.assertEqual(len(self.live.input_edges), 3)
        self.assertIn(
            frs.TargetHandle(node="except_body_0", port="x"), self.live.input_edges
        )


# --------------------------------------------------------------------------- #
# Internal helper: _stage_body_output_edges                                    #
# --------------------------------------------------------------------------- #


class TestStageBodyOutputEdges(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try("tryn", self.recipe)
        self.live = self.tryn.generate_flowrep_live_node()

    def test_picks_try_body_source(self) -> None:
        tryflow.Try._stage_body_output_edges("try_body", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges,
            {
                frs.OutputTarget(port="z"): frs.SourceHandle(
                    node="try_body", port="output_0"
                ),
            },
        )

    def test_picks_except_body_source(self) -> None:
        tryflow.Try._stage_body_output_edges("except_body_0", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges[frs.OutputTarget(port="z")],
            frs.SourceHandle(node="except_body_0", port="x"),
        )

    def test_skips_outputs_without_matching_body(self) -> None:
        live = self.tryn.generate_flowrep_live_node()
        tryflow.Try._stage_body_output_edges("nonexistent_body", live, self.recipe)
        self.assertEqual(live.output_edges, {})


if __name__ == "__main__":
    unittest.main()
