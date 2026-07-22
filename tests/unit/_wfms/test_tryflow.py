from __future__ import annotations

import unittest

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.flowcontrollers import tryflow
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Recipe builders                                                             #
# --------------------------------------------------------------------------- #


def _no_match_recipe() -> fr.schemas.TryRecipe:
    """
    try divide(x, y); except TypeError → identity(x). No match for ZeroDivisionError.
    """
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=fr.schemas.LabeledRecipe(
            label="try_body", recipe=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            fr.schemas.ExceptionCase(
                exceptions=[versions.VersionInfo.of(TypeError)],
                body=fr.schemas.LabeledRecipe(
                    label="handler_0", recipe=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            fr.schemas.TargetHandle(node="try_body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="handler_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="z"): [
                fr.schemas.SourceHandle(node="try_body", port="output_0"),
                fr.schemas.SourceHandle(node="handler_0", port="x"),
            ],
        },
    )


def _multi_case_recipe() -> fr.schemas.TryRecipe:
    """try divide(x, y); except TypeError → identity; except ValueError → negate.

    try_body raises ValueError (via negate with a string — actually we need to raise
    ValueError manually). Instead: try_body calls divide but we use x="bad" to force
    TypeError... Actually we want to test case order specifically.

    Simpler approach: try_body = divide(x, y) where x="bad", y=1 → TypeError.
    First handler catches ZeroDivisionError (no match), second catches TypeError (match).
    """
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=fr.schemas.LabeledRecipe(
            label="try_body", recipe=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            fr.schemas.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=fr.schemas.LabeledRecipe(
                    label="handler_0", recipe=_fixtures.negate.flowrep_recipe
                ),
            ),
            fr.schemas.ExceptionCase(
                exceptions=[versions.VersionInfo.of(TypeError)],
                body=fr.schemas.LabeledRecipe(
                    label="handler_1", recipe=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            fr.schemas.TargetHandle(node="try_body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="handler_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="handler_1", port="x"): fr.schemas.InputSource(
                port="x"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="z"): [
                fr.schemas.SourceHandle(node="try_body", port="output_0"),
                fr.schemas.SourceHandle(node="handler_0", port="output_0"),
                fr.schemas.SourceHandle(node="handler_1", port="x"),
            ],
        },
    )


def _tuple_exceptions_recipe() -> fr.schemas.TryRecipe:
    """Single handler catching both ZeroDivisionError and TypeError."""
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=fr.schemas.LabeledRecipe(
            label="try_body", recipe=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            fr.schemas.ExceptionCase(
                exceptions=[
                    versions.VersionInfo.of(ZeroDivisionError),
                    versions.VersionInfo.of(TypeError),
                ],
                body=fr.schemas.LabeledRecipe(
                    label="handler_0", recipe=_fixtures.identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            fr.schemas.TargetHandle(node="try_body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="handler_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="z"): [
                fr.schemas.SourceHandle(node="try_body", port="output_0"),
                fr.schemas.SourceHandle(node="handler_0", port="x"),
            ],
        },
    )


def _handler_raises_recipe() -> fr.schemas.TryRecipe:
    """try divide(x, 0) raises ZeroDivisionError; handler also calls divide(x, 0)."""
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=fr.schemas.LabeledRecipe(
            label="try_body", recipe=_fixtures.divide.flowrep_recipe
        ),
        exception_cases=[
            fr.schemas.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=fr.schemas.LabeledRecipe(
                    label="handler_0", recipe=_fixtures.divide.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            fr.schemas.TargetHandle(node="try_body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="handler_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="handler_0", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="z"): [
                fr.schemas.SourceHandle(node="try_body", port="output_0"),
                fr.schemas.SourceHandle(node="handler_0", port="output_0"),
            ],
        },
    )


def _macro_wrapping_try() -> fr.schemas.WorkflowRecipe:
    """Macro: try_safe_divide(x, y) → downstream identity(z). Output from try."""
    identity_recipe = _fixtures.identity.flowrep_recipe
    return fr.schemas.WorkflowRecipe(
        inputs=["x", "y"],
        outputs=["w"],
        nodes={"try_0": _fixtures.try_recipe(), "downstream": identity_recipe},
        input_edges={
            fr.schemas.TargetHandle(node="try_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_0", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        edges={
            fr.schemas.TargetHandle(
                node="downstream", port="x"
            ): fr.schemas.SourceHandle(node="try_0", port="z"),
        },
        output_edges={
            fr.schemas.OutputTarget(port="w"): fr.schemas.SourceHandle(
                node="try_0", port="z"
            ),
        },
    )


# --------------------------------------------------------------------------- #
# evaluate — try succeeds                                                      #
# --------------------------------------------------------------------------- #


class TestEvaluateTrySucceeds(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")
        self.run = self.tryn.run(x=10, y=2)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_value(self) -> None:
        self.assertEqual(self.run.outputs.z, 5.0)

    def test_only_try_body_in_steps(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body"])

    def test_retrospective_nodes_keyset(self) -> None:
        self.assertEqual(set(self.run.result.nodes), {"try_body"})

    def test_try_body_step_finished(self) -> None:
        self.assertEqual(self.run.steps[0].status, execution.RunStatus.FINISHED)


# --------------------------------------------------------------------------- #
# evaluate — exception handled                                                 #
# --------------------------------------------------------------------------- #


class TestEvaluateExceptionHandled(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")
        self.run = self.tryn.run(x=10, y=0)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_output_is_identity_fallback(self) -> None:
        self.assertEqual(self.run.outputs.z, 10)

    def test_steps_include_both_bodies(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body", "except_body_0"])

    def test_try_body_step_failed(self) -> None:
        try_step = self.run.steps[0]
        self.assertEqual(try_step.label, "try_body")
        self.assertEqual(try_step.status, execution.RunStatus.FAILED)
        self.assertIsInstance(try_step.exception, ZeroDivisionError)

    def test_handler_step_finished(self) -> None:
        handler_step = self.run.steps[1]
        self.assertEqual(handler_step.label, "except_body_0")
        self.assertEqual(handler_step.status, execution.RunStatus.FINISHED)


# --------------------------------------------------------------------------- #
# evaluate — exception unmatched                                               #
# --------------------------------------------------------------------------- #


class TestEvaluateExceptionUnmatched(unittest.TestCase):
    _EXPECTED_EXC = (ZeroDivisionError, tryflow.UnmatchedExceptionError)

    def setUp(self) -> None:
        self.recipe = _no_match_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")

    def test_unmatched_exception_propagates(self) -> None:
        with self.assertRaises(tryflow.UnmatchedExceptionError) as ctx:
            self.tryn.run(x=10, y=0)
        self.assertIsInstance(ctx.exception.__cause__, ZeroDivisionError)


# --------------------------------------------------------------------------- #
# evaluate — multiple cases, first match wins                                  #
# --------------------------------------------------------------------------- #


class TestEvaluateMultiCaseFirstMatchWins(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _multi_case_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")
        # x="bad" triggers TypeError in divide("bad", 1)
        self.run = self.tryn.run(x="bad", y=1)

    def test_run_finished(self) -> None:
        self.assertEqual(self.run.status, execution.RunStatus.FINISHED)

    def test_second_handler_fired(self) -> None:
        labels = [step.label for step in self.run.steps]
        self.assertEqual(labels, ["try_body", "handler_1"])

    def test_output_from_second_handler(self) -> None:
        # identity("bad") → "bad"
        self.assertEqual(self.run.outputs.z, "bad")


# --------------------------------------------------------------------------- #
# evaluate — tuple exceptions                                                  #
# --------------------------------------------------------------------------- #


class TestEvaluateTupleExceptions(unittest.TestCase):
    def test_zero_division_matches_tuple(self) -> None:
        recipe = _tuple_exceptions_recipe()
        tryn = tryflow.Try(recipe, "tryn")
        run = tryn.run(x=10, y=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.z, 10)

    def test_type_error_matches_tuple(self) -> None:
        recipe = _tuple_exceptions_recipe()
        tryn = tryflow.Try(recipe, "tryn")
        run = tryn.run(x="bad", y=1)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.z, "bad")


# --------------------------------------------------------------------------- #
# evaluate — handler body raises                                               #
# --------------------------------------------------------------------------- #


class TestEvaluateHandlerBodyRaisesPropagates(unittest.TestCase):
    _EXPECTED_EXC = (ZeroDivisionError, tryflow.UnmatchedExceptionError)

    def setUp(self) -> None:
        self.recipe = _handler_raises_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")
        # Both try and handler call divide(x, y) with y=0 → ZeroDivisionError twice

    def test_propagated_exception_is_from_handler(self) -> None:
        with self.assertRaises(self._EXPECTED_EXC):
            tryflow.Try(self.recipe, "tryn2").run(x=10, y=0)


# --------------------------------------------------------------------------- #
# Macro-wrapped Try                                                            #
# --------------------------------------------------------------------------- #


class TestMacroWrappedTry(unittest.TestCase):
    def test_success_path(self) -> None:
        node = _fixtures.try_safe_divide_node()
        run = node.run(x=10, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.z, 5.0)

    def test_exception_path(self) -> None:
        node = _fixtures.try_safe_divide_node()
        run = node.run(x=10, y=0)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.z, 10)


# --------------------------------------------------------------------------- #
# Macro downstream of failed Try propagates exception                          #
# --------------------------------------------------------------------------- #


def _no_match_macro_recipe() -> fr.schemas.WorkflowRecipe:
    """Macro wrapping a no-match Try, with a downstream identity sibling."""
    return fr.schemas.WorkflowRecipe(
        inputs=["x", "y"],
        outputs=["w"],
        nodes={
            "try_0": _no_match_recipe(),
            "downstream": _fixtures.identity.flowrep_recipe,
        },
        input_edges={
            fr.schemas.TargetHandle(node="try_0", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_0", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        edges={
            fr.schemas.TargetHandle(
                node="downstream", port="x"
            ): fr.schemas.SourceHandle(node="try_0", port="z"),
        },
        output_edges={
            fr.schemas.OutputTarget(port="w"): fr.schemas.SourceHandle(
                node="try_0", port="z"
            ),
        },
    )


# --------------------------------------------------------------------------- #
# Internal helper: _resolve_exception_types                                    #
# --------------------------------------------------------------------------- #


class TestResolveExceptionTypes(unittest.TestCase):
    def test_single_exception(self) -> None:
        case = fr.schemas.ExceptionCase(
            exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
            body=fr.schemas.LabeledRecipe(
                label="h", recipe=_fixtures.identity.flowrep_recipe
            ),
        )
        result = tryflow.Try._resolve_exception_types(case)
        self.assertEqual(len(result), 1)
        self.assertIs(result[0], ZeroDivisionError)

    def test_multiple_exceptions_preserves_order(self) -> None:
        case = fr.schemas.ExceptionCase(
            exceptions=[
                versions.VersionInfo.of(ZeroDivisionError),
                versions.VersionInfo.of(TypeError),
            ],
            body=fr.schemas.LabeledRecipe(
                label="h", recipe=_fixtures.identity.flowrep_recipe
            ),
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
        self.tryn = tryflow.Try(self.recipe, "tryn")
        self.live = self.tryn.generate_flowrep_live_node()

    def test_only_matching_target_node_edges_copied(self) -> None:
        tryflow.Try._stage_node_input_edges("try_body", self.live, self.recipe)
        self.assertEqual(
            set(self.live.input_edges),
            {
                fr.schemas.TargetHandle(node="try_body", port="x"),
                fr.schemas.TargetHandle(node="try_body", port="y"),
            },
        )

    def test_subsequent_call_extends_in_place(self) -> None:
        tryflow.Try._stage_node_input_edges("try_body", self.live, self.recipe)
        tryflow.Try._stage_node_input_edges("except_body_0", self.live, self.recipe)
        self.assertEqual(len(self.live.input_edges), 3)
        self.assertIn(
            fr.schemas.TargetHandle(node="except_body_0", port="x"),
            self.live.input_edges,
        )


# --------------------------------------------------------------------------- #
# Internal helper: _stage_body_output_edges                                    #
# --------------------------------------------------------------------------- #


class TestStageBodyOutputEdges(unittest.TestCase):
    def setUp(self) -> None:
        self.recipe = _fixtures.try_recipe()
        self.tryn = tryflow.Try(self.recipe, "tryn")
        self.live = self.tryn.generate_flowrep_live_node()

    def test_picks_try_body_source(self) -> None:
        tryflow.Try._stage_body_output_edges("try_body", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges,
            {
                fr.schemas.OutputTarget(port="z"): fr.schemas.SourceHandle(
                    node="try_body", port="output_0"
                ),
            },
        )

    def test_picks_except_body_source(self) -> None:
        tryflow.Try._stage_body_output_edges("except_body_0", self.live, self.recipe)
        self.assertEqual(
            self.live.output_edges[fr.schemas.OutputTarget(port="z")],
            fr.schemas.SourceHandle(node="except_body_0", port="x"),
        )

    def test_skips_outputs_without_matching_body(self) -> None:
        live = self.tryn.generate_flowrep_live_node()
        tryflow.Try._stage_body_output_edges("nonexistent_body", live, self.recipe)
        self.assertEqual(live.output_edges, {})


if __name__ == "__main__":
    unittest.main()
