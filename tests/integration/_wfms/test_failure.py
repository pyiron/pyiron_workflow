"""Regression tests for partial-state preservation across nested failures.

These tests verify that when a workflow crashes deep inside nested control
flow (macros, while loops, for-each loops), the dump-on-failure mechanism
captures the partial state of every level of nesting, not just the
top-level run.
"""

from __future__ import annotations

import pathlib
import pickle
import tempfile
import unittest
from concurrent import futures

import flowrep as fr

from pyiron_workflow._wfms import api as wfms

# --------------------------------------------------------------------------- #
# Workflow fixtures                                                           #
# --------------------------------------------------------------------------- #


@fr.atomic
def raise_if_5(x):
    if x == 5:
        raise RuntimeError("I told you I'd raise on 5")
    return x


def less_than(n, limit):
    lt = n < limit
    return lt


def increment(n):
    return n + 1


def range_list(n):
    return list(range(n))


@fr.workflow
def risk_it(limit, n=42):
    x = raise_if_5(n)
    y = raise_if_5(limit)
    return x, y


@fr.workflow
def risk_it_nested(limit, n=42):
    x, y = risk_it(limit, n)
    return x, y


@fr.workflow
def risk_it_while(limit, n=0):
    while less_than(n, limit):
        m = raise_if_5(n)
        n = increment(m)
    return n


@fr.workflow
def risk_it_for(limit):
    ns = range_list(n=limit)
    out = []
    for n in ns:
        m = raise_if_5(n)
        out.append(m)
    return out


@fr.workflow
def risk_it_if(limit, n=42):
    if less_than(n, limit):  # noqa: SIM108
        x = raise_if_5(n)
    else:
        x = raise_if_5(limit)
    return x


def value_out(x):
    raise ValueError("To force the try-node's hand")
    return 42  # Force the parser to see an output


@fr.workflow
def risk_it_try(limit, n=42):
    try:
        x = value_out(n)
    except ValueError:
        x = raise_if_5(limit)
    return x


@fr.workflow
def composite_failure(limit, n=0):
    m = increment(n)
    while less_than(m, limit):
        incremented = increment(m)
        m = raise_if_5(incremented)
    return m


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

_LABEL = "risk_it_wf"


def _failure_name(lexical_path: str) -> str:
    return f"failure_{lexical_path}"


def _pickle_failure(path: pathlib.Path, run: wfms.schemas.Run, exception) -> None:
    with open(path / _failure_name(run.lexical_path), "wb") as f:
        pickle.dump(run, f)


def _run_and_reload(wf_fnc, **input_data) -> wfms.schemas.Run:
    """Run ``wf_fnc``, expect RuntimeError, return the loaded pickled failure."""
    with tempfile.TemporaryDirectory() as tmp:
        progress_dir = pathlib.Path(tmp)
        wf = wfms.node(wf_fnc, label=_LABEL)
        config = wfms.RunConfig(
            prime_mover=_LABEL,
            progress_dir=progress_dir,
            progress_hooks=[],
            exception_hooks=[_pickle_failure],
        )
        try:
            return wfms.tools.run(wf, config, **input_data)
        except RuntimeError:
            dump_path = progress_dir / _failure_name(wf.lexical_path)
            with open(dump_path, "rb") as f:
                return pickle.load(f)


def _is_not_data(value) -> bool:
    return isinstance(value, fr.schemas.NotData)


# --------------------------------------------------------------------------- #
# Linear failure (single macro)                                               #
# --------------------------------------------------------------------------- #


class TestLinearFailure(unittest.TestCase):
    """Failure in a flat workflow -- the simplest case."""

    def test_partial_state_preserved(self) -> None:
        run_obj = _run_and_reload(risk_it, limit=5)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)

        # The first raise_if_5 (n=42) finished and stored its output.
        self.assertEqual(
            run_obj.result.nodes["raise_if_5_0"].output_ports["x"].value, 42
        )
        # The second raise_if_5 (limit=5) crashed -- its port is NOT_DATA.
        self.assertTrue(
            _is_not_data(run_obj.result.nodes["raise_if_5_1"].output_ports["x"].value)
        )

        # Steps mirror the same picture.
        self.assertEqual(run_obj.steps[0].outputs["x"].value, 42)
        self.assertTrue(_is_not_data(run_obj.steps[1].outputs["x"].value))


# --------------------------------------------------------------------------- #
# Nested macro failure                                                        #
# --------------------------------------------------------------------------- #


class TestNestedFailure(unittest.TestCase):
    """A workflow that wraps another workflow that crashes."""

    def test_partial_state_preserved_through_nesting(self) -> None:
        run_obj = _run_and_reload(risk_it_nested, limit=5)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)

        inner = run_obj.result.nodes["risk_it_0"]
        self.assertEqual(inner.nodes["raise_if_5_0"].output_ports["x"].value, 42)
        self.assertTrue(
            _is_not_data(inner.nodes["raise_if_5_1"].output_ports["x"].value)
        )

        # Step tree: top -> nested macro -> raise_if_5_{0,1}.
        inner_steps = run_obj.steps[0].steps
        self.assertEqual(
            inner_steps[0].outputs["x"].value,
            42,
            msg="Finishes running before we get to the failure",
        )
        self.assertTrue(_is_not_data(inner_steps[1].outputs["x"].value))


# --------------------------------------------------------------------------- #
# While-loop failure                                                          #
# --------------------------------------------------------------------------- #


class TestWhileFailure(unittest.TestCase):
    """A while-loop body that crashes on the final iteration."""

    def test_partial_state_preserved_across_iterations(self) -> None:
        run_obj = _run_and_reload(risk_it_while, limit=6)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)

        # `risk_it_while` is the single top-level step; its `.steps` is the
        # alternating list [cond_0, body_0, cond_1, body_1, ...]. With limit=6,
        # cond_5 emits True and body_5 crashes inside raise_if_5(5):
        # 6 conditions + 5 finished bodies + 1 crashed body = 12 entries.
        while_steps = run_obj.steps[0].steps
        self.assertEqual(len(while_steps), 12, msg="Spot check comment")

        # Iteration 0 body: raise_if_5(0) -> x=0, increment(0) -> 1.
        body_0 = while_steps[1]
        self.assertEqual(body_0.steps[0].outputs["x"].value, 0)

        # Iteration 4 body: raise_if_5(4) -> x=4.
        # Counting from the end: -1 = body_5 (crashed), -2 = cond_5, -3 = body_4.
        body_4 = while_steps[-3]
        self.assertEqual(
            body_4.steps[0].outputs["x"].value, 4, msg="Raise if should pass"
        )
        self.assertEqual(
            body_4.steps[1].outputs["output_0"].value, 5, msg="Incremented"
        )

        # Iteration 5 body: raise_if_5(5) crashed; output stays NOT_DATA.
        body_5 = while_steps[-1]
        self.assertTrue(_is_not_data(body_5.steps[0].outputs["x"].value))


# --------------------------------------------------------------------------- #
# For-each failure                                                            #
# --------------------------------------------------------------------------- #


class TestForEachFailure(unittest.TestCase):
    """A for-each whose body crashes on one of its iterations."""

    def test_partial_state_preserved_in_for_each(self) -> None:
        run_obj = _run_and_reload(risk_it_for, limit=6)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)

        # The for-each runtime DAG scatters raise_if_5_{0..5}; iteration 5
        # crashes inside raise_if_5(5).
        for_each_result = run_obj.result.nodes["for_each_0"]
        self.assertGreater(
            len(for_each_result.nodes),
            0,
            "Expected the for-each runtime DAG to be populated on the dumped run",
        )

        # Sub-step state should survive the failure dump.
        # Scatter + bodies 0-5 (final one fails) = 1 + 6 = 7 steps
        for_each_step = run_obj.steps[1]
        self.assertEqual(len(for_each_step.steps), 7)
        self.assertIn("scatter", for_each_step.steps[0].label)
        self.assertEqual(for_each_step.steps[1].outputs["m"].value, 0)
        self.assertEqual(for_each_step.steps[-2].outputs["m"].value, 4)
        self.assertTrue(_is_not_data(for_each_step.steps[-1].outputs["m"].value))


# --------------------------------------------------------------------------- #
# If failure                                                            #
# --------------------------------------------------------------------------- #


class TestIfFailure(unittest.TestCase):
    """An If whose clauses both crash but will pass condition evaluation."""

    def test_partial_state_preserved_in_for_each(self) -> None:
        run_obj = _run_and_reload(risk_it_if, limit=5)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)

        condition_step = run_obj.steps[0].steps[0]
        self.assertEqual(condition_step.outputs["lt"].value, False)
        self.assertEqual(condition_step.status, wfms.schemas.RunStatus.FINISHED)

        else_step = run_obj.steps[0].steps[1]
        self.assertEqual(else_step.status, wfms.schemas.RunStatus.FAILED)
        self.assertTrue(_is_not_data(else_step.outputs["x"].value))


# --------------------------------------------------------------------------- #
# Try failure                                                            #
# --------------------------------------------------------------------------- #


class TestTryFailure(unittest.TestCase):
    """A Try who encounters an unhandled error"""

    def test_partial_state_preserved_in_for_each(self) -> None:
        run_obj = _run_and_reload(risk_it_try, limit=5)

        self.assertEqual(run_obj.status, wfms.schemas.RunStatus.FAILED)
        try_body = run_obj.steps[0].steps[0]
        self.assertIn("try", try_body.label)
        self.assertEqual(
            try_body.status,
            wfms.schemas.RunStatus.FAILED,
            msg="It fails as a regular part of the try/except node",
        )
        except_body = run_obj.steps[0].steps[1]
        self.assertIn("except", except_body.label)
        self.assertEqual(
            except_body.status,
            wfms.schemas.RunStatus.FAILED,
            msg="It fails because of our forced runtime error, triggering the dump",
        )
        self.assertEqual(len(run_obj.steps[0].steps), 2, msg="And that's all there is")


# --------------------------------------------------------------------------- #
# Out-of-process failure                                                            #
# --------------------------------------------------------------------------- #


class TestOutOfProcessFailure(unittest.TestCase):
    """
    A composite node where we can nest the failure.
    We expect multiple dump files: one for the parent, and one for the out-of-process
    failure.
    """

    def test_while_failure_on_process_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            progress_dir = pathlib.Path(tmp)
            wf = wfms.node(composite_failure, label=_LABEL)
            config = wfms.RunConfig(
                prime_mover=_LABEL,
                progress_dir=progress_dir,
                progress_hooks=[],
                exception_hooks=[_pickle_failure],
            )
            try:
                with futures.ProcessPoolExecutor() as exe:
                    # Apply an executor deeply inside the graph
                    wf.while_0.body.raise_if_5_0.executor = exe
                    wfms.tools.run(wf, config, limit=6)
            except RuntimeError:
                pass  # That's the point here

            with open(progress_dir / _failure_name(wf.lexical_path), "rb") as f:
                reloaded = pickle.load(f)

            penultimate_body_child = (
                reloaded.steps[1]  # while_0
                .steps[-3]  # body_2
                .steps[1]  # raise_if_5_0
            )
            self.assertEqual(
                penultimate_body_child.outputs["x"].value,
                4,
                msg="Penultimate state should be recovered, even though it's from a "
                "remote process (and different remote than the failure process)",
            )
            self.assertIs(
                penultimate_body_child.result,
                reloaded.result.nodes["while_0"].nodes["body_2"].nodes["raise_if_5_0"],
                msg="Memory-efficient `is` synchronization between steps and result "
                "must be preserved, even in cross-process runs.",
            )
            failed_body = reloaded.steps[1].steps[-1]  # while_0  # body_3
            failed_body_increment = failed_body.steps[0]
            failed_body_raise_if_5 = failed_body.steps[1]
            self.assertEqual(
                failed_body_increment.outputs["output_0"].value,
                5,
                msg="We should be recovering state right up until the very last minute",
            )
            self.assertTrue(
                _is_not_data(failed_body_raise_if_5.outputs["x"].value),
                msg="And this is it, this is the last instance and we cannot recover "
                "data from the failed atomic node",
            )
            self.assertTrue(
                _is_not_data(failed_body.outputs["m"].value),
                msg="Unavailability of output should have propagated up to the parent "
                "output.",
            )


if __name__ == "__main__":
    unittest.main()
