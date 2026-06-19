from __future__ import annotations

import datetime
import pathlib
import tempfile
import unittest
from concurrent import futures
from typing import Any

import flowrep as fr

from pyiron_workflow._wfms import atomic, execution
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Helper subclasses                                                           #
# --------------------------------------------------------------------------- #


class _FailingAtomic(atomic.Atomic):
    """:meth:`evaluate` raises `RuntimeError`."""

    def evaluate(
        self,
        run: execution.Run[fr.schemas.AtomicData],
        config: execution.RunConfig,
    ) -> None:
        raise RuntimeError("boom")


def _make_failing_node(label: str = "boom_node") -> _FailingAtomic:
    """Build a :class:`_FailingDumpingAtomic` reusing the `add` recipe."""
    return _FailingAtomic(label, _fixtures.add.flowrep_recipe)


def _default_config(node: Any, progress_dir: pathlib.Path) -> execution.RunConfig:
    return execution.RunConfig(
        prime_mover=node.lexical_path,
        progress_dir=progress_dir,
        progress_hooks=[],
    )


# --------------------------------------------------------------------------- #
# Run.duration                                                                #
# --------------------------------------------------------------------------- #


class TestRunDuration(unittest.TestCase):
    def _make_run(
        self,
        started_at: datetime.datetime | None,
        finished_at: datetime.datetime | None,
        lexical_path: str = "some_path_root",
    ) -> execution.Run[fr.schemas.AtomicData]:
        # `result` is not exercised here; a freshly minted live atomic suffices.
        live = _fixtures.atomic_add_node().generate_flowrep_live_node()
        return execution.Run[fr.schemas.AtomicData](
            lexical_path=lexical_path,
            result=live,
            status=execution.RunStatus.PENDING,
            started_at=started_at,
            finished_at=finished_at,
        )

    def test_both_timestamps_returns_timedelta(self) -> None:
        start = datetime.datetime(2026, 1, 1, 12, 0, 0)
        end = start + datetime.timedelta(seconds=5)
        run = self._make_run(start, end)
        self.assertEqual(run.duration, datetime.timedelta(seconds=5))

    def test_missing_start_returns_none(self) -> None:
        end = datetime.datetime(2026, 1, 1, 12, 0, 0)
        run = self._make_run(None, end)
        self.assertIsNone(run.duration)

    def test_missing_end_returns_none(self) -> None:
        start = datetime.datetime(2026, 1, 1, 12, 0, 0)
        run = self._make_run(start, None)
        self.assertIsNone(run.duration)

    def test_both_missing_returns_none(self) -> None:
        run = self._make_run(None, None)
        self.assertIsNone(run.duration)


# --------------------------------------------------------------------------- #
# RunConfig                                                                   #
# --------------------------------------------------------------------------- #


class TestRunConfigIsPrimeMover(unittest.TestCase):
    def test_matching_lexical_path_returns_true(self) -> None:
        node = _fixtures.atomic_add_node()
        config = _default_config(node, pathlib.Path.cwd())
        self.assertTrue(config.is_prime_mover(node))

    def test_non_matching_lexical_path_returns_false(self) -> None:
        node = _fixtures.atomic_add_node()
        other = _fixtures.atomic_add_node(label="not_the_same")
        config = _default_config(node, pathlib.Path.cwd())
        self.assertFalse(config.is_prime_mover(other))


class TestRunConfigEmitProgress(unittest.TestCase):
    def test_emit_progress_calls_every_hook_with_four_args(self) -> None:
        captured_a: list[
            tuple[pathlib.Path, datetime.datetime, str, execution.RunStatus]
        ] = []
        captured_b: list[
            tuple[pathlib.Path, datetime.datetime, str, execution.RunStatus]
        ] = []

        def hook_a(
            progress_dir: pathlib.Path,
            t: datetime.datetime,
            lp: str,
            status: execution.RunStatus,
        ) -> None:
            captured_a.append((progress_dir, t, lp, status))

        def hook_b(
            progress_dir: pathlib.Path,
            t: datetime.datetime,
            lp: str,
            status: execution.RunStatus,
        ) -> None:
            captured_b.append((progress_dir, t, lp, status))

        progress_dir = pathlib.Path("/tmp/whatever")
        config = execution.RunConfig(
            prime_mover="pm",
            progress_dir=progress_dir,
            progress_hooks=[hook_a, hook_b],
        )
        now = datetime.datetime(2026, 1, 1, 12, 0, 0)
        config.emit_progress(now, "pm", execution.RunStatus.PENDING)

        expected = [(progress_dir, now, "pm", execution.RunStatus.PENDING)]
        self.assertEqual(captured_a, expected)
        self.assertEqual(captured_b, expected)


# --------------------------------------------------------------------------- #
# ExecutorInstructions                                                        #
# --------------------------------------------------------------------------- #


class TestExecutorInstructions(unittest.TestCase):
    def test_instantiate_builds_thread_pool_with_kwargs(self) -> None:
        instructions = execution.ExecutorInstructions(
            constructor=futures.ThreadPoolExecutor,
            args=(),
            kwargs={"max_workers": 2},
        )
        exe = instructions.instantiate()
        try:
            self.assertIsInstance(exe, futures.ThreadPoolExecutor)
            # `_max_workers` is the public-ish attribute used by the stdlib
            # implementation; assert it matches the kwarg we passed in.
            self.assertEqual(exe._max_workers, 2)
        finally:
            exe.shutdown(wait=True)

    def test_instantiate_returns_fresh_instance_per_call(self) -> None:
        instructions = execution.ExecutorInstructions(
            constructor=futures.ThreadPoolExecutor,
            kwargs={"max_workers": 1},
        )
        a = instructions.instantiate()
        b = instructions.instantiate()
        try:
            self.assertIsNot(a, b)
        finally:
            a.shutdown(wait=True)
            b.shutdown(wait=True)


# --------------------------------------------------------------------------- #
# run() — happy path                                                          #
# --------------------------------------------------------------------------- #


class TestRunHappyPath(unittest.TestCase):
    def test_happy_path_atomic_add(self) -> None:
        node = _fixtures.atomic_add_node()
        captured: list[tuple[str, execution.RunStatus]] = []

        def hook(
            progress_dir: pathlib.Path,
            t: datetime.datetime,
            lp: str,
            status: execution.RunStatus,
        ) -> None:
            captured.append((lp, status))

        with tempfile.TemporaryDirectory() as tmp:
            config = execution.RunConfig(
                prime_mover=node.lexical_path,
                progress_dir=pathlib.Path(tmp),
                progress_hooks=[hook],
            )
            run = execution.run(node, config, x=1, y=2)

        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["output_0"].value, 3)
        self.assertIsNotNone(run.duration)

        # Hook sees RUNNING and FINISHED (never PENDING).
        statuses = [s for _, s in captured]
        self.assertNotIn(execution.RunStatus.PENDING, statuses)
        self.assertIn(execution.RunStatus.FINISHED, statuses)
        self.assertIn(execution.RunStatus.RUNNING, statuses)


# --------------------------------------------------------------------------- #
# run() — failure path                                                        #
# --------------------------------------------------------------------------- #


class TestRunFailurePath(unittest.TestCase):
    def test_failure_records_state_and_dumps(self) -> None:
        node = _make_failing_node()

        dump_calls = []

        def dump(progress_dir: pathlib.Path, run, exception) -> None:
            dump_calls.append((progress_dir / run.lexical_path, run.status))

        with tempfile.TemporaryDirectory() as tmp:
            progress_dir = pathlib.Path(tmp)

            config = execution.RunConfig(
                prime_mover=node.lexical_path,
                progress_dir=progress_dir,
                progress_hooks=[],
                exception_hooks=[dump],
            )

            with self.assertRaises(RuntimeError) as ctx:
                execution.run(node, config, x=1, y=2)

            self.assertEqual(str(ctx.exception), "boom")
            self.assertEqual(
                dump_calls,
                [
                    (
                        progress_dir / node.lexical_path,
                        execution.RunStatus.FAILED,
                    )
                ],
            )


# --------------------------------------------------------------------------- #
# run() — executor branches                                                   #
# --------------------------------------------------------------------------- #


class TestRunExecutorBranches(unittest.TestCase):
    def test_executor_instructions_branch_succeeds(self) -> None:
        node = _fixtures.atomic_add_node()
        node.executor = execution.ExecutorInstructions(
            constructor=futures.ThreadPoolExecutor,
            kwargs={"max_workers": 1},
        )
        with tempfile.TemporaryDirectory() as tmp:
            config = _default_config(node, pathlib.Path(tmp))
            run = execution.run(node, config, x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["output_0"].value, 3)

    def test_live_executor_branch_succeeds(self) -> None:
        node = _fixtures.atomic_add_node()
        with futures.ThreadPoolExecutor(max_workers=1) as exe:
            node.executor = exe
            with tempfile.TemporaryDirectory() as tmp:
                config = _default_config(node, pathlib.Path(tmp))
                run = execution.run(node, config, x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["output_0"].value, 3)

    def test_none_executor_branch_succeeds(self) -> None:
        node = _fixtures.atomic_add_node()
        self.assertIsNone(node.executor)
        with tempfile.TemporaryDirectory() as tmp:
            config = _default_config(node, pathlib.Path(tmp))
            run = execution.run(node, config, x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs["output_0"].value, 3)

    def test_invalid_executor_type_raises_with_lexical_path(self) -> None:
        # The prime-mover failure branch will call `node.dump` after the
        # TypeError. Use `_DumpingAtomic` so that no-op succeeds rather than
        # surfacing `NotImplementedError`.
        node = _FailingAtomic("add_invalid", _fixtures.add.flowrep_recipe)
        # Bypass static type checking — the branch under test is specifically
        # the "anything other than ExecutorInstructions / futures.Executor"
        # fallthrough.
        node.executor = "not an executor"  # type: ignore[assignment]
        with tempfile.TemporaryDirectory() as tmp:
            config = _default_config(node, pathlib.Path(tmp))
            with self.assertRaises(TypeError) as ctx:
                execution.run(node, config, x=1, y=2)
        self.assertIn(node.lexical_path, str(ctx.exception))


# --------------------------------------------------------------------------- #
# run() — progress hooks                                                      #
# --------------------------------------------------------------------------- #


class TestRunProgressHooks(unittest.TestCase):
    def test_prime_mover_status_set_is_running_then_finished(self) -> None:
        macro = _fixtures.macro_node()
        captured: list[tuple[str, execution.RunStatus]] = []

        def hook(
            progress_dir: pathlib.Path,
            t: datetime.datetime,
            lp: str,
            status: execution.RunStatus,
        ) -> None:
            captured.append((lp, status))

        with tempfile.TemporaryDirectory() as tmp:
            config = execution.RunConfig(
                prime_mover=macro.lexical_path,
                progress_dir=pathlib.Path(tmp),
                progress_hooks=[hook],
            )
            execution.run(macro, config, x=1, y=2, z=3)

        prime_statuses = {status for lp, status in captured if lp == macro.lexical_path}
        self.assertEqual(
            prime_statuses,
            {execution.RunStatus.RUNNING, execution.RunStatus.FINISHED},
        )


# --------------------------------------------------------------------------- #
# populate_input_ports                                                        #
# --------------------------------------------------------------------------- #


class TestPopulateInputPorts(unittest.TestCase):
    def test_known_port_assigns_value(self) -> None:
        node = _fixtures.atomic_add_node()
        live = node.generate_flowrep_live_node()
        execution.populate_input_ports(live, {"x": 1})
        self.assertEqual(live.input_ports["x"].value, 1)

    def test_unknown_port_raises_value_error_with_name_and_inputs(self) -> None:
        node = _fixtures.atomic_add_node()
        live = node.generate_flowrep_live_node()
        with self.assertRaises(ValueError) as ctx:
            execution.populate_input_ports(live, {"nonsense": 1})
        msg = str(ctx.exception)
        self.assertIn("nonsense", msg)
        # The error message includes the recipe's input list verbatim.
        self.assertIn(str(live.recipe.inputs), msg)


if __name__ == "__main__":
    unittest.main()
