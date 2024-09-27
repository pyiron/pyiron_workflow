from concurrent.futures import Future
import unittest

from pyiron_workflow.executors.cloudpickleprocesspool import (
    CloudpickleProcessPoolExecutor
)
from pyiron_workflow.mixin.run import Runnable, ReadinessError


class ConcreteRunnable(Runnable):
    @property
    def label(self) -> str:
        return "child_class_with_all_methods_implemented"

    def on_run(self, **kwargs):
        return kwargs

    @property
    def run_args(self):
        return (), {"foo": 42}

    def process_run_result(self, run_output):
        self.processed = dict(run_output)
        self.processed["bar"] = 0
        return run_output

    def custom_callback(self, result):
        result = self._finish_run(result)
        self.processed.update({"baz": 1})
        return result

    @property
    def expected_run_output(self):
        return {"foo": 42}

    @property
    def expected_processed_value(self):
        return {"foo": 42, "bar": 0}

    @property
    def expected_custom_callback_processed_value(self):
        return {"foo": 42, "bar": 0, "baz": 1}


class FailingRunnable(ConcreteRunnable):
    def on_run(self, **kwargs):
        raise RuntimeError()


class TestRunnable(unittest.TestCase):
    def test_runnable_not_ready(self):
        runnable = ConcreteRunnable()

        self.assertTrue(
            runnable.ready,
            msg="Freshly instantiated, it is neither running nor failed!"
        )

        with self.subTest("Running"):
            try:
                runnable.running = True
                self.assertFalse(runnable.ready)
                with self.assertRaises(ReadinessError):
                    runnable.run()
            finally:
                runnable.running = False

        with self.subTest("Failed"):
            try:
                runnable.failed = True
                self.assertFalse(runnable.ready)
                with self.assertRaises(ReadinessError):
                    runnable.run()
            finally:
                runnable.failed = False

        with self.subTest("Bypass"):
            runnable.failed = True
            self.assertFalse(runnable.ready)
            result = runnable.run(check_readiness=False)
            self.assertEqual(
                runnable.expected_run_output,
                result,
                msg="We should be able to bypass the readiness check with a flag, and "
                    "in this simple case expect to get perfectly normal behaviour "
                    "afterwards"
            )

    def test_failure(self):
        runnable = FailingRunnable()

        with self.assertRaises(RuntimeError):
            runnable.run()
        self.assertTrue(
            runnable.failed,
            msg="Encountering an error should set status to failed"
        )

        runnable.failed = False
        runnable.run(raise_run_exceptions=False)
        self.assertTrue(
            runnable.failed,
            msg="We should be able to stop the exception from getting raised, but the "
                "status should still be failed"
        )

    def test_runnable_run_local(self):
        runnable = ConcreteRunnable()

        result = runnable.run()
        self.assertIsNone(
            runnable.future,
            msg="Without an executor, we expect no future"
        )
        self.assertDictEqual(
            runnable.expected_run_output,
            result,
            msg="Expected the result"
        )
        self.assertDictEqual(
            runnable.expected_processed_value,
            runnable.processed,
            msg="Expected the result, including post-processing 'bar' value"
        )

    def test_runnable_run_with_executor(self):
        runnable = ConcreteRunnable()

        def maybe_get_executor(get_executor):
            if get_executor:
                return CloudpickleProcessPoolExecutor()
            else:
                return "This should result in an error!"

        for label, executor in [
            ("Instance", CloudpickleProcessPoolExecutor()),
            ("Argument free instructions", (CloudpickleProcessPoolExecutor, (), {})),
            ("Argument instructions", (maybe_get_executor, (True,), {})),
        ]:
            with self.subTest(label):
                runnable.executor = executor

                result = runnable.run()
                self.assertIsInstance(
                    result,
                    Future,
                    msg="With an executor, a future should be returned"
                )
                self.assertIs(
                    result,
                    runnable.future,
                    msg="With an executor, the future attribute should get populated"
                )
                self.assertDictEqual(
                    runnable.expected_run_output,
                    result.result(timeout=30),
                    msg="Expected the result (after waiting for it to compute, of course)"
                )
                self.assertDictEqual(
                    runnable.expected_processed_value,
                    runnable.processed,
                    msg="Expected the result, including post-processing 'bar' value"
                )

        with self.assertRaises(
            NotImplementedError,
            msg="That's not an executor at all"
        ):
            runnable.executor = 42
            runnable.run()

        with self.assertRaises(
            TypeError,
            msg="Callables are ok, but if they don't return an executor we should get "
                "and error."
        ):
            runnable.executor = (maybe_get_executor, (False,), {})
            runnable.run()


if __name__ == '__main__':
    unittest.main()
