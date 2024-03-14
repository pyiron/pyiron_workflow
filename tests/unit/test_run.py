from concurrent.futures import Future
import unittest

from pyiron_workflow.executors.cloudpickleprocesspool import (
    CloudpickleProcessPoolExecutor
)
from pyiron_workflow.run import Runnable, ReadinessError


class ConcreteRunnable(Runnable):
    @property
    def label(self) -> str:
        return "child_class_with_all_methods_implemented"

    def on_run(self, **kwargs):
        return kwargs

    @property
    def run_args(self):
        return {"foo": 42}

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

    def test_runnable_run_local(self):
        runnable = ConcreteRunnable()
        runnable.executor = CloudpickleProcessPoolExecutor()

        result = runnable.run(force_local_execution=True)
        self.assertIsNone(
            runnable.future,
            msg="The local execution flag should override the executor"
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
        runnable.executor = CloudpickleProcessPoolExecutor()

        result = runnable.run(force_local_execution=False)
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

    def test_runnable_run_with_executor_and_callback(self):
        runnable = ConcreteRunnable()
        runnable.executor = CloudpickleProcessPoolExecutor()

        result = runnable.run(
            force_local_execution=False,
            _finished_callback=runnable.custom_callback,
        )
        self.assertDictEqual(
            runnable.expected_run_output,
            result.result(timeout=30),
            msg="Callback does not impact the actual run function"
        )
        self.assertDictEqual(
            runnable.expected_custom_callback_processed_value,
            runnable.processed
        )


if __name__ == '__main__':
    unittest.main()
