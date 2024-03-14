import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future
from pyiron_workflow.run import Runnable, ReadinessError


class ConcreteRunnable(Runnable):
    def on_run(self, **kwargs):
        pass

    @property
    def run_args(self):
        return {}


class TestRunnable(unittest.TestCase):
    def test_runnable_not_ready(self):
        # Arrange
        runnable = ConcreteRunnable()
        runnable.ready = False

        # Act & Assert
        with self.assertRaises(ReadinessError):
            runnable.run()

    def test_runnable_run_local(self):
        # Arrange
        runnable = ConcreteRunnable()
        runnable.ready = True
        runnable.on_run = MagicMock(return_value="result")

        # Act
        result = runnable.run(force_local_execution=True)

        # Assert
        self.assertEqual(result, "result")

    def test_runnable_run_with_executor(self):
        # Arrange
        runnable = ConcreteRunnable()
        runnable.ready = True
        runnable.executor = MagicMock()
        runnable.on_run = MagicMock(return_value="result")

        # Act
        result = runnable.run(force_local_execution=False)

        # Assert
        self.assertEqual(result, "result")
        runnable.executor.submit.assert_called_once()

    def test_runnable_run_with_executor_and_callback(self):
        # Arrange
        def custom_callback(result):
            return f"Processed: {result}"

        runnable = ConcreteRunnable()
        runnable.ready = True
        runnable.executor = MagicMock()
        runnable.on_run = MagicMock(return_value="result")

        # Act
        result = runnable.run(force_local_execution=False, _finished_callback=custom_callback)

        # Assert
        self.assertEqual(result, "Processed: result")
        runnable.executor.submit.assert_called_once()


if __name__ == '__main__':
    unittest.main()
