from concurrent.futures import Future

import unittest

from pyiron_workflow.mixin.run import manage_status


class FauxNode:
    def __init__(self):
        self.running = False
        self.failed = False

    @manage_status
    def success(self, x):
        return x / 2

    @manage_status
    def failure(self):
        return 1 / 0

    @manage_status
    def future(self):
        return Future()


class TestStatusManagement(unittest.TestCase):
    def setUp(self) -> None:
        self.node = FauxNode()

    def test_success(self):
        out = self.node.success(4)
        self.assertFalse(self.node.running)
        self.assertFalse(self.node.failed)
        self.assertEqual(out, 2)

    def test_failure(self):
        with self.assertRaises(ZeroDivisionError):
            self.node.failure()
        self.assertFalse(self.node.running)
        self.assertTrue(self.node.failed)

    def test_future(self):
        out = self.node.future()
        self.assertTrue(self.node.running)
        self.assertFalse(self.node.failed)
        self.assertIsInstance(out, Future)


if __name__ == '__main__':
    unittest.main()
