from concurrent.futures import Future
from sys import version_info
from unittest import TestCase, skipUnless

from pyiron_workflow.node import manage_status


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


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestStatusManagement(TestCase):
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
