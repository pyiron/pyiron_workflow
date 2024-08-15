import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.standard import UserInput
from pyiron_workflow.storage import (
    available_backends,
    PickleStorage,
    TypeNotFoundError
)


class TestAvailableBackends(unittest.TestCase):

    def test_default_backend(self):
        backends = list(available_backends())
        self.assertIsInstance(
            backends[0],
            PickleStorage,
            msg="If more standard backends are added, this will fail -- that's fine, "
                "just update the test to make sure you're getting the defaults you now "
                "expect."
        )

    def test_specific_backend(self):
        backends = list(available_backends(backend="pickle", only_requested=True))
        self.assertEqual(
            len(backends),
            1,
            msg="Once more standard backends are available, we should test that string "
                "access results in the the correct priority assignment among these "
                "defaults."
        )
        self.assertIsInstance(backends[0], PickleStorage)

    def test_extra_backend(self):
        my_interface = PickleStorage()
        backends = list(available_backends(my_interface))
        self.assertEqual(
            len(backends),
            2,
            msg="We expect both the one we passed, and all defaults"
        )
        self.assertIs(backends[0], my_interface)
        self.assertIsNot(
            backends[0],
            backends[1],
            msg="They should be separate instances"
        )

    def test_exclusive_backend(self):
        my_interface = PickleStorage()
        backends = list(available_backends(my_interface, only_requested=True))
        self.assertEqual(
            len(backends),
            1,
            msg="We expect to filter out everything except the one we asked for"
        )
        self.assertIs(backends[0], my_interface)


class TestPickleStorage(unittest.TestCase):

    def setUp(self):
        self.node = UserInput(label="test_node")
        self.storage = PickleStorage()
        self.temp_dir = TemporaryDirectory()
        self.filename = Path(self.temp_dir.name) / "test_node"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_save_and_load(self):
        with self.subTest("By Node"):
            try:
                self.storage.save(self.node)
                self.assertTrue(self.storage.has_contents(node=self.node))
                loaded_node = self.storage.load(node=self.node)
                self.assertIsNot(loaded_node, self.node, msg="Should be a new instance")
                self.assertEqual(loaded_node.label, self.node.label)
            finally:
                self.storage.delete(node=self.node)
                self.assertFalse(self.storage.has_contents(node=self.node))

        with self.subTest("By filename"):
            try:
                self.storage.save(self.node, self.filename)
                self.assertFalse(self.storage.has_contents(node=self.node))
                self.assertTrue(self.storage.has_contents(filename=self.filename))
                loaded_node = self.storage.load(filename=self.filename)
                self.assertEqual(loaded_node.label, self.node.label)
            finally:
                self.storage.delete(filename=self.filename)
                self.assertFalse(self.storage.has_contents(filename=self.filename))

    def test_input_validity(self):
        for method in [
            self.storage.load,
            self.storage.has_contents,
            self.storage.delete
        ]:
            with self.subTest(method.__name__):
                with self.assertRaises(ValueError):
                    method(self.node, self.filename)
                with self.assertRaises(ValueError):
                    method(None, None)

        with self.assertRaises(ValueError):
            self.storage.save(None, None)


class TestPickleStorage(unittest.TestCase):
    def test_cloudpickle(self):
        @as_function_node
        def Unimportable(x):
            return x + 1

        u = Unimportable()

        try:
            interface = PickleStorage(cloudpickle_fallback=False)
            with self.assertRaises(
                TypeNotFoundError,
                msg="We can't import from <locals>, so this is unpicklable"
            ):
                interface.save(u)

            interface.save(u, cloudpickle_fallback=True)
            self.assertFalse(interface.has_contents(u))
            self.assertTrue(interface.has_contents(u, cloudpickle_fallback=True))

            new_u = interface.load(node=u, cloudpickle_fallback=True)
            self.assertIsInstance(new_u, Unimportable)
        finally:
            interface.delete(node=u, cloudpickle_fallback=True)


if __name__ == "__main__":
    unittest.main()

