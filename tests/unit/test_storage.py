import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import cloudpickle
from pint import UnitRegistry

from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.standard import UserInput
from pyiron_workflow.storage import (
    FileTypeError,
    H5BagStorage,
    PickleStorage,
    TypeNotFoundError,
    _standard_backends,
    available_backends,
)


class TestAvailableBackends(unittest.TestCase):
    def test_default_backend(self):
        backends = list(available_backends())
        for got, expected in zip(backends, _standard_backends.values(), strict=True):
            self.assertIsInstance(got, expected)

    def test_specific_backend(self):
        backends = list(available_backends(backend="pickle", only_requested=True))
        self.assertEqual(
            len(backends),
            1,
            msg="Once more standard backends are available, we should test that string "
            "access results in the the correct priority assignment among these "
            "defaults.",
        )
        self.assertIsInstance(backends[0], PickleStorage)

    def test_extra_backend(self):
        with self.subTest("String backend"):
            backends = list(available_backends("pickle"))
            print(backends)
            self.assertEqual(
                len(backends),
                len(_standard_backends),
                msg="We expect only the defaults",
            )
            self.assertIsInstance(backends[0], PickleStorage)

        with self.subTest("Object backend"):
            my_interface = PickleStorage()
            backends = list(available_backends(my_interface))
            self.assertEqual(
                len(backends),
                len(_standard_backends) + 1,
                msg="We expect both the one we passed, and all defaults",
            )
            self.assertIs(backends[0], my_interface)
            for other in backends[1:]:
                self.assertIsNot(
                    backends[0], other, msg="They should be separate instances"
                )

    def test_exclusive_backend(self):
        my_interface = PickleStorage()
        backends = list(available_backends(my_interface, only_requested=True))
        self.assertEqual(
            len(backends),
            1,
            msg="We expect to filter out everything except the one we asked for",
        )
        self.assertIs(backends[0], my_interface)


class TestStorage(unittest.TestCase):
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
                self.assertTrue(self.storage.has_saved_content(node=self.node))
                loaded_node = self.storage.load(node=self.node)
                self.assertIsNot(loaded_node, self.node, msg="Should be a new instance")
                self.assertEqual(loaded_node.label, self.node.label)
            finally:
                self.storage.delete(node=self.node)
                self.assertFalse(self.storage.has_saved_content(node=self.node))

        with self.subTest("By filename"):
            try:
                self.storage.save(self.node, self.filename)
                self.assertFalse(self.storage.has_saved_content(node=self.node))
                self.assertTrue(self.storage.has_saved_content(filename=self.filename))
                loaded_node = self.storage.load(filename=self.filename)
                self.assertEqual(loaded_node.label, self.node.label)
            finally:
                self.storage.delete(filename=self.filename)
                self.assertFalse(self.storage.has_saved_content(filename=self.filename))

    def test_delete(self):
        try:
            self.storage.save(self.node)
            with open(self.node.as_path() / "hello.txt", "w") as f:
                f.write("Hello, World!")
        finally:
            self.storage.delete(node=self.node)
            self.assertTrue((self.node.as_path() / "hello.txt").exists())
            self.storage.delete(node=self.node, delete_even_if_not_empty=True)
            self.assertFalse((self.node.as_path() / "hello.txt").exists())

    def test_input_validity(self):
        for method in [
            self.storage.load,
            self.storage.has_saved_content,
            self.storage.delete,
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
                msg="We can't import from <locals>, so this is unpicklable",
            ):
                interface.save(u)

            interface.save(u, cloudpickle_fallback=True)
            self.assertFalse(interface.has_saved_content(u))
            self.assertTrue(interface.has_saved_content(u, cloudpickle_fallback=True))

            new_u = interface.load(node=u, cloudpickle_fallback=True)
            self.assertIsInstance(new_u, Unimportable)
        finally:
            interface.delete(node=u, cloudpickle_fallback=True)

    def test_uncloudpickleable(self):
        ureg = UnitRegistry()
        with self.assertRaises(
            TypeError, msg="Sanity check that this can't even be cloudpickled"
        ):
            cloudpickle.dumps(ureg)

        interface = PickleStorage(cloudpickle_fallback=True)
        n = UserInput(ureg, label="uncloudpicklable_node")
        with self.assertRaises(
            TypeError, msg="Exception should be caught and saving should fail"
        ):
            interface.save(n)


class TestH5BagStorage(unittest.TestCase):
    def test_wrong_file_gives_no_content(self):
        p = Path("bad_file.h5")
        p.write_text("lorem ipsum")

        try:
            with self.assertRaises(FileTypeError):
                H5BagStorage().has_saved_content(filename=p)
        finally:
            p.unlink()


if __name__ == "__main__":
    unittest.main()
