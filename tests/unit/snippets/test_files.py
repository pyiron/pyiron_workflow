import unittest
from pyiron_workflow.snippets.files import DirectoryObject, FileObject
from pathlib import Path


class TestFiles(unittest.TestCase):
    def setUp(cls):
        cls.directory = DirectoryObject("test")

    def tearDown(cls):
        cls.directory.delete()

    def test_directory_instantiation(self):
        directory = DirectoryObject(Path("test"))
        self.assertEqual(directory.path, self.directory.path)
        directory = DirectoryObject(self.directory)
        self.assertEqual(directory.path, self.directory.path)

    def test_directory_exists(self):
        self.assertTrue(Path("test").exists() and Path("test").is_dir())

    def test_write(self):
        self.directory.write(file_name="test.txt", content="something")
        self.assertTrue(self.directory.file_exists("test.txt"))
        self.assertTrue(
            "test/test.txt" in [
                ff.replace("\\", "/")
                for ff in self.directory.list_content()['file']
            ]
        )
        self.assertEqual(len(self.directory), 1)

    def test_create_subdirectory(self):
        self.directory.create_subdirectory("another_test")
        self.assertTrue(Path("test/another_test").exists())

    def test_path(self):
        f = FileObject("test.txt", self.directory)
        self.assertEqual(str(f.path).replace("\\", "/"), "test/test.txt")

    def test_read_and_write(self):
        f = FileObject("test.txt", self.directory)
        f.write("something")
        self.assertEqual(f.read(), "something")

    def test_is_file(self):
        f = FileObject("test.txt", self.directory)
        self.assertFalse(f.is_file())
        f.write("something")
        self.assertTrue(f.is_file())
        f.delete()
        self.assertFalse(f.is_file())

    def test_is_empty(self):
        self.assertTrue(self.directory.is_empty())
        self.directory.write(file_name="test.txt", content="something")
        self.assertFalse(self.directory.is_empty())

    def test_delete(self):
        self.assertTrue(
            Path("test").exists() and Path("test").is_dir(),
            msg="Sanity check on initial state"
        )
        self.directory.write(file_name="test.txt", content="something")
        self.directory.delete(only_if_empty=True)
        self.assertFalse(
            self.directory.is_empty(),
            msg="Flag argument on delete should have prevented removal"
        )
        self.directory.delete()
        self.assertFalse(
            Path("test").exists(),
            msg="Delete should remove the entire directory"
        )
        self.directory = DirectoryObject("test")  # Rebuild it so the tearDown works

    def test_remove(self):
        self.directory.write(file_name="test1.txt", content="something")
        self.directory.write(file_name="test2.txt", content="something")
        self.directory.write(file_name="test3.txt", content="something")
        self.assertEqual(
            3,
            len(self.directory),
            msg="Sanity check on initial state"
        )
        self.directory.remove_files("test1.txt", "test2.txt")
        self.assertEqual(
            1,
            len(self.directory),
            msg="Should be able to remove multiple files at once",
        )
        self.directory.remove_files("not even there", "nor this")
        self.assertEqual(
            1,
            len(self.directory),
            msg="Removing non-existent things should have no effect",
        )
        self.directory.remove_files("test3.txt")
        self.assertEqual(
            0,
            len(self.directory),
            msg="Should be able to remove just one file",
        )

    def test_copy(self):
        f = FileObject("test_copy.txt", self.directory)
        f.write("sam wrote this wondrful thing")
        new_file_1 = f.copy("another_test")
        self.assertEqual(new_file_1.read(), "sam wrote this wondrful thing")
        new_file_2 = f.copy("another_test", ".")
        with open("another_test", "r") as file:
            txt = file.read()
        self.assertEqual(txt, "sam wrote this wondrful thing")
        new_file_2.delete()  # needed because current directory
        new_file_3 = f.copy(str(f.path.parent / "another_test"), ".")
        self.assertEqual(new_file_1.path.absolute(), new_file_3.path.absolute())


if __name__ == '__main__':
    unittest.main()
