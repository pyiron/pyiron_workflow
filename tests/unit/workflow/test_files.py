import unittest
from pyiron_contrib.workflow.files import DirectoryObject, FileObject
from pathlib import Path


class TestFiles(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.directory = DirectoryObject("test")

    def test_directory_exists(self):
        self.assertTrue(Path("test").exists() and Path("test").is_dir())

    def test_write(self):
        self.directory.write(file_name="test.txt", content="something")
        self.assertTrue(self.directory.file_exists("test.txt"))
        self.assertTrue("test/test.txt" in self.directory.list_content()['file'])
        self.assertEqual(len(self.directory), 1)

    def test_create_subdirectory(self):
        self.directory.create_subdirectory("another_test")
        self.assertTrue(Path("test/another_test").exists())

    def test_path(self):
        f = FileObject("test.txt", self.directory)
        self.assertEqual(str(f.path), "test/test.txt")

    def test_read_and_write(self):
        f = FileObject("test.txt", self.directory)
        f.write("something")
        self.assertEqual(f.read(), "something")

    @classmethod
    def tearDown(cls):
        cls.directory.delete()


if __name__ == '__main__':
    unittest.main()
