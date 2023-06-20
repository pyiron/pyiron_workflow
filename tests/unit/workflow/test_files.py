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

    @classmethod
    def tearDown(cls):
        cls.directory.delete()


if __name__ == '__main__':
    unittest.main()
