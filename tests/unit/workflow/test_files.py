import unittest
from pyiron_contrib.workflow.files import DirectoryObject, FileObject
from pathlib import Path


class TestFiles(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = DirectoryObject("test")

    def test_directory_exists(self):
        self.assertTrue(Path("test").exists() and Path("test").is_dir())

    def test_write(self):
        self.directory.write(file_name="test.txt", content="something")
        self.assertEqual(len(self.directory), 1)

    @classmethod
    def tearDownClass(cls):
        cls.directory.delete()


if __name__ == '__main__':
    unittest.main()
