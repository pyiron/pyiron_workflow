import unittest
import pyiron_module_template


class TestVersion(unittest.TestCase):
    def test_version(self):
        version = pyiron_module_template.__version__
        print(version)
        self.assertTrue(version.startswith('0'))
