import unittest
import pyiron_workflow


class TestVersion(unittest.TestCase):
    def test_version(self):
        version = pyiron_workflow.__version__
        print(version)
        self.assertTrue(version.startswith('0'))
