from sys import version_info
import unittest


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestModule(unittest.TestCase):
    def test_single_point_of_entry(self):
        from pyiron_workflow import Workflow
        # That's it, let's just make sure the main class is available at the topmost
        # level
