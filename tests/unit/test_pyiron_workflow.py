
import unittest


class TestModule(unittest.TestCase):
    def test_single_point_of_entry(self):
        from pyiron_workflow import Workflow
        # That's it, let's just make sure the main class is available at the topmost
        # level


if __name__ == '__main__':
    unittest.main()
