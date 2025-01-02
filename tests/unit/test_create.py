import unittest

from pyiron_workflow.create import Creator


class TestCreator(unittest.TestCase):
    def test_instantiate(self):
        Creator()


if __name__ == "__main__":
    unittest.main()
