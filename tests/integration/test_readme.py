import doctest
import unittest


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocFileSuite("../../docs/README.md"))
    return tests


class TestTriggerFromIDE(unittest.TestCase):
    """
    Just so we can instruct it to run unit tests here with a gui run command on the file
    """

    def test_void(self):
        pass


if __name__ == "__main__":
    unittest.main()
