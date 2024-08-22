import doctest
import pkgutil
import sys
import unittest

import pyiron_workflow


def load_tests(loader, tests, ignore):
    for importer, name, ispkg in pkgutil.walk_packages(
        pyiron_workflow.__path__, pyiron_workflow.__name__ + '.'
    ):
        tests.addTests(doctest.DocTestSuite(name))
    return tests


class TestTriggerFromIDE(unittest.TestCase):
    """
    Just so we can instruct it to run unit tests here with a gui run command on the file
    """

    def test_void(self):
        pass


if __name__ == '__main__':
    unittest.main()
