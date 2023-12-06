import doctest
import pkgutil
import unittest

import pyiron_workflow


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite("pyiron_workflow.executors.cloudpickleprocesspool"))
    for importer, name, ispkg in pkgutil.walk_packages(
        pyiron_workflow.__path__, pyiron_workflow.__name__ + '.'
    ):
        tests.addTests(doctest.DocTestSuite(name))
    return tests


class TestTriggerFromPycharm(unittest.TestCase):
    """Just so I can instruct it to run unit tests here"""

    def test_void(self):
        pass


if __name__ == '__main__':
    unittest.main()
