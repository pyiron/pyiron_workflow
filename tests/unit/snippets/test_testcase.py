import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.snippets.testcase import PyironTestCase


ensure_tests_in_python_path()
from static import docs_submodule
from static.docs_submodule import bad_class, good_function, mix, bad_init_example


class TestTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.tester = PyironTestCase()

    def test_none(self):
        self.tester.test_docstrings()

    def test_function_and_pass(self):
        self.tester.docstring_modules = good_function
        self.tester.test_docstrings()

    def test_class_and_fail(self):
        self.tester.docstring_modules = bad_class
        with self.assertRaises(
            AssertionError,
            msg="Expect to fail due to misalignment of code and output"
        ):
            self.tester.test_docstrings()

    def test_mix(self):
        self.tester.docstring_modules = mix
        with self.assertRaises(
            AssertionError,
            msg="If _any_ docstring in the module fails, expect to fail"
        ):
            self.tester.test_docstrings()

    def test_bad_init(self):
        self.tester.docstring_modules = docs_submodule.bad_init_example
        with self.assertRaises(
            AssertionError,
            msg="Pointing to a module should test its __init__"
        ):
            self.tester.test_docstrings()

    def test_good_init(self):
        """
        Note that the docstring tests don't recurse on the module, so even though this
        module has failing sub-modules, it's __init__ is fine, so it passes.
        """
        self.tester.docstring_modules = docs_submodule
        self.tester.test_docstrings()

    def test_multiple(self):
        with self.subTest("All good"):
            self.tester.docstring_modules = [docs_submodule, good_function]
            self.tester.test_docstrings()

        with self.subTest("At least one bad"):
            self.tester.docstring_modules = [bad_class, good_function]
            with self.assertRaises(
                AssertionError,
                msg="Any failure should cause overall failure"
            ):
                self.tester.test_docstrings()


if __name__ == '__main__':
    unittest.main()
