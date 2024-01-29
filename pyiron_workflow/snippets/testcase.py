"""
A test case that encourages you to test your docstrings and makes it easier to test
numpy arrays (if numpy is available).
"""

from abc import ABC
from contextlib import redirect_stdout
import doctest
from io import StringIO
from types import ModuleType
import unittest

try:
    import numpy as np
except ModuleNotFoundError:
    np = None


__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "liamhuber@greyhavensolutions.com"
__status__ = "production"
__date__ = "Dec 5, 2023"


class PyironTestCase(unittest.TestCase, ABC):
    """
    Base class for all pyiron unit tets.

    If numpy is avaiable, registers utility type equality function:

        - `np.testing.assert_array_equal`

    Demands that you provide information on modules(s) for docstring testing, but
    allows you to get around this by explicitly setting `None`.

    Remember to call `super()` on `setUp` and `setUpClass`!
    """

    docstring_modules: list[ModuleType] | tuple[ModuleType] | ModuleType | None = None

    def _assert_equal_numpy(self, a, b, msg=None):
        try:
            np.testing.assert_array_equal(a, b, err_msg=msg)
        except AssertionError as e:
            raise self.failureException(*e.args) from None

    def setUp(self) -> None:
        super().setUp()
        if np is not None:
            self.addTypeEqualityFunc(np.ndarray, self._assert_equal_numpy)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if any([cls is c for c in _TO_SKIP]):
            raise unittest.SkipTest(f"{cls.__name__} tests, it's a base test class")

    def test_docstrings(self):
        """
        Fails with output if docstrings in the given module fails.

        Output capturing adapted from https://stackoverflow.com/a/22434594/12332968
        """
        docstring_modules = (
            [self.docstring_modules]
            if isinstance(self.docstring_modules, ModuleType)
            else self.docstring_modules
        )
        if docstring_modules is not None:
            for mod in docstring_modules:
                with self.subTest(f"Testing docs in {mod}"):
                    with StringIO() as buf, redirect_stdout(buf):
                        result = doctest.testmod(mod)
                        output = buf.getvalue()
                    self.assertFalse(result.failed > 0, msg=output)


_TO_SKIP = [
    PyironTestCase,
]
