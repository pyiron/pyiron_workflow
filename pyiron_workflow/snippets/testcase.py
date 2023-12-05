# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""A test case that encourages you to test your docstrings."""

from contextlib import redirect_stdout
import doctest
from io import StringIO
import unittest
import os
from pyiron_base import PythonTemplateJob, state
from pyiron_base.project.generic import Project
from abc import ABC
from inspect import getfile
import numpy as np


__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Mar 23, 2021"


class PyironTestCase(unittest.TestCase, ABC):

    """
    Base class for all pyiron unit tets.

    Registers utility type equality functions:
        - np.testing.assert_array_equal

    Optionally includes testing the docstrings in the specified module by
    overloading :attr:`~.docstring_module`.
    """

    def setUp(self):
        self.addTypeEqualityFunc(np.ndarray, self._assert_equal_numpy)

    def _assert_equal_numpy(self, a, b, msg=None):
        try:
            np.testing.assert_array_equal(a, b, err_msg=msg)
        except AssertionError as e:
            raise self.failureException(*e.args) from None

    @classmethod
    def setUpClass(cls):
        cls._initial_settings_configuration = state.settings.configuration.copy()
        if any([cls is c for c in _TO_SKIP]):
            raise unittest.SkipTest(f"{cls.__name__} tests, it's a base class")
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        state.update(cls._initial_settings_configuration)

    @property
    def docstring_module(self):
        """
        Define module whose docstrings will be tested
        """
        return None

    def test_docstrings(self):
        """
        Fails with output if docstrings in the given module fails.

        Output capturing adapted from https://stackoverflow.com/a/22434594/12332968
        """
        with StringIO() as buf, redirect_stdout(buf):
            result = doctest.testmod(self.docstring_module)
            output = buf.getvalue()
        self.assertFalse(result.failed > 0, msg=output)