"""Unit tests for :mod:`pyiron_workflow._wfms.api`."""

from __future__ import annotations

import importlib
import unittest

EXPECTED: list[tuple[str, str]] = [
    ("Atomic", "pyiron_workflow._wfms.atomic"),
    ("function2node", "pyiron_workflow._wfms.constructors"),
    ("Macro", "pyiron_workflow._wfms.dag"),
    ("Workflow", "pyiron_workflow._wfms.workflow"),
    ("ExecutorInstructions", "pyiron_workflow._wfms.execution"),
    ("Run", "pyiron_workflow._wfms.execution"),
    ("RunConfig", "pyiron_workflow._wfms.execution"),
    ("RunStatus", "pyiron_workflow._wfms.execution"),
    ("run", "pyiron_workflow._wfms.execution"),
    ("ForEach", "pyiron_workflow._wfms.flowcontrollers.foreach"),
    ("If", "pyiron_workflow._wfms.flowcontrollers.ifflow"),
    ("Try", "pyiron_workflow._wfms.flowcontrollers.tryflow"),
    ("While", "pyiron_workflow._wfms.flowcontrollers.whileflow"),
    ("Transform1toN", "pyiron_workflow._wfms.transformers"),
    ("TransformNto1", "pyiron_workflow._wfms.transformers"),
]


class TestApi(unittest.TestCase):
    def test_reexports_are_identical(self) -> None:
        api = importlib.import_module("pyiron_workflow._wfms.api")
        for name, src_mod_name in EXPECTED:
            with self.subTest(name=name):
                src = importlib.import_module(src_mod_name)
                self.assertIs(getattr(api, name), getattr(src, name), msg=name)

    def test_public_surface_is_exactly_expected(self) -> None:
        api = importlib.import_module("pyiron_workflow._wfms.api")
        public = {name for name in vars(api) if not name.startswith("_")}
        expected = {name for name, _ in EXPECTED}
        self.assertEqual(public, expected)


if __name__ == "__main__":
    unittest.main()
