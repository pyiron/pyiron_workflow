from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import constant, execution

# --------------------------------------------------------------------------- #
# Tiny recipe builder.                                                        #
# --------------------------------------------------------------------------- #


def _constant_recipe(value: object) -> fr.schemas.ConstantRecipe:
    return fr.schemas.ConstantRecipe(constant=value)


# --------------------------------------------------------------------------- #
# Constant._result_type / evaluate                                            #
# --------------------------------------------------------------------------- #


class TestConstantResultType(unittest.TestCase):
    def test_result_type_is_live_constant(self) -> None:
        self.assertIs(constant.Constant._result_type(), fr.schemas.ConstantData)


class TestConstantEvaluate(unittest.TestCase):
    def test_evaluate_emits_value_on_constant_port(self) -> None:
        # Any JSON-able value survives the recipe -> data round-trip and lands
        # on the single output port named `constant`.
        for value in (42, "hi", [1, 2, 3], {"a": [1, 2]}):
            with self.subTest(value=value):
                node = constant.Constant("c", _constant_recipe(value))
                run = node.run()
                self.assertEqual(run.status, execution.RunStatus.FINISHED)
                self.assertEqual(run.outputs["constant"].value, value)


if __name__ == "__main__":
    unittest.main()
