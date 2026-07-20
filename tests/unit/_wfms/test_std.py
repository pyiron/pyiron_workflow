from __future__ import annotations

import unittest

import flowrep as fr

from pyiron_workflow._wfms import constructors


def _return_42(*args, **kwargs):
    if args:
        if kwargs:
            return 42, args, kwargs
        return 42, args
    elif kwargs:
        return 42, kwargs
    return 42


class _Mat:
    def __init__(self, v):
        self.v = v

    def __matmul__(self, other):
        return ("mm", self.v, other.v)

    def __imatmul__(self, other):
        return ("imm", self.v, other.v)


class TestStdExecution(unittest.TestCase):

    def test_abs(self):
        n = constructors.recipe2node(fr.std.abs.flowrep_recipe)
        self.assertEqual(3, n.run(a=-3).outputs["absolute"].value)

    def test_add(self):
        n = constructors.recipe2node(fr.std.add.flowrep_recipe)
        self.assertEqual(3, n.run(a=1, b=2).outputs["added"].value)

    def test_index(self):
        n = constructors.recipe2node(fr.std.index.flowrep_recipe)
        self.assertEqual(5, n.run(a=5).outputs["index"].value)

    def test_inv(self):
        n = constructors.recipe2node(fr.std.inv.flowrep_recipe)
        self.assertEqual(-1, n.run(a=0).outputs["inverted"].value)

    def test_invert(self):
        n = constructors.recipe2node(fr.std.invert.flowrep_recipe)
        self.assertEqual(-1, n.run(a=0).outputs["inverted"].value)

    def test_neg(self):
        n = constructors.recipe2node(fr.std.neg.flowrep_recipe)
        self.assertEqual(-2, n.run(a=2).outputs["negative"].value)

    def test_pos(self):
        n = constructors.recipe2node(fr.std.pos.flowrep_recipe)
        self.assertEqual(-2, n.run(a=-2).outputs["positive"].value)

    def test_not_(self):
        n = constructors.recipe2node(fr.std.not_.flowrep_recipe)
        self.assertEqual(True, n.run(a=False).outputs["negated"].value)

    def test_truth(self):
        n = constructors.recipe2node(fr.std.truth.flowrep_recipe)
        self.assertEqual(False, n.run(a=[]).outputs["truth"].value)

    def test_length_hint(self):
        n = constructors.recipe2node(fr.std.length_hint.flowrep_recipe)
        self.assertEqual(3, n.run(obj=[1, 2, 3]).outputs["length"].value)

    def test_sub(self):
        n = constructors.recipe2node(fr.std.sub.flowrep_recipe)
        self.assertEqual(3, n.run(a=5, b=2).outputs["difference"].value)

    def test_isub(self):
        n = constructors.recipe2node(fr.std.isub.flowrep_recipe)
        self.assertEqual(3, n.run(a=5, b=2).outputs["difference"].value)

    def test_iadd(self):
        n = constructors.recipe2node(fr.std.iadd.flowrep_recipe)
        self.assertEqual(3, n.run(a=1, b=2).outputs["added"].value)

    def test_mul(self):
        n = constructors.recipe2node(fr.std.mul.flowrep_recipe)
        self.assertEqual(6, n.run(a=2, b=3).outputs["product"].value)

    def test_imul(self):
        n = constructors.recipe2node(fr.std.imul.flowrep_recipe)
        self.assertEqual(6, n.run(a=2, b=3).outputs["product"].value)

    def test_floordiv(self):
        n = constructors.recipe2node(fr.std.floordiv.flowrep_recipe)
        self.assertEqual(3, n.run(a=7, b=2).outputs["quotient"].value)

    def test_ifloordiv(self):
        n = constructors.recipe2node(fr.std.ifloordiv.flowrep_recipe)
        self.assertEqual(3, n.run(a=7, b=2).outputs["quotient"].value)

    def test_truediv(self):
        n = constructors.recipe2node(fr.std.truediv.flowrep_recipe)
        self.assertEqual(3.0, n.run(a=6, b=2).outputs["quotient"].value)

    def test_itruediv(self):
        n = constructors.recipe2node(fr.std.itruediv.flowrep_recipe)
        self.assertEqual(3.0, n.run(a=6, b=2).outputs["quotient"].value)

    def test_mod(self):
        n = constructors.recipe2node(fr.std.mod.flowrep_recipe)
        self.assertEqual(1, n.run(a=7, b=3).outputs["remainder"].value)

    def test_imod(self):
        n = constructors.recipe2node(fr.std.imod.flowrep_recipe)
        self.assertEqual(1, n.run(a=7, b=3).outputs["remainder"].value)

    def test_pow(self):
        n = constructors.recipe2node(fr.std.pow.flowrep_recipe)
        self.assertEqual(8, n.run(a=2, b=3).outputs["power"].value)

    def test_ipow(self):
        n = constructors.recipe2node(fr.std.ipow.flowrep_recipe)
        self.assertEqual(8, n.run(a=2, b=3).outputs["power"].value)

    def test_and_(self):
        n = constructors.recipe2node(fr.std.and_.flowrep_recipe)
        self.assertEqual(2, n.run(a=6, b=3).outputs["conjunction"].value)

    def test_iand(self):
        n = constructors.recipe2node(fr.std.iand.flowrep_recipe)
        self.assertEqual(2, n.run(a=6, b=3).outputs["conjunction"].value)

    def test_or_(self):
        n = constructors.recipe2node(fr.std.or_.flowrep_recipe)
        self.assertEqual(7, n.run(a=6, b=1).outputs["disjunction"].value)

    def test_ior(self):
        n = constructors.recipe2node(fr.std.ior.flowrep_recipe)
        self.assertEqual(7, n.run(a=6, b=1).outputs["disjunction"].value)

    def test_xor(self):
        n = constructors.recipe2node(fr.std.xor.flowrep_recipe)
        self.assertEqual(5, n.run(a=6, b=3).outputs["exclusive_or"].value)

    def test_ixor(self):
        n = constructors.recipe2node(fr.std.ixor.flowrep_recipe)
        self.assertEqual(5, n.run(a=6, b=3).outputs["exclusive_or"].value)

    def test_lshift(self):
        n = constructors.recipe2node(fr.std.lshift.flowrep_recipe)
        self.assertEqual(8, n.run(a=1, b=3).outputs["left_shifted"].value)

    def test_ilshift(self):
        n = constructors.recipe2node(fr.std.ilshift.flowrep_recipe)
        self.assertEqual(8, n.run(a=1, b=3).outputs["left_shifted"].value)

    def test_rshift(self):
        n = constructors.recipe2node(fr.std.rshift.flowrep_recipe)
        self.assertEqual(2, n.run(a=8, b=2).outputs["right_shifted"].value)

    def test_irshift(self):
        n = constructors.recipe2node(fr.std.irshift.flowrep_recipe)
        self.assertEqual(2, n.run(a=8, b=2).outputs["right_shifted"].value)

    def test_matmul(self):
        n = constructors.recipe2node(fr.std.matmul.flowrep_recipe)
        result = n.run(a=_Mat(1), b=_Mat(2)).outputs["matrix_product"].value
        self.assertEqual(("mm", 1, 2), result)

    def test_imatmul(self):
        n = constructors.recipe2node(fr.std.imatmul.flowrep_recipe)
        result = n.run(a=_Mat(1), b=_Mat(2)).outputs["matrix_product"].value
        self.assertEqual(("imm", 1, 2), result)

    def test_eq(self):
        n = constructors.recipe2node(fr.std.eq.flowrep_recipe)
        self.assertEqual(True, n.run(a=1, b=1).outputs["equal"].value)

    def test_ne(self):
        n = constructors.recipe2node(fr.std.ne.flowrep_recipe)
        self.assertEqual(True, n.run(a=1, b=2).outputs["not_equal"].value)

    def test_lt(self):
        n = constructors.recipe2node(fr.std.lt.flowrep_recipe)
        self.assertEqual(True, n.run(a=1, b=2).outputs["less"].value)

    def test_le(self):
        n = constructors.recipe2node(fr.std.le.flowrep_recipe)
        self.assertEqual(True, n.run(a=2, b=2).outputs["less_equal"].value)

    def test_gt(self):
        n = constructors.recipe2node(fr.std.gt.flowrep_recipe)
        self.assertEqual(True, n.run(a=2, b=1).outputs["greater"].value)

    def test_ge(self):
        n = constructors.recipe2node(fr.std.ge.flowrep_recipe)
        self.assertEqual(True, n.run(a=2, b=2).outputs["greater_equal"].value)

    def test_is_(self):
        n = constructors.recipe2node(fr.std.is_.flowrep_recipe)
        self.assertEqual(True, n.run(a=None, b=None).outputs["identical"].value)

    def test_is_not(self):
        n = constructors.recipe2node(fr.std.is_not.flowrep_recipe)
        self.assertEqual(True, n.run(a=1, b=2).outputs["not_identical"].value)

    def test_contains(self):
        n = constructors.recipe2node(fr.std.contains.flowrep_recipe)
        self.assertEqual(True, n.run(a=[1, 2, 3], b=2).outputs["contains"].value)

    def test_countOf(self):
        n = constructors.recipe2node(fr.std.countOf.flowrep_recipe)
        self.assertEqual(2, n.run(a=[1, 2, 2, 3], b=2).outputs["count"].value)

    def test_indexOf(self):
        n = constructors.recipe2node(fr.std.indexOf.flowrep_recipe)
        self.assertEqual(2, n.run(a=[1, 2, 3], b=3).outputs["index"].value)

    def test_concat(self):
        n = constructors.recipe2node(fr.std.concat.flowrep_recipe)
        self.assertEqual([1, 2], n.run(a=[1], b=[2]).outputs["concatenated"].value)

    def test_iconcat(self):
        n = constructors.recipe2node(fr.std.iconcat.flowrep_recipe)
        self.assertEqual([1, 2], n.run(a=[1], b=[2]).outputs["concatenated"].value)

    def test_getitem(self):
        n = constructors.recipe2node(fr.std.getitem.flowrep_recipe)
        self.assertEqual(20, n.run(a=[10, 20], b=1).outputs["item"].value)

    def test_setitem(self):
        d = {}
        n = constructors.recipe2node(fr.std.setitem.flowrep_recipe)
        n.run(a=d, b="k", c=1)
        self.assertEqual(1, d["k"])

    def test_delitem(self):
        d = {"k": 1}
        n = constructors.recipe2node(fr.std.delitem.flowrep_recipe)
        n.run(a=d, b="k")
        self.assertNotIn("k", d)

    def test_call(self):
        n = constructors.recipe2node(fr.std.call.flowrep_recipe)
        with self.subTest("no variadics"):
            self.assertEqual(42, n.run(obj=_return_42).outputs["result"].value)
        with self.subTest("args"):
            self.assertEqual(
                (42, (1, 2)),
                n.run(obj=_return_42, args_=(1, 2)).outputs["result"].value,
            )
        with self.subTest("kwargs"):
            self.assertEqual(
                (42, {"a": 3, "b": 4}),
                n.run(obj=_return_42, kwargs_={"a": 3, "b": 4}).outputs["result"].value,
            )
        with self.subTest("both"):
            self.assertEqual(
                (42, (1, 2), {"a": 3, "b": 4}),
                n.run(obj=_return_42, args_=(1, 2), kwargs_={"a": 3, "b": 4})
                .outputs["result"]
                .value,
            )


if __name__ == "__main__":
    unittest.main()
