from __future__ import annotations

import unittest

from pyiron_workflow._wfms import constructors, std


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
        n = constructors.recipe2node(std.abs.node)
        self.assertEqual(3, n.run(a=-3).outputs["absolute"].value)

    def test_add(self):
        n = constructors.recipe2node(std.add.node)
        self.assertEqual(3, n.run(a=1, b=2).outputs["added"].value)

    def test_index(self):
        n = constructors.recipe2node(std.index.node)
        self.assertEqual(5, n.run(a=5).outputs["index"].value)

    def test_inv(self):
        n = constructors.recipe2node(std.inv.node)
        self.assertEqual(-1, n.run(a=0).outputs["inverted"].value)

    def test_invert(self):
        n = constructors.recipe2node(std.invert.node)
        self.assertEqual(-1, n.run(a=0).outputs["inverted"].value)

    def test_neg(self):
        n = constructors.recipe2node(std.neg.node)
        self.assertEqual(-2, n.run(a=2).outputs["negative"].value)

    def test_pos(self):
        n = constructors.recipe2node(std.pos.node)
        self.assertEqual(-2, n.run(a=-2).outputs["positive"].value)

    def test_not_(self):
        n = constructors.recipe2node(std.not_.node)
        self.assertEqual(True, n.run(a=False).outputs["negated"].value)

    def test_truth(self):
        n = constructors.recipe2node(std.truth.node)
        self.assertEqual(False, n.run(a=[]).outputs["truth"].value)

    def test_length_hint(self):
        n = constructors.recipe2node(std.length_hint.node)
        self.assertEqual(3, n.run(obj=[1, 2, 3]).outputs["length"].value)

    def test_sub(self):
        n = constructors.recipe2node(std.sub.node)
        self.assertEqual(3, n.run(a=5, b=2).outputs["difference"].value)

    def test_isub(self):
        n = constructors.recipe2node(std.isub.node)
        self.assertEqual(3, n.run(a=5, b=2).outputs["difference"].value)

    def test_iadd(self):
        n = constructors.recipe2node(std.iadd.node)
        self.assertEqual(3, n.run(a=1, b=2).outputs["added"].value)

    def test_mul(self):
        n = constructors.recipe2node(std.mul.node)
        self.assertEqual(6, n.run(a=2, b=3).outputs["product"].value)

    def test_imul(self):
        n = constructors.recipe2node(std.imul.node)
        self.assertEqual(6, n.run(a=2, b=3).outputs["product"].value)

    def test_floordiv(self):
        n = constructors.recipe2node(std.floordiv.node)
        self.assertEqual(3, n.run(a=7, b=2).outputs["quotient"].value)

    def test_ifloordiv(self):
        n = constructors.recipe2node(std.ifloordiv.node)
        self.assertEqual(3, n.run(a=7, b=2).outputs["quotient"].value)

    def test_truediv(self):
        n = constructors.recipe2node(std.truediv.node)
        self.assertEqual(3.0, n.run(a=6, b=2).outputs["quotient"].value)

    def test_itruediv(self):
        n = constructors.recipe2node(std.itruediv.node)
        self.assertEqual(3.0, n.run(a=6, b=2).outputs["quotient"].value)

    def test_mod(self):
        n = constructors.recipe2node(std.mod.node)
        self.assertEqual(1, n.run(a=7, b=3).outputs["remainder"].value)

    def test_imod(self):
        n = constructors.recipe2node(std.imod.node)
        self.assertEqual(1, n.run(a=7, b=3).outputs["remainder"].value)

    def test_pow(self):
        n = constructors.recipe2node(std.pow.node)
        self.assertEqual(8, n.run(a=2, b=3).outputs["power"].value)

    def test_ipow(self):
        n = constructors.recipe2node(std.ipow.node)
        self.assertEqual(8, n.run(a=2, b=3).outputs["power"].value)

    def test_and_(self):
        n = constructors.recipe2node(std.and_.node)
        self.assertEqual(2, n.run(a=6, b=3).outputs["conjunction"].value)

    def test_iand(self):
        n = constructors.recipe2node(std.iand.node)
        self.assertEqual(2, n.run(a=6, b=3).outputs["conjunction"].value)

    def test_or_(self):
        n = constructors.recipe2node(std.or_.node)
        self.assertEqual(7, n.run(a=6, b=1).outputs["disjunction"].value)

    def test_ior(self):
        n = constructors.recipe2node(std.ior.node)
        self.assertEqual(7, n.run(a=6, b=1).outputs["disjunction"].value)

    def test_xor(self):
        n = constructors.recipe2node(std.xor.node)
        self.assertEqual(5, n.run(a=6, b=3).outputs["exclusive_or"].value)

    def test_ixor(self):
        n = constructors.recipe2node(std.ixor.node)
        self.assertEqual(5, n.run(a=6, b=3).outputs["exclusive_or"].value)

    def test_lshift(self):
        n = constructors.recipe2node(std.lshift.node)
        self.assertEqual(8, n.run(a=1, b=3).outputs["left_shifted"].value)

    def test_ilshift(self):
        n = constructors.recipe2node(std.ilshift.node)
        self.assertEqual(8, n.run(a=1, b=3).outputs["left_shifted"].value)

    def test_rshift(self):
        n = constructors.recipe2node(std.rshift.node)
        self.assertEqual(2, n.run(a=8, b=2).outputs["right_shifted"].value)

    def test_irshift(self):
        n = constructors.recipe2node(std.irshift.node)
        self.assertEqual(2, n.run(a=8, b=2).outputs["right_shifted"].value)

    def test_matmul(self):
        n = constructors.recipe2node(std.matmul.node)
        result = n.run(a=_Mat(1), b=_Mat(2)).outputs["matrix_product"].value
        self.assertEqual(("mm", 1, 2), result)

    def test_imatmul(self):
        n = constructors.recipe2node(std.imatmul.node)
        result = n.run(a=_Mat(1), b=_Mat(2)).outputs["matrix_product"].value
        self.assertEqual(("imm", 1, 2), result)

    def test_eq(self):
        n = constructors.recipe2node(std.eq.node)
        self.assertEqual(True, n.run(a=1, b=1).outputs["equal"].value)

    def test_ne(self):
        n = constructors.recipe2node(std.ne.node)
        self.assertEqual(True, n.run(a=1, b=2).outputs["not_equal"].value)

    def test_lt(self):
        n = constructors.recipe2node(std.lt.node)
        self.assertEqual(True, n.run(a=1, b=2).outputs["less"].value)

    def test_le(self):
        n = constructors.recipe2node(std.le.node)
        self.assertEqual(True, n.run(a=2, b=2).outputs["less_equal"].value)

    def test_gt(self):
        n = constructors.recipe2node(std.gt.node)
        self.assertEqual(True, n.run(a=2, b=1).outputs["greater"].value)

    def test_ge(self):
        n = constructors.recipe2node(std.ge.node)
        self.assertEqual(True, n.run(a=2, b=2).outputs["greater_equal"].value)

    def test_is_(self):
        n = constructors.recipe2node(std.is_.node)
        self.assertEqual(True, n.run(a=None, b=None).outputs["identical"].value)

    def test_is_not(self):
        n = constructors.recipe2node(std.is_not.node)
        self.assertEqual(True, n.run(a=1, b=2).outputs["not_identical"].value)

    def test_contains(self):
        n = constructors.recipe2node(std.contains.node)
        self.assertEqual(True, n.run(a=[1, 2, 3], b=2).outputs["contains"].value)

    def test_countOf(self):
        n = constructors.recipe2node(std.countOf.node)
        self.assertEqual(2, n.run(a=[1, 2, 2, 3], b=2).outputs["count"].value)

    def test_indexOf(self):
        n = constructors.recipe2node(std.indexOf.node)
        self.assertEqual(2, n.run(a=[1, 2, 3], b=3).outputs["index"].value)

    def test_concat(self):
        n = constructors.recipe2node(std.concat.node)
        self.assertEqual([1, 2], n.run(a=[1], b=[2]).outputs["concatenated"].value)

    def test_iconcat(self):
        n = constructors.recipe2node(std.iconcat.node)
        self.assertEqual([1, 2], n.run(a=[1], b=[2]).outputs["concatenated"].value)

    def test_getitem(self):
        n = constructors.recipe2node(std.getitem.node)
        self.assertEqual(20, n.run(a=[10, 20], b=1).outputs["item"].value)

    def test_setitem(self):
        d = {}
        n = constructors.recipe2node(std.setitem.node)
        n.run(a=d, b="k", c=1)
        self.assertEqual(1, d["k"])

    def test_delitem(self):
        d = {"k": 1}
        n = constructors.recipe2node(std.delitem.node)
        n.run(a=d, b="k")
        self.assertNotIn("k", d)

    def test_attrgetter(self):
        n = constructors.recipe2node(std.attrgetter.node)
        getter = n.run(attr="real").outputs["getter"].value
        self.assertEqual(5, getter(5))

    def test_itemgetter(self):
        n = constructors.recipe2node(std.itemgetter.node)
        getter = n.run(item=0).outputs["getter"].value
        self.assertEqual(10, getter([10, 20]))

    def test_methodcaller(self):
        n = constructors.recipe2node(std.methodcaller.node)
        caller = n.run(name="upper").outputs["caller"].value
        self.assertEqual("HI", caller("hi"))

    def test_call(self):
        n = constructors.recipe2node(std.call.node)
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
