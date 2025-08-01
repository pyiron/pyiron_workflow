import unittest

from pyiron_workflow import Workflow
from pyiron_workflow.node import Node


class TestOutputInjection(unittest.TestCase):
    """
    I.e. the process of inserting new nodes on-the-fly by modifying output channels"
    """

    def setUp(self) -> None:
        self.wf = Workflow("injection")
        self.int = Workflow.create.std.UserInput(42, autorun=True)
        self.list = Workflow.create.std.UserInput(list(range(10)), autorun=True)

    def test_equality(self):
        with self.subTest("True expressions"):
            for expression in [
                self.int < 100,
                self.int > 0,
                self.int <= 100,
                self.int <= 42,
                self.int >= 0,
                self.int.eq(42),
                self.int != 43,
                self.int > 0,
                self.int < 100,
                self.int >= 0,
                self.int <= 100,
                self.int >= 42,
            ]:
                with self.subTest(expression.label):
                    self.assertTrue(expression.value)

        with self.subTest("False expressions"):
            for expression in [
                self.int > 100,
                self.int < 0,
                self.int >= 100,
                self.int <= 0,
                self.int != 42,
                self.int.eq(43),
                self.int < 0,
                self.int > 100,
                self.int <= 0,
                self.int >= 100,
            ]:
                with self.subTest(expression.label):
                    self.assertFalse(expression.value)

    def test_bool(self):
        b = self.int.bool()
        self.assertTrue(b.value)
        self.int.inputs.user_input = False
        self.assertFalse(b())

    def test_len(self):
        self.assertEqual(10, self.list.len().value)

    def test_contains(self):
        self.assertTrue(self.list.contains(1).value)
        self.assertFalse(self.list.contains(-1).value)

    def test_algebra(self):
        x = self.int  # 42
        for lhs, rhs in [
            (x + x, 2 * x),
            (2 * x, x * 2),
            (x * x, x**2),
            (x - x, 0 * x),
            (x + x - x, x),
            (x / 42, x / x),
            (x // 2, x / 2),
            (x // 43, 0 * x),
            ((x + 1) % x, x + 1 - x),
            (-x, -1 * x),
            (+x, (-x) ** 2 / x),
            (x, abs(-x)),
        ]:
            with self.subTest(f"{lhs.label} == {rhs.label}"):
                self.assertEqual(lhs.value, rhs.value)

    def test_logic(self):
        # Note: We can't invert with not etc. because overloading __bool__ does not work
        self.true = Workflow.create.std.UserInput(True, autorun=True)
        self.false = Workflow.create.std.UserInput(False, autorun=True)

        with self.subTest("True expressions"):
            for expression in [
                self.true & True,
                # True & self.true,  # There's no __land__ etc.
                self.true & self.true,
                self.true ^ False,
                # False ^ self.true,
                self.true ^ self.false,
                self.false ^ self.true,
                self.true | False,
                self.true | self.false,
                self.false | self.true,
                self.false | False | self.true,
                # False | self.true,
            ]:
                with self.subTest(expression.label):
                    self.assertTrue(expression.value)

        with self.subTest("False expressions"):
            for expression in [
                self.true & False,
                self.false & self.false,
                self.false & self.true,
                self.true & self.false,
                self.true ^ self.true,
                self.false ^ self.false,
                self.false | self.false,
                self.false | False,
            ]:
                with self.subTest(expression.label):
                    self.assertFalse(expression.value)

    def test_casts(self):
        self.float = Workflow.create.std.UserInput(42.2, autorun=True)

        self.assertIsInstance(self.int.float().value, float)
        self.assertIsInstance(self.float.int().value, int)
        self.assertEqual(self.int.value, round(self.float).value)

    def test_access(self):
        self.dict = Workflow.create.std.UserInput({"foo": 42}, autorun=True)

        class Something:
            myattr = 1

        self.obj = Workflow.create.std.UserInput(Something(), autorun=True)

        self.assertIsInstance(self.list[0].value, int)
        self.assertEqual(5, self.list[:5].len().value)
        self.assertEqual(4, self.list[1:5].len().value)
        self.assertEqual(3, self.list[-3:].len().value)
        self.assertEqual(2, self.list[1:5:2].len().value)

        self.assertEqual(42, self.dict["foo"].value)
        self.assertEqual(1, self.obj.myattr.value)

    def test_chaining(self):
        self.assertFalse((self.list[: self.int // 42][0] != 0).value)

    def test_repeated_access_in_parent_scope(self):
        wf = Workflow("output_manipulation")
        wf.list = Workflow.create.std.UserInput(list(range(10)))

        a = wf.list[:4]
        b = wf.list[:4]
        c = wf.list[1:]

        self.assertIs(
            a,
            b,
            msg="The same operation should re-access an existing node in the parent",
        )
        self.assertIsNot(a, c, msg="Unique operations should yield unique nodes")

    def test_without_parent(self):
        d1 = self.list[5]
        d2 = self.list[5]

        self.assertIsInstance(d1, Node)
        self.assertIsNot(
            d1,
            d2,
            msg="Outside the scope of a parent, we can't expect to re-access an "
            "equivalent node",
        )
        self.assertEqual(
            d1.label,
            d2.label,
            msg="Equivalent operations should nonetheless generate equal labels",
        )

    def test_shape_access(self):
        with self.assertRaises(
            AttributeError,
            msg="This is a hack to stop Jupyter cells from injecting getattr nodes",
        ):
            self.int.shape  # noqa: B018


if __name__ == "__main__":
    unittest.main()
