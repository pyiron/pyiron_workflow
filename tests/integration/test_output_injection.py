import unittest

from pyiron_workflow import Workflow
from pyiron_workflow.node import Node


class TestOutputInjection(unittest.TestCase):
    """
    I.e. the process of inserting new nodes on-the-fly by modifying output channels"
    """

    def test_a_few_operations(self):
        wf = Workflow("output_manipulation")

        wf.a = Workflow.create.standard.Add(1, 2)
        wf.b = Workflow.create.standard.Add(3, 4)
        wf.c = Workflow.create.standard.UserInput(list(range(10)))
        wf.d = Workflow.create.standard.UserInput({"foo": 42})

        class Something:
            myattr = 1

        wf.e = Workflow.create.standard.UserInput(Something())

        wf.a.outputs.add < wf.b.outputs.add
        wf.c.outputs.user_input[:5]
        wf.d.outputs.user_input["foo"]
        wf.e.outputs.user_input.myattr
        out = wf()
        self.assertDictEqual(
            out,
            {
                'a__add_LessThan_b__add__lt': True,
                'c__user_input_GetItem_slice(None, 5, None)__getitem': [0, 1, 2, 3, 4],
                'd__user_input_GetItem_foo__getitem': 42,
                'e__user_input_GetAttr_myattr__getattr': 1
            }
        )

    def test_repeated_access(self):
        wf = Workflow("output_manipulation")
        wf.n = Workflow.create.standard.UserInput(list(range(10)))

        a = wf.n.outputs.user_input[:4]
        b = wf.n.outputs.user_input[:4]
        c = wf.n.outputs.user_input[1:]

        self.assertIs(
            a,
            b,
            msg="The same operation should re-access an existing node in the parent"
        )
        self.assertIsNot(
            a,
            c,
            msg="Unique operations should yield unique nodes"
        )

    def test_without_parent(self):
        n = Workflow.create.standard.UserInput(list(range(10)))
        d1 = n.outputs.user_input[5]
        d2 = n.outputs.user_input[5]

        self.assertIsInstance(d1, Node)
        self.assertIsNot(
            d1,
            d2,
            msg="Outside the scope of a parent, we can't expect to re-access an "
                "equivalent node"
        )
        self.assertEqual(
            d1.label,
            d2.label,
            msg="Equivalent operations should nonetheless generate equal labels"
        )


if __name__ == '__main__':
    unittest.main()
