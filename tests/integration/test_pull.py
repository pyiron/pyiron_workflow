import unittest

from pyiron_workflow.workflow import Workflow


class TestPullingOutput(unittest.TestCase):
    def test_without_workflow(self):
        from pyiron_workflow import Workflow

        @Workflow.wrap_as.single_value_node("sum")
        def x_plus_y(x: int = 0, y: int = 0) -> int:
            return x + y

        node = x_plus_y(
            x=x_plus_y(0, 1),
            y=x_plus_y(2, 3)
        )
        self.assertEqual(6, node.pull())

        for n in [
            node,
            node.inputs.x.connections[0].node,
            node.inputs.y.connections[0].node,
        ]:
            self.assertFalse(
                n.signals.connected,
                msg="Connections should be unwound after the pull is done"
            )
            self.assertEqual(
                "x_plus_y",
                n.label,
                msg="Original labels should be restored after the pull is done"
            )

    def test_pulling_from_inside_a_macro(self):
        @Workflow.wrap_as.single_value_node("sum")
        def x_plus_y(x: int = 0, y: int = 0) -> int:
            # print("EXECUTING")
            return x + y

        @Workflow.wrap_as.macro_node()
        def b2_leaves_a1_alone(macro):
            macro.a1 = x_plus_y(0, 0)
            macro.a2 = x_plus_y(0, 1)
            macro.b1 = x_plus_y(macro.a1, macro.a2)
            macro.b2 = x_plus_y(macro.a2, 10)

        wf = Workflow("demo")
        wf.upstream = x_plus_y()
        wf.macro = b2_leaves_a1_alone(a2__x=wf.upstream)

        # Pulling b1 -- executes a1, a2, b2
        self.assertEqual(1, wf.macro.b1.pull())
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> 1

        # Pulling b2 -- executes a2, a1
        self.assertEqual(11, wf.macro.b2.pull())
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> 11

        # Updated inputs get reflected in the pull
        wf.macro.set_input_values(a1__x=100, a2__x=-100)
        self.assertEqual(-89, wf.macro.b2.pull())
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> -89

        # Connections are restored after a pull
        # Crazy negative value of a2 gets written over by pulling in the upstream
        # connection value
        # Running wf -- executes upstream, macro (is silent), a1, a2, b1, b2
        out = wf()
        self.assertEqual(101, out.macro__b1__sum)
        self.assertEqual(11, out.macro__b2__sum)
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> EXECUTING
        # >>> {'macro__b1__sum': 101, 'macro__b2__sum': 11}