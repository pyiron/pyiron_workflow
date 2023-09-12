from functools import partialmethod
import unittest
from sys import version_info

from pyiron_contrib.workflow.channels import NotData
from pyiron_contrib.workflow.function import SingleValue
from pyiron_contrib.workflow.macro import Macro


def add_one(x):
    result = x + 1
    return result


def add_three_macro(macro):
    macro.one = SingleValue(add_one)
    SingleValue(add_one, macro.one, label="two", parent=macro)
    macro.add(SingleValue(add_one, macro.two, label="three"))
    # Cover a handful of addition methods,
    # although these are more thoroughly tested in Workflow tests


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestMacro(unittest.TestCase):

    def test_labels(self):
        m = Macro(add_three_macro)
        self.assertEqual(
            m.label,
            add_three_macro.__name__,
            msg="Label should be automatically generated"
        )
        label = "custom_name"
        m2 = Macro(add_three_macro, label=label)
        self.assertEqual(m2.label, label, msg="Should be able to specify a label")

    def test_wrapper_function(self):
        m = Macro(add_three_macro)

        self.assertIs(
            m.outputs.three__result.value,
            NotData,
            msg="Output should be accessible with the usual naming convention, but we "
                "have not run yet so there shouldn't be any data"
        )

        input_x = 1
        expected_value = add_one(add_one(add_one(input_x)))
        out = m(one__x=input_x)  # Take kwargs to set input at runtime

        self.assertEqual(
            out.three__result,
            expected_value,
            msg="Macros should return the output, just like other nodes"
        )
        self.assertEqual(
            m.outputs.three__result.value,
            expected_value,
            msg="Macros should get output updated, just like other nodes"
        )

    def test_subclass(self):
        class MyMacro(Macro):
            def build_graph(self):
                add_three_macro(self)

            __init__ = partialmethod(
                Macro.__init__,
                build_graph,
            )

        x = 0
        m = MyMacro(one__x=x)
        m.run()
        self.assertEqual(
            m.outputs.three__result.value,
            add_one(add_one(add_one(x))),
            msg="Subclasses should be able to simply override the graph_creator arg"
        )

    def test_key_map(self):
        m = Macro(
            add_three_macro,
            inputs_map={"one__x": "my_input"},
            outputs_map={"three__result": "my_output", "two__result": "intermediate"},
        )
        self.assertSetEqual(
            set(m.inputs.labels),
            set(("my_input",)),
            msg="Input should be relabelled, but not added to or taken away from"
        )
        self.assertSetEqual(
            set(m.outputs.labels),
            set(("my_output", "intermediate")),
            msg="Output should be relabelled and expanded"
        )

        with self.subTest("Make new names can be used as usual"):
            x = 0
            out = m(my_input=x)
            self.assertEqual(
                out.my_output,
                add_one(add_one(add_one(x))),
                msg="Expected output but relabeled should be accessible"
            )
            self.assertEqual(
                out.intermediate,
                add_one(add_one(x)),
                msg="New, internally connected output that was specifically requested "
                    "should be accessible"
            )

    def test_nesting(self):
        def nested_macro(macro):
            macro.a = SingleValue(add_one)
            macro.b = Macro(
                add_three_macro,
                one__x=macro.a,
                outputs_map={"two__result": "intermediate_result"}
            )
            macro.c = SingleValue(add_one, x=macro.b.outputs.three__result)
            macro.a > macro.b > macro.c
            macro.starting_nodes = [macro.a]
            macro.outputs_map = {"b__intermediate_result": "deep_output"}

        m = Macro(nested_macro)
        self.assertEqual(m(a__x=0).c__result, 5)

        with self.subTest("Test Channel.get_node_belonging_to"):
            deep_channel = m.outputs.deep_output
            self.assertIs(
                m.b.two,
                deep_channel.node,
                msg="Channel node should be the node that holds it directly."
            )
            self.assertIs(
                m.b,
                deep_channel.get_node_belonging_to(m)
            )

            with self.assertRaises(ValueError):
                m2 = Macro(nested_macro)  # Not in deep_channel's parentage!
                deep_channel.get_node_belonging_to(m2)

    def test_execution_automation(self):
        fully_automatic = add_three_macro

        def fully_defined(macro):
            add_three_macro(macro)
            macro.one > macro.two > macro.three
            macro.starting_nodes = [macro.one]

        def only_order(macro):
            add_three_macro(macro)
            macro.two > macro.three

        def only_starting(macro):
            add_three_macro(macro)
            macro.starting_nodes = [macro.one]

        m_auto = Macro(fully_automatic)
        m_user = Macro(fully_defined)

        x = 0
        expected = add_one(add_one(add_one(x)))
        self.assertEqual(
            m_auto(one__x=x).three__result,
            expected,
            "DAG macros should run fine without user specification of execution."
        )
        self.assertEqual(
            m_user(one__x=x).three__result,
            expected,
            "Macros should run fine if the user nicely specifies the exeuction graph."
        )

        with self.subTest("Partially specified execution should fail"):
            # We don't yet check for _crappy_ user-defined execution,
            # But we should make sure it's at least valid in principle
            with self.assertRaises(ValueError):
                Macro(only_order)

            with self.assertRaises(ValueError):
                Macro(only_starting)


if __name__ == '__main__':
    unittest.main()
