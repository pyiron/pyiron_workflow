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

    def test_by_function(self):
        m = Macro(add_three_macro)

        self.assertIs(
            m.outputs.three_result.value,
            NotData,
            msg="Output should be accessible with the usual naming convention, but we "
                "asked the node not to run yet so there shouldn't be any data"
        )

        input_x = 1
        expected_value = add_one(add_one(add_one(input_x)))
        out = m(one_x=input_x)  # Take kwargs to set input at runtime

        self.assertEqual(
            out.three_result,
            expected_value,
            msg="Macros should return the output, just like other nodes"
        )
        self.assertEqual(
            m.outputs.three_result.value,
            expected_value,
            msg="Macros should get output updated, just like other nodes"
        )

    def test_by_subclass(self):
        class MyMacro(Macro):
            def build_graph(self):
                add_three_macro(self)

            __init__ = partialmethod(
                Macro.__init__,
                build_graph,
            )

        x = 0
        m = MyMacro(one_x=x)
        self.assertEqual(
            m.outputs.three_result.value,
            add_one(add_one(add_one(x))),
            msg="Subclasses should be able to simply override the graph_creator arg"
        )

    def test_key_map(self):
        m = Macro(
            add_three_macro,
            inputs_map={"one_x": "my_input"},
            outputs_map={"three_result": "my_output", "two_result": "intermediate"},
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
            macro.b = Macro(add_three_macro, one_x=macro.a)
            macro.c = SingleValue(add_one, x=macro.b.outputs.three_result)

        m = Macro(nested_macro)
        self.assertEqual(m(a_x=0).c_result, 5)

    def test_custom_start(self):
        def modified_start_macro(macro):
            macro.a = SingleValue(add_one, x=0, run_on_updates=False)
            macro.b = SingleValue(add_one, x=0, run_on_updates=False)
            macro.starting_nodes = [macro.b]

        m = Macro(modified_start_macro, update_on_instantiation=False)
        self.assertIs(
            m.outputs.a_result.value,
            NotData,
            msg="Node should not have run when the macro batch updated input"
        )
        self.assertIs(
            m.outputs.b_result.value,
            NotData,
            msg="Node should not have run when the macro batch updated input"
        )
        m.run()
        self.assertIs(
            m.outputs.a_result.value,
            NotData,
            msg="Was not included in starting nodes, should not have run"
        )
        self.assertEqual(
            m.outputs.b_result.value,
            1,
            msg="Was included in starting nodes, should have run"
        )
