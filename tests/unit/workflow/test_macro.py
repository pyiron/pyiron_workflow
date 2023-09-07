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
    macro.one > macro.two > macro.three


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
            m.outputs.three__result.value,
            NotData,
            msg="Output should be accessible with the usual naming convention, but we "
                "asked the node not to run yet so there shouldn't be any data"
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

    def test_by_subclass(self):
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
            macro.b = Macro(add_three_macro, one__x=macro.a)
            macro.c = SingleValue(add_one, x=macro.b.outputs.three__result)
            macro.a > macro.b > macro.c

        m = Macro(nested_macro)
        self.assertEqual(m(a__x=0).c__result, 5)

    def test_upstream_detection(self):
        def my_macro(macro):
            macro.a = SingleValue(add_one, x=0)
            macro.b = SingleValue(add_one, x=macro.a)

        m = Macro(my_macro)
        self.assertTrue(
            m.connects_to_input_of(m.b),
            msg="b should have input from a"
        )
        self.assertFalse(
            m.connects_to_output_of(m.b),
            msg="b should not show any local output connections"
        )
        self.assertFalse(
            m.connects_to_input_of(m.a),
            msg="a should not show any local input connections"
        )
        self.assertTrue(
            m.connects_to_output_of(m.a),
            msg="b should have input from a"
        )
        self.assertEqual(
            len(m.upstream_nodes),
            1,
            msg="Only the a-node should have connected output but no connected input"
        )
        self.assertIs(m.upstream_nodes[0], m.a)

        m2 = Macro(my_macro)
        m.inputs.a__x = m2.outputs.b__result
        self.assertIs(
            m.upstream_nodes[0],
            m.a,
            msg="External connections should not impact upstream-ness"
        )
        self.assertTrue(
            m.connects_to_output_of(m2.b),
            msg="Should be able to check if external nodes have local connections"
        )

        m.inputs.a__x = m.outputs.b__result  # Infinite loop self-connection
        self.assertEqual(
            len(m.upstream_nodes),
            0,
            msg="Internal connections _should_ impact upstream-ness"
        )

        m.b.disconnect()
        self.assertEqual(
            m.upstream_nodes[0],
            m.a,
            msg="After disconnecting the b-node, the a-node no longer has internal "
                "input and should register as upstream again, regardless of whether its "
                "output is connected to anything (which it isn't, since we fully "
                "disconnected m.b)"
        )

        def deep_macro(macro):
            macro.a = SingleValue(add_one, x=0)
            macro.m = Macro(my_macro)
            macro.m.inputs.a__x = macro.a

        nested = Macro(deep_macro)
        plain = Macro(my_macro)
        plain.inputs.a__x = nested.m.outputs.b__result
        print(nested.m.outputs.labels)
        self.assertTrue(
            nested.connects_to_input_of(plain),
            msg="A child of the nested macro has a connection to the plain macros"
                "input, so the entire nested macro should count as having a "
                "connection to the plain macro's input."
        )

    def test_custom_start(self):
        def modified_start_macro(macro):
            macro.a = SingleValue(add_one, x=0)
            macro.b = SingleValue(add_one, x=0)
            macro.starting_nodes = [macro.b]

        m = Macro(modified_start_macro)
        self.assertIs(
            m.outputs.a__result.value,
            NotData,
            msg="Node should not have run when the macro batch updated input"
        )
        self.assertIs(
            m.outputs.b__result.value,
            NotData,
            msg="Node should not have run when the macro batch updated input"
        )
        m.run()
        self.assertIs(
            m.outputs.a__result.value,
            NotData,
            msg="Was not included in starting nodes, should not have run"
        )
        self.assertEqual(
            m.outputs.b__result.value,
            1,
            msg="Was included in starting nodes, should have run"
        )


if __name__ == '__main__':
    unittest.main()
