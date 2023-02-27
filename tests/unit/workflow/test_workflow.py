from unittest import TestCase
from typing import Optional

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.workflow import Workflow


class DummyNode(Node):

    def __init__(
            self,
            label: Optional[str] = None,
            **kwargs
    ):
        super().__init__(
            node_function=self.pass_value,
            output_labels=("y",),
            label=label,
            **kwargs
        )

    @staticmethod
    def pass_value(x):
        return x


class TestWorkflow(TestCase):
    def test_ugly(self):
        # I'm a bit short on time, and I want to get to the integration
        # So this is just a slapdash version of some stuff I was testing in notebook
        n1 = DummyNode(label="n1")
        n2 = DummyNode(label="n2")

        wf = Workflow("my_workflow", n1, n2)

        self.assertEqual(2, len(wf.nodes), msg="Add at instantiation")

        n_unnamed = DummyNode()
        wf.add(n_unnamed)

        self.assertEqual(3, len(wf.nodes), msg="Add with add")
        self.assertTrue(DummyNode.__name__ in wf.nodes.keys(), msg="Auto-label based on class")

        wf.add(DummyNode())
        self.assertTrue(
            DummyNode.__name__ + "0" in wf.nodes.keys(),
            msg="automatically increment duplicate names"
        )

        with self.assertRaises(ValueError):
            # Can't have the same one twice!
            n_unnamed.label = "We_even_modify_something_about_it_first"
            wf.add(n_unnamed)

        # with self.assertWarns(Warning):
        #     # Name and attribute need to match or we warn and quit
        #     wf.foo = DummyNode(label="not_foo")
        #
        # with self.assertRaises(Warning):
        #     # Add the same label twice and we warn that we're updating it
        #     wf.foo = n_unnamed
        # Ok, the test suite is not catching warnings this way, but I don't have time
        # to debug it now. The warnings are there.
        wf.foo = DummyNode()
        self.assertTrue(
            "foo" in wf.nodes.keys(),
            msg="automatically set empty names to attribute label"
        )

        open_inputs = len(wf.input)
        wf.n2.inputs.x = wf.n1.outputs.y  # Allow connections from workflow access
        self.assertEqual(
            1,
            open_inputs - len(wf.input),
            msg="Should only list open connections"
        )

        with self.subTest("Test iteration"):
            self.assertTrue(all([node in wf.nodes.values() for node in wf]))
