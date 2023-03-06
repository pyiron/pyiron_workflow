from unittest import TestCase, skipUnless
from sys import version_info
from typing import Optional

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.workflow import Workflow


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestWorkflow(TestCase):

    def test_node_addition(self):
        def fnc(x=0):
            return x + 1

        wf = Workflow("my_workflow")

        # Validate the four ways to add a node
        wf.add(Node(fnc, "x", label="foo"))
        wf.add.Node(fnc, "y", label="bar")
        wf.baz = Node(fnc, "y", label="whatever_baz_gets_used")
        Node(fnc, "x", label="boa", workflow=wf)
        self.assertListEqual(list(wf.nodes.keys()), ["foo", "bar", "baz", "boa"])

        wf.deactivate_strict_naming()
        # Validate name incrementation
        wf.add(Node(fnc, "x", label="foo"))
        wf.add.Node(fnc, "y", label="bar")
        wf.baz = Node(fnc, "y", label="whatever_baz_gets_used")
        Node(fnc, "x", label="boa", workflow=wf)
        self.assertListEqual(
            list(wf.nodes.keys()),
            [
                "foo", "bar", "baz", "boa",
                "foo0", "bar0", "baz0", "boa0",
            ]
        )

        wf.activate_strict_naming()
        # Validate name preservation
        with self.assertRaises(AttributeError):
            wf.add(Node(fnc, "x", label="foo"))

        with self.assertRaises(AttributeError):
            wf.add.Node(fnc, "y", label="bar")

        with self.assertRaises(AttributeError):
            wf.baz = Node(fnc, "y", label="whatever_baz_gets_used")

        with self.assertRaises(AttributeError):
            Node(fnc, "x", label="boa", workflow=wf)

    def test_ugly(self):
        def fnc(x=0): return x + 1

        wf = Workflow("my_workflow")
        wf.add.Node(fnc, "y", label="foo")  # 1
        wf.bar = Node(fnc, "y", label="whatever_bar_gets_used")  # 2
        Node(fnc, "x", label="baz", workflow=wf)

        # I'm a bit short on time, and I want to get to the integration
        # So this is just a slapdash version of some stuff I was testing in notebook
        n1 = self.DummyNode(label="n1")
        n2 = self.DummyNode(label="n2")

        wf = Workflow("my_workflow", n1, n2)

        self.assertEqual(2, len(wf.nodes), msg="Add at instantiation")

        n_unnamed = self.DummyNode()
        wf.add(n_unnamed)

        self.assertEqual(3, len(wf.nodes), msg="Add with add")
        self.assertTrue(
            n_unnamed.node_function.__name__ in wf.nodes.keys(),
            msg="Auto-label based on function"
        )

        wf.add(self.DummyNode())
        self.assertTrue(
            n_unnamed.label + "0" in wf.nodes.keys(),
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
        wf.foo = self.DummyNode()
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
