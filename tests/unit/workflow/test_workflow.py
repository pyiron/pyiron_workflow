from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.workflow import Workflow


def fnc(x=0):
    return x + 1


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestWorkflow(TestCase):

    def test_node_addition(self):
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

    def test_double_workfloage_and_node_removal(self):
        wf1 = Workflow("one")
        wf1.add.Node(fnc, "y", label="node1")
        node2 = Node(fnc, "y", label="node2", workflow=wf1, x=wf1.node1.outputs.y)
        self.assertTrue(node2.connected)

        wf2 = Workflow("two")
        with self.assertRaises(ValueError):
            # Can't belong to two workflows at once
            wf2.add(node2)
        wf1.remove(node2)
        wf2.add(node2)
        self.assertEqual(node2.workflow, wf2)
        self.assertFalse(node2.connected)

    def test_workflow_io(self):
        wf = Workflow("wf")
        wf.add.Node(fnc, "y", label="n1")
        wf.add.Node(fnc, "y", label="n2")
        wf.add.Node(fnc, "y", label="n3")

        self.assertEqual(len(wf.input), 3)
        self.assertEqual(len(wf.output), 3)

        wf.n3.inputs.x = wf.n2.outputs.y
        wf.n2.inputs.x = wf.n1.outputs.y

        self.assertEqual(len(wf.input), 1)
        self.assertEqual(len(wf.output), 1)
