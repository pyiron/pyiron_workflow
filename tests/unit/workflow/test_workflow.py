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
        Node(fnc, "x", label="boa", parent=wf)
        self.assertListEqual(list(wf.nodes.keys()), ["foo", "bar", "baz", "boa"])

        wf.deactivate_strict_naming()
        # Validate name incrementation
        wf.add(Node(fnc, "x", label="foo"))
        wf.add.Node(fnc, "y", label="bar")
        wf.baz = Node(
            fnc,
            "y",
            label="without_strict_you_can_override_by_assignment"
        )
        Node(fnc, "x", label="boa", parent=wf)
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
            Node(fnc, "x", label="boa", parent=wf)

    def test_node_packages(self):
        wf = Workflow("my_workflow")

        # Test invocation
        wf.add.atomistics.BulkStructure(repeat=3, cubic=True, element="Al")
        # Test invocation with attribute assignment
        wf.engine = wf.add.atomistics.Lammps(structure=wf.bulk_structure)

        self.assertSetEqual(
            set(wf.nodes.keys()),
            set(["bulk_structure", "engine"]),
            msg=f"Expected one node label generated automatically from the class and "
                f"the other from the attribute assignment, but got {wf.nodes.keys()}"
        )

    def test_double_workfloage_and_node_removal(self):
        wf1 = Workflow("one")
        wf1.add.Node(fnc, "y", label="node1")
        node2 = Node(fnc, "y", label="node2", parent=wf1, x=wf1.node1.outputs.y)
        self.assertTrue(node2.connected)

        wf2 = Workflow("two")
        with self.assertRaises(ValueError):
            # Can't belong to two workflows at once
            wf2.add(node2)
        wf1.remove(node2)
        wf2.add(node2)
        self.assertEqual(node2.parent, wf2)
        self.assertFalse(node2.connected)

    def test_workflow_io(self):
        wf = Workflow("wf")
        wf.add.Node(fnc, "y", label="n1")
        wf.add.Node(fnc, "y", label="n2")
        wf.add.Node(fnc, "y", label="n3")

        with self.subTest("Workflow IO should be drawn from its nodes"):
            self.assertEqual(len(wf.inputs), 3)
            self.assertEqual(len(wf.outputs), 3)

        wf.n3.inputs.x = wf.n2.outputs.y
        wf.n2.inputs.x = wf.n1.outputs.y

        with self.subTest("Only unconnected channels should count"):
            self.assertEqual(len(wf.inputs), 1)
            self.assertEqual(len(wf.outputs), 1)

    def test_node_decorator_access(self):
        @Workflow.wrap_as.fast_node("y")
        def plus_one(x: int = 0) -> int:
            return x + 1

        self.assertEqual(plus_one().outputs.y.value, 1)
