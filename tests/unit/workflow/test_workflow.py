import unittest
from sys import version_info
from time import sleep

from pyiron_contrib.workflow.channels import NotData
from pyiron_contrib.workflow.files import DirectoryObject
from pyiron_contrib.workflow.function import Function
from pyiron_contrib.workflow.workflow import Workflow


def fnc(x=0):
    return x + 1


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestWorkflow(unittest.TestCase):

    def test_node_addition(self):
        wf = Workflow("my_workflow")

        # Validate the four ways to add a node
        wf.add(Function(fnc, "x", label="foo"))
        wf.add.Function(fnc, "y", label="bar")
        wf.baz = Function(fnc, "y", label="whatever_baz_gets_used")
        Function(fnc, "x", label="qux", parent=wf)
        self.assertListEqual(list(wf.nodes.keys()), ["foo", "bar", "baz", "qux"])
        wf.boa = wf.qux
        self.assertListEqual(
            list(wf.nodes.keys()),
            ["foo", "bar", "baz", "boa"],
            msg="Reassignment should remove the original instance"
        )

        wf.strict_naming = False
        # Validate name incrementation
        wf.add(Function(fnc, "x", label="foo"))
        wf.add.Function(fnc, "y", label="bar")
        wf.baz = Function(
            fnc,
            "y",
            label="without_strict_you_can_override_by_assignment"
        )
        Function(fnc, "x", label="boa", parent=wf)
        self.assertListEqual(
            list(wf.nodes.keys()),
            [
                "foo", "bar", "baz", "boa",
                "foo0", "bar0", "baz0", "boa0",
            ]
        )

        wf.strict_naming = True
        # Validate name preservation
        with self.assertRaises(AttributeError):
            wf.add(Function(fnc, "x", label="foo"))

        with self.assertRaises(AttributeError):
            wf.add.Function(fnc, "y", label="bar")

        with self.assertRaises(AttributeError):
            wf.baz = Function(fnc, "y", label="whatever_baz_gets_used")

        with self.assertRaises(AttributeError):
            Function(fnc, "x", label="boa", parent=wf)

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
        wf1.add.Function(fnc, "y", label="node1")
        node2 = Function(fnc, "y", label="node2", parent=wf1, x=wf1.node1.outputs.y)
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
        wf.add.Function(fnc, "y", label="n1")
        wf.add.Function(fnc, "y", label="n2")
        wf.add.Function(fnc, "y", label="n3")

        with self.subTest("Workflow IO should be drawn from its nodes"):
            self.assertEqual(len(wf.inputs), 3)
            self.assertEqual(len(wf.outputs), 3)

        wf.n3.inputs.x = wf.n2.outputs.y
        wf.n2.inputs.x = wf.n1.outputs.y

        with self.subTest("Only unconnected channels should count"):
            self.assertEqual(len(wf.inputs), 1)
            self.assertEqual(len(wf.outputs), 1)

    def test_node_decorator_access(self):
        @Workflow.wrap_as.function_node("y")
        def plus_one(x: int = 0) -> int:
            return x + 1

        self.assertEqual(plus_one().outputs.y.value, 1)

    def test_working_directory(self):
        wf = Workflow("wf")
        self.assertTrue(wf._working_directory is None)
        self.assertIsInstance(wf.working_directory, DirectoryObject)
        self.assertTrue(str(wf.working_directory.path).endswith(wf.label))
        wf.add.Function(fnc, "output")
        self.assertTrue(str(wf.fnc.working_directory.path).endswith(wf.fnc.label))
        wf.working_directory.delete()

    def test_no_parents(self):
        wf = Workflow("wf")
        wf2 = Workflow("wf2")
        wf2.parent = None  # Is already the value and should ignore this
        with self.assertRaises(TypeError):
            # We currently specify workflows shouldn't get parents, this just verifies
            # the spec. If that spec changes, test instead that you _can_ set parents!
            wf2.parent = "not None"

        with self.assertRaises(AttributeError):
            # Setting a non-None value to parent raises the type error above
            # If that value is further a nodal object, the __setattr__ definition
            # takes over, and we try to add it to the nodes, but there we will run into
            # the fact you can't add a node to a taken attribute label
            # In both cases, we satisfy the spec that workflow's can't have parents
            wf2.parent = wf

    def test_parallel_execution(self):
        wf = Workflow("wf")

        @Workflow.wrap_as.single_value_node("five", run_on_updates=False)
        def five(sleep_time=0.):
            sleep(sleep_time)
            return 5

        @Workflow.wrap_as.single_value_node("sum")
        def sum(a, b):
            return a + b

        wf.slow = five(sleep_time=1)
        wf.fast = five()
        wf.sum = sum(a=wf.fast, b=wf.slow)

        wf.slow.executor = wf.create.CloudpickleProcessPoolExecutor()

        wf.slow.run()
        wf.fast.run()
        self.assertTrue(
            wf.slow.running,
            msg="The slow node should still be running"
        )
        self.assertEqual(
            wf.fast.outputs.five.value,
            5,
            msg="The slow node should not prohibit the completion of the fast node"
        )
        self.assertEqual(
            wf.sum.outputs.sum.value,
            NotData,
            msg="The slow node _should_ hold up the downstream node to which it inputs"
        )

        while wf.slow.future.running():
            sleep(0.1)

        self.assertEqual(
            wf.sum.outputs.sum.value,
            5 + 5,
            msg="After the slow node completes, its output should be updated as a "
                "callback, and downstream nodes should proceed"
        )


if __name__ == '__main__':
    unittest.main()
