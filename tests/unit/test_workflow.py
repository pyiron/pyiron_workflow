from concurrent.futures import Future
from sys import version_info
from time import sleep
import unittest

from bidict import ValueDuplicationError

from pyiron_workflow.channels import NotData
from pyiron_workflow.files import DirectoryObject
from pyiron_workflow.util import DotDict
from pyiron_workflow.workflow import Workflow


def plus_one(x=0):
    y = x + 1
    return y


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestWorkflow(unittest.TestCase):

    def test_node_addition(self):
        wf = Workflow("my_workflow")

        # Validate the four ways to add a node
        wf.add(Workflow.create.Function(plus_one, label="foo"))
        wf.create.Function(plus_one, label="bar")
        wf.baz = wf.create.Function(plus_one, label="whatever_baz_gets_used")
        Workflow.create.Function(plus_one, label="qux", parent=wf)
        self.assertListEqual(list(wf.nodes.keys()), ["foo", "bar", "baz", "qux"])
        wf.boa = wf.qux
        self.assertListEqual(
            list(wf.nodes.keys()),
            ["foo", "bar", "baz", "boa"],
            msg="Reassignment should remove the original instance"
        )

        wf.strict_naming = False
        # Validate name incrementation
        wf.add(Workflow.create.Function(plus_one, label="foo"))
        wf.create.Function(plus_one, label="bar")
        wf.baz = wf.create.Function(
            plus_one,
            label="without_strict_you_can_override_by_assignment"
        )
        Workflow.create.Function(plus_one, label="boa", parent=wf)
        self.assertListEqual(
            list(wf.nodes.keys()),
            [
                "foo", "bar", "baz", "boa",
                "foo0", "bar0", "baz0", "boa0",
            ]
        )

        with self.subTest("Make sure strict naming causes a bunch of attribute errors"):
            wf.strict_naming = True
            # Validate name preservation
            with self.assertRaises(AttributeError):
                wf.add(wf.create.Function(plus_one, label="foo"))

            with self.assertRaises(AttributeError):
                wf.create.Function(plus_one, label="bar")

            with self.assertRaises(AttributeError):
                wf.baz = wf.create.Function(plus_one, label="whatever_baz_gets_used")

            with self.assertRaises(AttributeError):
                Workflow.create.Function(plus_one, label="boa", parent=wf)

    def test_node_removal(self):
        wf = Workflow("my_workflow")
        wf.owned = Workflow.create.Function(plus_one)
        node = Workflow.create.Function(plus_one)
        wf.foo = node
        # Add it to starting nodes manually, otherwise it's only there at run time
        wf.starting_nodes = [wf.foo]
        # Connect it inside the workflow
        wf.foo.inputs.x = wf.owned.outputs.y

        wf.remove(node)
        self.assertIsNone(node.parent, msg="Removal should de-parent")
        self.assertFalse(node.connected, msg="Removal should disconnect")
        self.assertListEqual(
            wf.starting_nodes,
            [],
            msg="Removal should also remove from starting nodes"
        )

    def test_node_packages(self):
        wf = Workflow("my_workflow")
        wf.register("demo", "static.demo_nodes")

        # Test invocation
        wf.create.demo.OptionallyAdd(label="by_add")
        # Test invocation with attribute assignment
        wf.by_assignment = wf.create.demo.OptionallyAdd()

        self.assertSetEqual(
            set(wf.nodes.keys()),
            set(["by_add", "by_assignment"]),
            msg=f"Expected one node label generated automatically from the class and "
                f"the other from the attribute assignment, but got {wf.nodes.keys()}"
        )

    def test_double_workfloage_and_node_removal(self):
        wf1 = Workflow("one")
        wf1.create.Function(plus_one, label="node1")
        node2 = Workflow.create.Function(
            plus_one, label="node2", parent=wf1, x=wf1.node1.outputs.y
        )
        self.assertTrue(node2.connected)

        wf2 = Workflow("two")
        with self.assertRaises(ValueError):
            # Can't belong to two workflows at once
            wf2.add(node2)
        disconnections = wf1.remove(node2)
        self.assertFalse(node2.connected, msg="Removal should first disconnect")
        self.assertListEqual(
            disconnections,
            [(node2.inputs.x, wf1.node1.outputs.y)],
            msg="Disconnections should be returned by removal"
        )
        wf2.add(node2)
        self.assertEqual(node2.parent, wf2)

        node1 = wf1.node1
        disconnections = wf1.remove(node1.label)
        self.assertEqual(
            node1.parent,
            None,
            msg="Should be able to remove nodes by label as well as by object"
        )
        self.assertListEqual(
            [],
            disconnections,
            msg="node1 should have no connections left"
        )

    def test_workflow_io(self):
        wf = Workflow("wf")
        wf.create.Function(plus_one, label="n1")
        wf.create.Function(plus_one, label="n2")
        wf.create.Function(plus_one, label="n3")

        with self.subTest("Workflow IO should be drawn from its nodes"):
            self.assertEqual(len(wf.inputs), 3)
            self.assertEqual(len(wf.outputs), 3)

        wf.n3.inputs.x = wf.n2.outputs.y
        wf.n2.inputs.x = wf.n1.outputs.y

        with self.subTest("Only unconnected channels should count"):
            self.assertEqual(len(wf.inputs), 1)
            self.assertEqual(len(wf.outputs), 1)

        with self.subTest(
                "IO should be re-mappable, including exposing internally connected "
                "channels"
        ):
            wf.inputs_map = {"n1__x": "inp"}
            wf.outputs_map = {"n3__y": "out", "n2__y": "intermediate"}
            out = wf(inp=0)
            self.assertEqual(out.out, 3)
            self.assertEqual(out.intermediate, 2)

    def test_node_decorator_access(self):
        @Workflow.wrap_as.function_node("y")
        def plus_one(x: int = 0) -> int:
            return x + 1

        self.assertEqual(plus_one().run(), 1)

    def test_working_directory(self):
        wf = Workflow("wf")
        self.assertTrue(wf._working_directory is None)
        self.assertIsInstance(wf.working_directory, DirectoryObject)
        self.assertTrue(str(wf.working_directory.path).endswith(wf.label))
        wf.create.Function(plus_one)
        self.assertTrue(
            str(wf.plus_one.working_directory.path).endswith(wf.plus_one.label)
        )
        wf.working_directory.delete()

    def test_no_parents(self):
        wf = Workflow("wf")
        wf2 = Workflow("wf2")
        wf2.parent = None  # Is already the value and should ignore this
        with self.assertRaises(TypeError):
            # We currently specify workflows shouldn't get parents, this just verifies
            # the spec. If that spec changes, test instead that you _can_ set parents!
            wf2.parent = "not None"

        with self.assertRaises(TypeError):
            # Setting a non-None value to parent raises the type error from the setter
            wf2.parent = wf

    def test_with_executor(self):

        wf = Workflow("wf")
        wf.a = wf.create.SingleValue(plus_one)
        wf.b = wf.create.SingleValue(plus_one, x=wf.a)

        original_a = wf.a
        wf.executor = True

        self.assertIs(
            NotData,
            wf.outputs.b__y.value,
            msg="Sanity check that test is in right starting condition"
        )

        result = wf(a__x=0)
        self.assertIsInstance(
            result,
            Future,
            msg="Should be running as a parallel process"
        )

        returned_nodes = result.result()  # Wait for the process to finish
        self.assertIsNot(
            original_a,
            returned_nodes.a,
            msg="Executing in a parallel process should be returning new instances"
        )
        self.assertIs(
            wf,
            wf.nodes.a.parent,
            msg="Returned nodes should get the macro as their parent"
        )
        self.assertIsNone(
            original_a.parent,
            msg="Original nodes should be orphaned"
            # Note: At time of writing, this is accomplished in Node.__getstate__,
            #       which feels a bit dangerous...
        )
        self.assertEqual(
            0 + 1 + 1,
            wf.outputs.b__y.value,
            msg="And of course we expect the calculation to actually run"
        )

    def test_parallel_execution(self):
        wf = Workflow("wf")

        @Workflow.wrap_as.single_value_node()
        def five(sleep_time=0.):
            sleep(sleep_time)
            five = 5
            return five

        @Workflow.wrap_as.single_value_node("sum")
        def sum(a, b):
            return a + b

        wf.slow = five(sleep_time=1)
        wf.fast = five()
        wf.sum = sum(a=wf.fast, b=wf.slow)

        wf.slow.executor = wf.create.Executor()

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

        wf.sum.run()
        self.assertEqual(
            wf.sum.outputs.sum.value,
            5 + 5,
            msg="After the slow node completes, its output should be updated as a "
                "callback, and downstream nodes should proceed"
        )

    def test_call(self):
        wf = Workflow("wf")

        wf.a = wf.create.SingleValue(plus_one)
        wf.b = wf.create.SingleValue(plus_one)

        @Workflow.wrap_as.single_value_node("sum")
        def sum_(a, b):
            return a + b

        wf.sum = sum_(wf.a, wf.b)
        wf.run()
        self.assertEqual(
            wf.a.outputs.y.value + wf.b.outputs.y.value,
            wf.sum.outputs.sum.value,
            msg="Sanity check"
        )
        wf(a__x=42, b__x=42)
        self.assertEqual(
            plus_one(42) + plus_one(42),
            wf.sum.outputs.sum.value,
            msg="Workflow should accept input channel kwargs and update inputs "
                "accordingly"
            # Since the nodes run automatically, there is no need for wf.run() here
        )

        with self.assertRaises(TypeError):
            # IO is not ordered, so args make no sense for a workflow call
            # We _must_ use kwargs
            wf(42, 42)

    def test_return_value(self):
        wf = Workflow("wf")
        wf.a = wf.create.SingleValue(plus_one)
        wf.b = wf.create.SingleValue(plus_one, x=wf.a)

        with self.subTest("Run on main process"):
            return_on_call = wf(a__x=1)
            self.assertEqual(
                return_on_call,
                DotDict({"b__y": 1 + 2}),
                msg="Run output should be returned on call. Expecting a DotDict of "
                    "output values"
            )

            wf.inputs.a__x = 2
            return_on_explicit_run = wf.run()
            self.assertEqual(
                return_on_explicit_run["b__y"],
                2 + 2,
                msg="On explicit run, the most recent input data should be used and the "
                    "result should be returned"
            )

        # Note: We don't need to test running on an executor, because Workflows can't
        #       do that yet

    def test_execution_automation(self):
        @Workflow.wrap_as.single_value_node("out")
        def foo(x, y):
            return x + y

        def make_workflow():
            wf = Workflow("dag")
            wf.n1l = foo(0, 1)
            wf.n1r = foo(2, 0)
            wf.n2l = foo(-10, wf.n1l)
            wf.n2m = foo(wf.n1l, wf.n1r)
            wf.n2r = foo(wf.n1r, 10)
            return wf

        def matches_expectations(results):
            expected = {'n2l__out': -9, 'n2m__out': 3, 'n2r__out': 12}
            return all(expected[k] == v for k, v in results.items())

        auto = make_workflow()
        self.assertTrue(
            matches_expectations(auto()),
            msg="DAGs should run automatically"
        )

        user = make_workflow()
        user.automate_execution = False
        user.n1l > user.n1r > user.n2l
        user.n1r > user.n2m
        user.n1r > user.n2r
        user.starting_nodes = [user.n1l]
        self.assertTrue(
            matches_expectations(user()),
            msg="Users shoudl be allowed to ask to run things manually"
        )

        self.assertIn(
            user.n1r.signals.output.ran,
            user.n2r.signals.input.run.connections,
            msg="Expected execution signals as manually defined"
        )
        user.automate_execution = True
        self.assertTrue(
            matches_expectations(user()),
            msg="Users should be able to switch back to automatic execution"
        )
        self.assertNotIn(
            user.n1r.signals.output.ran,
            user.n2r.signals.input.run.connections,
            msg="Expected old execution signals to be overwritten"
        )
        self.assertIn(
            user.n2m.signals.output.ran,
            user.n2r.signals.input.run.connections,
            msg="At time of writing tests, automation makes a linear execution flow "
                "based on node topology and initialized by the order of appearance in "
                "the nodes list, so for a simple DAG like this the final node should "
                "be getting triggered by the penultimate node."
                "If this test failed, maybe you've written more sophisticated "
                "automation."
        )

        with self.subTest("Make sure automated cyclic graphs throw an error"):
            trivially_cyclic = make_workflow()
            trivially_cyclic.n1l.inputs.y = trivially_cyclic.n1l
            with self.assertRaises(ValueError):
                trivially_cyclic()

            cyclic = make_workflow()
            cyclic.n1l.inputs.y = cyclic.n2l
            with self.assertRaises(ValueError):
                cyclic()

    def test_io_label_maps_are_bijective(self):

        with self.subTest("Null case"):
            Workflow(
                "my_workflow",
                Workflow.create.Function(plus_one, label="foo1"),
                Workflow.create.Function(plus_one, label="foo2"),
                inputs_map={
                    "foo1__x": "x1",
                    "foo2__x": "x2"
                },
                outputs_map=None
            )

        with self.subTest("At instantiation"):
            with self.assertRaises(ValueDuplicationError):
                Workflow(
                    "my_workflow",
                    Workflow.create.Function(plus_one, label="foo1"),
                    Workflow.create.Function(plus_one, label="foo2"),
                    inputs_map={
                        "foo1__x": "x",
                        "foo2__x": "x"
                    }
                )

        with self.subTest("Post-facto assignment"):
            wf = Workflow(
                "my_workflow",
                Workflow.create.Function(plus_one, label="foo1"),
                Workflow.create.Function(plus_one, label="foo2"),
            )
            wf.outputs_map = None
            with self.assertRaises(ValueDuplicationError):
                wf.inputs_map = {"foo1__x": "x", "foo2__x": "x"}

        with self.subTest("Post-facto update"):
            wf = Workflow(
                "my_workflow",
                Workflow.create.Function(plus_one, label="foo1"),
                Workflow.create.Function(plus_one, label="foo2"),
            )
            wf.inputs_map = {"foo1__x": "x1", "foo2__x": "x2"}
            with self.assertRaises(ValueDuplicationError):
                wf.inputs_map["foo2__x"] = "x1"

    def test_pull_and_executors(self):
        def add_three_macro(macro):
            macro.one = Workflow.create.SingleValue(plus_one)
            macro.two = Workflow.create.SingleValue(plus_one, x=macro.one)
            macro.three = Workflow.create.SingleValue(plus_one, x=macro.two)

        wf = Workflow("pulling")

        wf.n1 = Workflow.create.SingleValue(plus_one, x=0)
        wf.m = Workflow.create.Macro(add_three_macro, one__x=wf.n1)

        self.assertEquals(
            (0 + 1) + (1 + 1),
            wf.m.two.pull(run_parent_trees_too=True),
            msg="Sanity check, pulling here should work perfectly fine"
        )

        wf.m.one.executor = True
        with self.assertRaises(
            ValueError,
            msg="Should not be able to pull with executor in local scope"
        ):
            wf.m.two.pull()
        wf.m.one.executor = False

        wf.n1.executor = True
        with self.assertRaises(
            ValueError,
            msg="Should not be able to pull with executor in parent scope"
        ):
            wf.m.two.pull(run_parent_trees_too=True)

        # Pulling in the local scope should be fine with an executor only in the parent
        # scope
        wf.m.two.pull(run_parent_trees_too=False)


if __name__ == '__main__':
    unittest.main()
