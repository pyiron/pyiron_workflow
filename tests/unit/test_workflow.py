from concurrent.futures import Future
from sys import version_info
from time import sleep
import unittest

from bidict import ValueDuplicationError

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NotData
from pyiron_workflow.util import DotDict
from pyiron_workflow.workflow import Workflow


def plus_one(x=0):
    y = x + 1
    return y


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_tests_in_python_path()
        super().setUpClass()

    def test_io(self):
        wf = Workflow("wf")
        wf.create.Function(plus_one, label="n1")
        wf.create.Function(plus_one, label="n2")
        wf.create.Function(plus_one, label="n3")

        inp = wf.inputs
        inp_again = wf.inputs
        self.assertIsNot(
            inp, inp_again, msg="Workflow input should always get rebuilt"
        )

        n_in = len(wf.inputs)
        n_out = len(wf.outputs)
        wf.create.Function(plus_one, label="n4")
        self.assertEqual(
            n_in + 1, len(wf.inputs), msg="Workflow IO should be drawn from its nodes"
        )
        self.assertEqual(
            n_out + 1, len(wf.outputs), msg="Workflow IO should be drawn from its nodes"
        )

        n_in = len(wf.inputs)
        n_out = len(wf.outputs)
        wf.n3.inputs.x = wf.n2.outputs.y
        wf.n2.inputs.x = wf.n1.outputs.y
        self.assertEqual(
            n_in -2, len(wf.inputs), msg="New connections should get reflected"
        )
        self.assertEqual(
            n_out - 2, len(wf.outputs), msg="New connections should get reflected"
        )

        wf.inputs_map = {"n1__x": "inp"}
        self.assertIs(wf.n1.inputs.x, wf.inputs.inp, msg="IO should be renamable")

        self.assertNotIn(wf.n2.outputs.y, wf.outputs, msg="Ensure starting condition")
        self.assertIn(wf.n3.outputs.y, wf.outputs, msg="Ensure starting condition")
        wf.outputs_map = {"n3__y": None, "n2__y": "intermediate"}
        self.assertIn(wf.n2.outputs.y, wf.outputs, msg="IO should be exposable")
        self.assertIs(
            wf.n2.outputs.y, wf.outputs.intermediate, msg="IO should be by reference"
        )
        self.assertNotIn(wf.n3.outputs.y, wf.outputs, msg="IO should be hidable")

    def test_is_parentmost(self):
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
                msg="On explicit run, the most recent input data should be used and "
                    "the result should be returned"
            )

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
