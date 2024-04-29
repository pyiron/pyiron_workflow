from concurrent.futures import Future
import pickle
import sys
from time import sleep
import unittest

from bidict import ValueDuplicationError

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.semantics import ParentMostError
from pyiron_workflow.snippets.dotdict import DotDict
from pyiron_workflow.storage import TypeNotFoundError
from pyiron_workflow.workflow import Workflow


def plus_one(x=0):
    y = x + 1
    return y


@Workflow.wrap.as_function_node("y")
def PlusOne(x: int = 0):
    return x + 1


class TestWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_tests_in_python_path()
        super().setUpClass()

    def test_io(self):
        wf = Workflow("wf")
        wf.n1 = wf.create.function_node(plus_one)
        wf.n2 = wf.create.function_node(plus_one)
        wf.n3 = wf.create.function_node(plus_one)

        inp = wf.inputs
        inp_again = wf.inputs
        self.assertIsNot(
            inp, inp_again, msg="Workflow input should always get rebuilt"
        )

        n_in = len(wf.inputs)
        n_out = len(wf.outputs)
        wf.n4 = wf.create.function_node(plus_one)
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
        
    def test_io_maps(self):
        # input and output, renaming, accessing connected, and deactivating disconnected
        wf = Workflow("wf")
        wf.n1 = Workflow.create.function_node(plus_one, x=0)
        wf.n2 = Workflow.create.function_node(plus_one, x=wf.n1)
        wf.n3 = Workflow.create.function_node(plus_one, x=wf.n2)
        wf.m = Workflow.create.function_node(plus_one, x=42)
        wf.inputs_map = {
            "n1__x": "x",  # Rename
            "n2__x": "intermediate_x",  # Expose
            "m__x": None,  # Hide
        }
        wf.outputs_map = {
            "n3__y": "y",  # Rename
            "n2__y": "intermediate_y",  # Expose,
            "m__y": None,  # Hide
        }
        self.assertIn("x", wf.inputs.labels, msg="Should be renamed")
        self.assertIn("y", wf.outputs.labels, msg="Should be renamed")
        self.assertIn("intermediate_x", wf.inputs.labels, msg="Should be exposed")
        self.assertIn("intermediate_y", wf.outputs.labels, msg="Should be exposed")
        self.assertNotIn("m__x", wf.inputs.labels, msg="Should be hidden")
        self.assertNotIn("m__y", wf.outputs.labels, msg="Should be hidden")
        self.assertNotIn("m__y", wf.outputs.labels, msg="Should be hidden")

        wf.set_run_signals_to_dag_execution()
        out = wf.run()
        self.assertEqual(
            3,
            out.y,
            msg="New names should be propagated to the returned value"
        )
        self.assertNotIn(
            "m__y",
            list(out.keys()),
            msg="IO filtering should be evident in returned value"
        )
        self.assertEqual(
            43,
            wf.m.outputs.y.value,
            msg="The child channel should still exist and have run"
        )
        self.assertEqual(
            1,
            wf.inputs.intermediate_x.value,
            msg="IO should be up-to-date post-run"
        )
        self.assertEqual(
            2,
            wf.outputs.intermediate_y.value,
            msg="IO should be up-to-date post-run"
        )

    def test_io_map_bijectivity(self):
        wf = Workflow("wf")
        with self.assertRaises(
            ValueDuplicationError,
            msg="Should not be allowed to map two children's channels to the same label"
        ):
            wf.inputs_map = {"n1__x": "x", "n2__x": "x"}

        wf.inputs_map = {"n1__x": "x"}
        with self.assertRaises(
            ValueDuplicationError,
            msg="Should not be allowed to update a second child's channel onto an "
                "existing mapped channel"
        ):
            wf.inputs_map["n2__x"] = "x"

        with self.subTest("Ensure we can use None to turn multiple off"):
            wf.inputs_map = {"n1__x": None, "n2__x": None}  # At once
            # Or in a row
            wf.inputs_map = {}
            wf.inputs_map["n1__x"] = None
            wf.inputs_map["n2__x"] = None
            wf.inputs_map["n3__x"] = None
            self.assertEqual(
                3,
                len(wf.inputs_map),
                msg="All entries should be stored"
            )
            self.assertEqual(
                0,
                len(wf.inputs),
                msg="No IO should be left exposed"
            )

    def test_is_parentmost(self):
        wf = Workflow("wf")
        wf2 = Workflow("wf2")

        with self.assertRaises(
            ParentMostError,
            msg="Workflows are promised in the docs to be parent-most"
        ):
            wf.parent = wf2

        with self.assertRaises(
            ParentMostError,
            msg="We want to catch parent-most failures early when assigning children"
        ):
            wf.sub_wf = wf2

    def test_with_executor(self):

        wf = Workflow("wf")
        wf.a = wf.create.function_node(plus_one)
        wf.b = wf.create.function_node(plus_one, x=wf.a)

        original_a = wf.a
        wf.executor = wf.create.Executor()

        self.assertIs(
            NOT_DATA,
            wf.outputs.b__y.value,
            msg="Sanity check that test is in right starting condition"
        )

        result = wf(a__x=0)
        self.assertIsInstance(
            result,
            Future,
            msg="Should be running as a parallel process"
        )

        returned_nodes = result.result(timeout=120)  # Wait for the process to finish
        self.assertIsNot(
            original_a,
            returned_nodes.a,
            msg="Executing in a parallel process should be returning new instances"
        )
        self.assertIs(
            wf,
            wf.a.parent,
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
        wf.executor_shutdown()

    def test_parallel_execution(self):
        wf = Workflow("wf")

        @Workflow.wrap.as_function_node()
        def five(sleep_time=0.):
            sleep(sleep_time)
            five = 5
            return five

        @Workflow.wrap.as_function_node("sum")
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
            NOT_DATA,
            msg="The slow node _should_ hold up the downstream node to which it inputs"
        )

        wf.slow.future.result(timeout=120)  # Wait for it to finish
        self.assertFalse(
            wf.slow.running,
            msg="The slow node should be done running"
        )

        wf.sum.run()
        self.assertEqual(
            wf.sum.outputs.sum.value,
            5 + 5,
            msg="After the slow node completes, its output should be updated as a "
                "callback, and downstream nodes should proceed"
        )

        wf.executor_shutdown()

    def test_call(self):
        wf = Workflow("wf")

        wf.a = wf.create.function_node(plus_one)
        wf.b = wf.create.function_node(plus_one)

        @Workflow.wrap.as_function_node("sum")
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
        wf.a = wf.create.function_node(plus_one)
        wf.b = wf.create.function_node(plus_one, x=wf.a)

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
        @Workflow.wrap.as_function_node("out")
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
        user.n1l >> user.n1r >> user.n2l
        user.n1r >> user.n2m
        user.n1r >> user.n2r
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
            user.n1r.signals.output.ran,
            user.n2r.signals.input.accumulate_and_run.connections,
            msg="The automated flow uses a non-linear accumulating approach, so the "
                "accumulating run signal is the one that should hold a connection"
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

    def test_pull_and_executors(self):
        @Workflow.wrap.as_macro_node("three__result")
        def add_three_macro(self, one__x):
            self.one = Workflow.create.function_node(plus_one, x=one__x)
            self.two = Workflow.create.function_node(plus_one, x=self.one)
            self.three = Workflow.create.function_node(plus_one, x=self.two)
            return self.three

        wf = Workflow("pulling")

        wf.n1 = Workflow.create.function_node(plus_one, x=0)
        wf.m = add_three_macro(one__x=wf.n1)

        self.assertEquals(
            (0 + 1) + (1 + 1),
            wf.m.two.pull(run_parent_trees_too=True),
            msg="Sanity check, pulling here should work perfectly fine"
        )

        wf.m.one.executor = wf.create.Executor()
        with self.assertRaises(
            ValueError,
            msg="Should not be able to pull with executor in local scope"
        ):
            wf.m.two.pull()
            wf.m.one.executor_shutdown()  # Shouldn't get this far, but if so, shutdown
        wf.m.one.executor = None

        wf.n1.executor = wf.create.Executor()
        with self.assertRaises(
            ValueError,
            msg="Should not be able to pull with executor in parent scope"
        ):
            wf.m.two.pull(run_parent_trees_too=True)

        # Pulling in the local scope should be fine with an executor only in the parent
        # scope
        wf.m.two.pull(run_parent_trees_too=False)
        wf.executor_shutdown()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_storage_values(self):
        for backend in Workflow.allowed_backends():
            with self.subTest(backend):
                try:
                    print("Trying", backend)
                    wf = Workflow("wf", storage_backend=backend)
                    wf.register("static.demo_nodes", domain="demo")
                    wf.inp = wf.create.demo.AddThree(x=0)
                    wf.out = wf.inp.outputs.add_three + 1
                    wf_out = wf()
                    three_result = wf.inp.three.outputs.add.value

                    if backend == "h5io":
                        with self.assertRaises(
                            TypeError,
                            msg="h5io can't handle custom reconstructors"
                        ):
                            wf.save()
                    else:
                        wf.save()
                        reloaded = Workflow("wf", storage_backend=backend)
                        self.assertEqual(
                            wf_out.out__add,
                            reloaded.outputs.out__add.value,
                            msg="Workflow-level data should get reloaded"
                        )
                        self.assertEqual(
                            three_result,
                            reloaded.inp.three.value,
                            msg="Child data arbitrarily deep should get reloaded"
                        )
                finally:
                    # Clean up after ourselves
                    wf.storage.delete()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_storage_scopes(self):
        wf = Workflow("wf")
        wf.register("static.demo_nodes", "demo")

        # Test invocation
        wf.add_child(wf.create.demo.AddPlusOne(label="by_add"))
        # Note that the type hint `Optional[int]` from OptionallyAdd defines a custom
        # reconstructor, which borks h5io

        for backend in Workflow.allowed_backends():
            with self.subTest(backend):
                try:
                    for backend in Workflow.allowed_backends():
                        if backend == "h5io":
                            with self.subTest(backend):
                                with self.assertRaises(
                                    TypeError,
                                    msg="h5io can't handle custom reconstructors"
                                ):
                                    wf.storage_backend = backend
                                    wf.save()
                        else:
                            with self.subTest(backend):
                                wf.storage_backend = backend
                                wf.save()
                                Workflow(wf.label, storage_backend=backend)
                finally:
                    wf.storage.delete()

        with self.subTest("No unimportable nodes for either back-end"):
            try:
                wf.import_type_mismatch = wf.create.demo.dynamic()
                for backend in Workflow.allowed_backends():
                    with self.subTest(backend):
                        with self.assertRaises(
                            TypeNotFoundError,
                            msg="Imported object is function but node type is node -- "
                                "should fail early on save"
                        ):
                            wf.storage_backend = backend
                            wf.save()
            finally:
                wf.remove_child(wf.import_type_mismatch)
                wf.storage.delete()

        if "h5io" in Workflow.allowed_backends():
            wf.add_child(PlusOne(label="local_but_importable"))
            try:
                with self.assertRaises(
                    TypeError, msg="h5io can't handle custom reconstructors"
                ):
                    wf.storage_backend = "h5io"
                    wf.save()
            finally:
                wf.storage.delete()

        if "tinybase" in Workflow.allowed_backends():
            try:
                with self.assertRaises(
                    NotImplementedError,
                    msg="Storage docs for tinybase claim all children must be registered "
                        "nodes"
                ):
                    wf.storage_backend = "tinybase"
                    wf.save()
            finally:
                wf.storage.delete()

        if "h5io" in Workflow.allowed_backends():
            with self.subTest("Instanced node"):
                wf.direct_instance = Workflow.create.function_node(plus_one)
                try:
                    with self.assertRaises(
                        TypeNotFoundError,
                        msg="No direct node instances, only children with functions as "
                            "_class_ attribtues"
                    ):
                        wf.storage_backend = "h5io"
                        wf.save()
                finally:
                    wf.remove_child(wf.direct_instance)
                    wf.storage.delete()

        with self.subTest("Unimportable node"):
            @Workflow.wrap.as_function_node("y")
            def UnimportableScope(x):
                return x

            wf.unimportable_scope = UnimportableScope()

            if "h5io" in Workflow.allowed_backends():
                try:
                    with self.assertRaises(
                        TypeNotFoundError,
                        msg="Nodes must live in an importable scope to save with the "
                            "h5io backend"
                    ):
                        wf.storage_backend = "h5io"
                        wf.save()
                finally:
                    wf.remove_child(wf.unimportable_scope)
                    wf.storage.delete()

    def test_pickle(self):
        wf = Workflow("wf")
        wf.register("static.demo_nodes", domain="demo")
        wf.inp = wf.create.demo.AddThree(x=0)
        wf.out = wf.inp.outputs.add_three + 1
        wf_out = wf()
        reloaded = pickle.loads(pickle.dumps(wf))
        self.assertDictEqual(
            wf_out,
            reloaded.outputs.to_value_dict(),
            msg="Pickling should work"
        )


if __name__ == '__main__':
    unittest.main()
