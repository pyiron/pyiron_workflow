import contextlib
import math
import pickle
import random
import time
import unittest

from static import demo_nodes
from pyiron_database.instance_database import get_hash

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import NOT_DATA, OutputSignal
from pyiron_workflow.nodes.composite import FailedChildError
from pyiron_workflow.nodes.function import Function
from pyiron_workflow.nodes.standard import Sleep, UserInput
from pyiron_workflow.workflow import Workflow

ensure_tests_in_python_path()


@Workflow.wrap.as_function_node("random")
def RandomFloat() -> float:
    return random.random()


@Workflow.wrap.as_function_node("gt")
def GreaterThan(x: float, threshold: float):
    return x > threshold


def foo(x):
    y = x + 2
    return y


@Workflow.wrap.as_function_node("my_output")
def Bar(x):
    return x * x


class TestWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

    def test_manually_constructed_cyclic_graph(self):
        """
        Check that cyclic graphs run.
        """

        @Workflow.wrap.as_function_node(use_cache=False)
        def randint(low=0, high=20):
            rand = random.randint(low, high)
            print(f"Generating random number between {low} and {high}...{rand}!")
            return rand

        class GreaterThanLimitSwitch(Function):
            """
            A switch class for sending signal output depending on a '>' check
            applied to input
            """

            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.signals.output.true = OutputSignal("true", self)
                self.signals.output.false = OutputSignal("false", self)

            @staticmethod
            def node_function(value, limit=10):
                value_gt_limit = value > limit
                return value_gt_limit

            @property
            def emitting_channels(self) -> tuple[OutputSignal, ...]:
                if self.outputs.value_gt_limit.value:
                    print(f"{self.inputs.value.value} > {self.inputs.limit.value}")
                    return (*super().emitting_channels, self.signals.output.true)
                else:
                    print(f"{self.inputs.value.value} <= {self.inputs.limit.value}")
                    return (*super().emitting_channels, self.signals.output.false)

        @Workflow.wrap.as_function_node("sqrt")
        def sqrt(value=0):
            root_value = math.sqrt(value)
            print(f"sqrt({value}) = {root_value}")
            return root_value

        wf = Workflow("rand_until_big_then_sqrt", automate_execution=False)

        wf.rand = randint()

        wf.gt_switch = GreaterThanLimitSwitch()
        wf.gt_switch.inputs.value = wf.rand

        wf.sqrt = sqrt()
        wf.sqrt.inputs.value = wf.rand

        wf.gt_switch.signals.output.false >> wf.rand >> wf.gt_switch  # Loop on false
        wf.gt_switch.signals.output.true >> wf.sqrt  # On true break to sqrt node
        wf.starting_nodes = [wf.rand]

        wf.run()
        self.assertAlmostEqual(
            math.sqrt(wf.rand.outputs.rand.value), wf.sqrt.outputs.sqrt.value, 6
        )

    def test_for_loop(self):
        base = 42
        to_add = list(range(5))
        bulk_loop = Workflow.create.for_node(
            demo_nodes.OptionallyAdd,
            iter_on="y",
            x=base,  # Broadcast
            y=to_add,  # Scattered
        )
        out = bulk_loop()

        for output, expectation in zip(
            out.df["sum"].values.tolist(), [base + v for v in to_add], strict=False
        ):
            self.assertAlmostEqual(
                output,
                expectation,
            )

    def test_executor_and_creator_interaction(self):
        """
        Make sure that submitting stuff to a parallel processor doesn't stop us from
        using the same stuff on the main process. This can happen because the
        (de)(cloud)pickle process messes with the `__globals__` attribute of the node
        function, and since the node function is a class attribute the original node
        gets updated on de-pickling.
        We code around this, but lets make sure it stays working by adding a test!
        Critical in this test is that the node used has complex type hints.

        C.f. `pyiron_workflow.function._wrapper_factory` for more detail.
        """
        wf = Workflow("depickle")

        wf.before_pickling = demo_nodes.OptionallyAdd(1)
        wf.before_pickling.executor = wf.create.ProcessPoolExecutor()
        wf()
        wf.before_pickling.future.result(timeout=120)  # Wait for it to finish
        wf.executor_shutdown()

        wf.before_pickling.executor = None
        wf.after_pickling = demo_nodes.OptionallyAdd(2, y=3)
        wf()

    def test_executors(self):
        executors = [
            Workflow.create.ProcessPoolExecutor,
            Workflow.create.ThreadPoolExecutor,
            Workflow.create.CloudpickleProcessPoolExecutor,
        ]
        try:
            executors.append(Workflow.create.executorlib.SingleNodeExecutor)
        except AttributeError:
            # executorlib < 0.1 had an Executor with optional backend parameter (defaulting to SingleNodeExecutor)
            executors.append(Workflow.create.executorlib.Executor)

        wf = Workflow("executed")
        wf.a = Workflow.create.standard.UserInput(42)  # Regular
        wf.b = wf.a + 1  # Injected
        wf.c = Workflow.create.function_node(foo, wf.b)  # Instantiated from function
        wf.d = Bar(wf.c)  # From decorated function
        wf.use_cache = False

        reference_output = wf()

        with self.subTest("Pickle sanity check"):
            reloaded = pickle.loads(pickle.dumps(wf))
            self.assertDictEqual(reference_output, reloaded.outputs.to_value_dict())

        for exe_cls in executors:
            with (
                self.subTest(
                    f"{exe_cls.__module__}.{exe_cls.__qualname__} entire workflow"
                ),
                exe_cls() as exe,
            ):
                wf.executor = exe
                self.assertDictEqual(
                    reference_output, wf().result().outputs.to_value_dict()
                )
                self.assertFalse(
                    wf.running,
                    msg="The workflow should stop. For thread pool this required a "
                    "little sleep",
                )
            wf.executor = None

            with self.subTest(f"{exe_cls.__module__}.{exe_cls.__qualname__} each node"):
                with exe_cls() as exe:
                    for child in wf:
                        child.executor = exe
                    executed_output = wf()
                self.assertDictEqual(reference_output, executed_output)
                self.assertFalse(
                    any(n.running for n in wf),
                    msg=f"All children should be done running -- for thread pools this "
                    f"requires a very short sleep -- got "
                    f"{[(n.label, n.running) for n in wf]}",
                )
            for child in wf:
                child.executor = None

    def test_cache(self):
        wf = Workflow("tmp")
        wf.use_cache = True
        wf.a = wf.create.standard.UserInput(0)
        wf.b = wf.a + 1

        first_out = wf()

        @Workflow.wrap.as_function_node("as_string")
        def Sleep(t):
            time.sleep(t)
            return "slept"

        wf.c = Sleep(wf.b)

        second_out = wf()
        self.assertNotEqual(
            first_out,
            second_out,
            msg="Even thought the _input_ hasn't changed, we expect to avoid the first "
            "(cached) result by virtue of resetting the cache when the body of "
            "the composite graph has changed",
        )

        t0 = time.perf_counter()
        third_out = wf()
        dt = time.perf_counter() - t0
        self.assertEqual(
            third_out,
            second_out,
            msg="This time there is no change and we expect the cached result",
        )
        self.assertLess(
            dt,
            0.1 * wf.c.inputs.t.value,
            msg="And because it used the cache we expect it much faster than the sleep "
            "time",
        )

    def test_failure(self):
        """
        We allow a node to continue in the wake of failure, and have a signal to
        indicate that the node failed.

        Let's push this to the limits by (a) having the failure occur on a remote
        process, and (b) chaining a second error downstream by leveraging the `failed`
        signal.
        """
        wf = Workflow("test")
        wf.a = Workflow.create.standard.UserInput(1)
        wf.b = Workflow.create.standard.UserInput("two")
        wf.c_fails = wf.a + wf.b  # Type error
        wf.d_if_success = Workflow.create.standard.UserInput(0)
        wf.d_if_failure = Workflow.create.standard.UserInput("But what's the question?")
        wf.e_fails = Workflow.create.standard.Add(wf.d_if_failure, 42)  # Type error

        wf.a >> wf.b >> wf.c_fails >> wf.d_if_success
        wf.c_fails.signals.output.failed >> wf.d_if_failure >> wf.e_fails
        wf.starting_nodes = [wf.a]
        wf.automate_execution = False

        with (
            self.subTest("Check completion"),
            Workflow.create.ProcessPoolExecutor() as exe,
        ):
            wf.c_fails.executor = exe
            wf(raise_run_exceptions=False)

            for data, expectation in [
                (wf.a.outputs.user_input.value, wf.a.inputs.user_input.value),
                (wf.b.outputs.user_input.value, wf.b.inputs.user_input.value),
                (wf.c_fails.outputs.add.value, NOT_DATA),
                (wf.d_if_success.outputs.user_input.value, NOT_DATA),  # Never ran
                (
                    wf.d_if_failure.outputs.user_input.value,
                    wf.d_if_failure.inputs.user_input.value,
                ),
                (wf.e_fails.outputs.add.value, NOT_DATA),
            ]:
                with self.subTest("Data expecations"):
                    self.assertEqual(data, expectation)

            for status, expectation in [
                (wf.a.failed, False),
                (wf.b.failed, False),
                (wf.c_fails.failed, True),
                (wf.d_if_success.failed, False),
                (wf.d_if_failure.failed, False),
                (wf.e_fails.failed, True),
                (wf.failed, True),
            ]:
                with self.subTest("Failure status expecations"):
                    self.assertEqual(status, expectation)

        with self.subTest("Let it fail"):
            try:
                wf(raise_run_exceptions=True)
            except FailedChildError as e:
                with self.subTest("Check messaging"):
                    self.assertIn(
                        wf.c_fails.run.full_label,
                        str(e),
                        msg="Failed node should be identified",
                    )
                    self.assertIn(
                        wf.e_fails.run.full_label,
                        str(e),
                        msg="Indeed, _both_ failed nodes should be identified",
                    )

                with self.subTest("Check recovery file"):
                    self.assertTrue(
                        wf.has_saved_content(
                            filename=wf.as_path().joinpath("recovery")
                        ),
                        msg="Expect a recovery file to be written for the parent-most"
                        "object when a child fails",
                    )
            finally:
                wf.delete_storage()
                wf.delete_storage(filename=wf.as_path().joinpath("recovery"))
                self.assertFalse(
                    wf.as_path().exists(),
                    msg=f"The parent-most object is the _only_ one who should have "
                    f"written a recovery file, so after removing that the whole "
                    f"node directory for the workflow should be cleaned up."
                    f"Instead, {wf.as_path()} exists and has content "
                    f"{list(wf.as_path().iterdir()) if wf.as_path().is_dir() else None}",
                )

    def test_out_of_process_caching(self):
        wf = Workflow("out_of_process_write")
        wf.time = UserInput(1)
        wf.s = Sleep(wf.time, file_cache=".")
        hash_ = get_hash(wf.s)
        t0 = time.time()
        wf()
        dt0 = time.time() - t0

        wf2 = Workflow("out_of_process_read")
        wf2.time = UserInput(1)
        wf2.s = Sleep(wf.time, file_cache=".")

        t1 = time.time()
        wf()
        dt1 = time.time() - t1

        self.assertLess(
            dt1, 0.1 * dt0,
            msg="On the second go we expect to read the cache and bypass actually "
                "sleeping, even though this is a totally different workflow",
        )
        wf.s.file_cache.joinpath(hash_).unlink()



if __name__ == "__main__":
    unittest.main()
