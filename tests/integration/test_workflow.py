import math
import pickle
import random
import time
import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import OutputSignal
from pyiron_workflow.nodes.function import Function
from pyiron_workflow.workflow import Workflow

ensure_tests_in_python_path()
from static import demo_nodes

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


class TestTopology(unittest.TestCase):
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
            def emitting_channels(self) -> tuple[OutputSignal]:
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
            y=to_add  # Scattered
        )
        out = bulk_loop()

        for output, expectation in zip(
            out.df["sum"].values.tolist(),
            [base + v for v in to_add]
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
            Workflow.create.ExecutorlibExecutor
        ]

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
            with self.subTest(
                f"{exe_cls.__module__}.{exe_cls.__qualname__} entire workflow"
            ):
                with exe_cls() as exe:
                    wf.executor = exe
                    self.assertDictEqual(
                        reference_output,
                        wf().result().outputs.to_value_dict()
                    )
                    self.assertFalse(
                        wf.running,
                        msg="The workflow should stop. For thread pool this required a "
                            "little sleep"
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
                        f"{[(n.label, n.running) for n in wf]}"
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
                "the composite graph has changed"
        )

        t0 = time.perf_counter()
        third_out = wf()
        dt = time.perf_counter() - t0
        self.assertEqual(
            third_out,
            second_out,
            msg="This time there is no change and we expect the cached result"
        )
        self.assertLess(
            dt,
            0.1 * wf.c.inputs.t.value,
            msg="And because it used the cache we expect it much faster than the sleep "
                "time"
        )


if __name__ == '__main__':
    unittest.main()
