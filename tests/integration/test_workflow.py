import math
import pickle
import random
import time
import unittest
from concurrent import futures

import executorlib
import rdflib
from semantikon.metadata import u
from static import demo_nodes

import pyiron_workflow as pwf
from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import OutputSignal
from pyiron_workflow.data import NOT_DATA
from pyiron_workflow.nodes.composite import FailedChildError
from pyiron_workflow.nodes.function import Function
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


HISTORY: str = ""


@Workflow.wrap.as_function_node(use_cache=False)
def SideEffect(x):
    y = x + 1
    global HISTORY  # noqa: PLW0603
    HISTORY += f"{y}"
    return y


EX = rdflib.Namespace("http://www.example.org/")


@pwf.as_dataclass_node(
    uri=EX.Jar
)  # Can pass u-kwargs to decorate the returned dataclass
class Jar:
    threading: str = "clockwise"
    contents: u(str, uri=EX.Contents) = "jam"


@pwf.as_function_node
def ItsStuck(
    jar: u(Jar.dataclass, uri=EX.Jar),
) -> u(Jar.dataclass, derived_from="inputs.jar", triples=(EX.lidState, EX.stuck)):
    return jar


@pwf.as_function_node
def OpenStuckJar(
    jar: u(
        Jar.dataclass,
        uri=EX.Jar,
        restrictions=(
            (rdflib.OWL.onProperty, EX.lidState),
            (rdflib.OWL.someValuesFrom, EX.stuck),
        ),
    ),
) -> u(str, uri=EX.Contents):
    contents = jar.contents
    return contents


@pwf.as_function_node
def MakeSandwich(made_with: u(str, uri=EX.Contents)) -> u(str, uri=EX.Sandwich):
    sandwich = f"{made_with} sandwich"
    return sandwich


@pwf.as_macro_node
def LunchTime(
    self, contents: u(str, uri=EX.Contents)
) -> u(str, uri=EX.Sandwich, triples=(EX.madeWith, "inputs.contents")):
    self.jar = Jar(contents=contents)
    self.stuck_jar = ItsStuck(self.jar)
    self.open_jar = OpenStuckJar(self.stuck_jar)
    self.lunch = MakeSandwich(self.open_jar)
    return self.lunch


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
        wf.before_pickling.executor = futures.ProcessPoolExecutor()
        wf()
        wf.executor_shutdown()

        wf.before_pickling.executor = None
        wf.after_pickling = demo_nodes.OptionallyAdd(2, y=3)
        wf()

    def test_executors(self):
        executors = [
            futures.ProcessPoolExecutor,
            futures.ThreadPoolExecutor,
            Workflow.create.CloudpickleProcessPoolExecutor,
            executorlib.SingleNodeExecutor,
        ]

        wf = Workflow("executed")
        wf.a = Workflow.create.std.UserInput(42)  # Regular
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
                wf().result()  # Run it with the executor and wait for it to finish
                self.assertDictEqual(reference_output, wf.outputs.to_value_dict())
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
        wf.a = wf.create.std.UserInput(0)
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

        def _make_wf() -> Workflow:
            wf = Workflow("test")
            wf.a = Workflow.create.std.UserInput(1)
            wf.b = Workflow.create.std.UserInput("two")
            wf.c_fails = wf.a + wf.b  # Type error
            wf.d_if_success = Workflow.create.std.UserInput(0)
            wf.d_if_failure = Workflow.create.std.UserInput("But what's the question?")
            wf.e_fails = Workflow.create.std.Add(wf.d_if_failure, 42)  # Type error

            wf.a >> wf.b >> wf.c_fails >> wf.d_if_success
            wf.c_fails.signals.output.failed >> wf.d_if_failure >> wf.e_fails
            wf.starting_nodes = [wf.a]
            wf.automate_execution = False
            return wf

        with (
            self.subTest("Check completion"),
            futures.ProcessPoolExecutor() as exe,
        ):
            wf = _make_wf()
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
                wf = _make_wf()
                wf(raise_run_exceptions=True)
            except FailedChildError as e:
                with self.subTest("Check messaging"):
                    self.assertIn(
                        wf.c_fails.full_label,
                        str(e),
                        msg="Failed node should be identified",
                    )
                    self.assertIn(
                        wf.e_fails.full_label,
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

    def test_push_pull(self):
        global HISTORY  # noqa: PLW0603

        def _setup() -> tuple[str, Workflow]:
            wf = Workflow("push_pull")
            wf.n1 = SideEffect(0)
            wf.n2 = SideEffect(wf.n1)
            wf.n3 = SideEffect(wf.n2)
            return "", wf

        # Note that we have not triggered a first run of the workflow, and so do not
        # yet have DAG signal connections wired
        with self.subTest("Push without automatic configuration"):
            HISTORY, wf = _setup()
            wf.automate_execution = False
            wf.n1.push()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n1.outputs.y.value,
                        ],
                    )
                ),
                msg="Expected only the pushed node to run",
            )

        with self.subTest("Push without automatic configuration"):
            HISTORY, wf = _setup()
            wf.n1.push()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n1.outputs.y.value,
                            wf.n2.outputs.y.value,
                            wf.n3.outputs.y.value,
                        ],
                    )
                ),
                msg="With automated execution, pushing should get us the whole thing",
            )

        with self.subTest("Run parent"):
            HISTORY, wf = _setup()
            wf()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n1.outputs.y.value,
                            wf.n2.outputs.y.value,
                            wf.n3.outputs.y.value,
                        ],
                    )
                ),
                msg="Expected all three to run",
            )

        with self.subTest("Pull"):
            HISTORY, wf = _setup()
            wf.n2.pull()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n1.outputs.y.value,
                            wf.n2.outputs.y.value,
                        ],
                    )
                ),
                msg="Expected only upstream and this",
            )

        with self.subTest("Call"):
            HISTORY, wf = _setup()
            wf.n2.__call__()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n1.outputs.y.value,
                            wf.n2.outputs.y.value,
                        ],
                    )
                ),
                msg="Calling maps to a pull (+parent data tree)",
            )

        with self.subTest("Push"):
            HISTORY, wf = _setup()
            wf.n1.pull()
            HISTORY = ""
            wf.n2.push()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n2.outputs.y.value,
                            wf.n3.outputs.y.value,
                        ],
                    )
                ),
                msg="Expected only this and downstream",
            )

    def test_test_pull_isolation(self):
        global HISTORY  # noqa: PLW0603
        HISTORY = ""

        wf = Workflow("pull_isolation")
        wf.n1 = SideEffect(0)
        wf.a = SideEffect(wf.n1)
        wf.n2 = SideEffect(wf.n1)
        wf.b = SideEffect(wf.n2)
        wf.n3 = SideEffect(wf.n2)
        wf.c = SideEffect(wf.n3)
        wf.automate_execution = False
        wf.n1 >> wf.n2 >> wf.n3 >> wf.c
        wf.n1 >> wf.a
        wf.n2 >> wf.b
        wf.starting_nodes = [wf.n1]

        wf.n3.pull()
        self.assertEqual(
            HISTORY,
            "".join(
                map(
                    str,
                    [
                        wf.n1.outputs.y.value,
                        wf.n2.outputs.y.value,
                        wf.n3.outputs.y.value,
                    ],
                )
            ),
            msg="Only those in the main chain should have been run",
        )

    def test_push_pull_with_unconfigured_workflows(self):
        global HISTORY  # noqa: PLW0603

        wf = Workflow("push_pull")
        wf.n1 = SideEffect(0)
        wf.n2 = SideEffect(wf.n1)
        wf.n3 = SideEffect(wf.n2)

        with self.subTest("Just run"):
            self.assertListEqual(
                [],
                wf.n2.signals.output.ran.connections,
                msg="Sanity check -- we have never run the workflow, so the parent "
                "workflow has never had a chance to automatically configure its "
                "execution flow.",
            )
            wf.n1.pull()
            HISTORY = ""
            wf.n2.run()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [
                            wf.n2.outputs.y.value,
                        ],
                    )
                ),
                msg="With no signals configured, we expect the run to go nowhere",
            )

        with self.subTest("Just run"):
            self.assertListEqual(
                [],
                wf.n2.signals.output.ran.connections,
                msg="Sanity check -- we have never run the workflow, so the parent "
                "workflow has never had a chance to automatically configure its "
                "execution flow.",
            )
            wf.n1.pull()
            HISTORY = ""
            wf.n2.push()
            self.assertEqual(
                HISTORY,
                "".join(
                    map(
                        str,
                        [wf.n2.outputs.y.value, wf.n3.outputs.y.value],
                    )
                ),
                msg="Explicitly pushing should guarantee push-like behaviour even for "
                "un-configured workflows.",
            )

    def test_run_in_thread(self):
        """
        Should be able to run workflows in a background thread, even if child nodes
        have their own executors.
        """
        t_sleep = 1
        wf = Workflow("background")
        wf.n1 = Workflow.create.std.Sleep()
        wf.n2 = Workflow.create.std.Sleep(wf.n1)

        wf.n2.executor = (futures.ThreadPoolExecutor, (), {})  # Set by constructor
        with futures.ProcessPoolExecutor() as exe:
            wf.n1.executor = exe  # Set by instance
            wf.run_in_thread(n1__t=t_sleep)

            time.sleep(t_sleep * 0.9)  # Give the process pool time to spin up
            self.assertTrue(wf.running)
            self.assertTrue(wf.n1.running)
            self.assertFalse(wf.n2.running)
            self.assertIs(wf.outputs.n2__time.value, NOT_DATA)

            time.sleep(t_sleep)
            self.assertTrue(wf.running)
            self.assertFalse(wf.n1.running)
            self.assertTrue(wf.n2.running)
            self.assertIs(wf.outputs.n2__time.value, NOT_DATA)

            time.sleep(t_sleep)
            self.assertFalse(wf.running)
            self.assertFalse(wf.n1.running)
            self.assertFalse(wf.n2.running)
            self.assertEqual(wf.outputs.n2__time.value, t_sleep)

    def test_ontological_validation(self):
        wf = Workflow("lunch_for_three")
        wf.platter = pwf.for_node(
            body_node_class=LunchTime,
            iter_on="contents",
            contents=["jam", "honey", "butter"],
            output_as_dataframe=False,
        )
        out = wf()
        self.assertDictEqual(
            {
                "platter__contents": ["jam", "honey", "butter"],
                "platter__lunch": ["jam sandwich", "honey sandwich", "butter sandwich"],
            },
            out,
            msg="This is the complexity boundary of what we test for ontological "
            "valididation; it should validate ok.",
        )


if __name__ == "__main__":
    unittest.main()
