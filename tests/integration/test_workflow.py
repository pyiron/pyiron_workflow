import math
import random
import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.channels import OutputSignal
from pyiron_workflow.function import Function
from pyiron_workflow.workflow import Workflow


class TestTopology(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ensure_tests_in_python_path()
        super().setUpClass()

    def test_manually_constructed_cyclic_graph(self):
        """
        Check that cyclic graphs run.
        """

        @Workflow.wrap_as.single_value_node()
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
                super().__init__(
                    None,
                    output_labels="value_gt_limit",
                    **kwargs
                )
                self.signals.output.true = OutputSignal("true", self)
                self.signals.output.false = OutputSignal("false", self)

            @staticmethod
            def node_function(value, limit=10):
                return value > limit

            def process_run_result(self, function_output):
                """
                Process the output as usual, then fire signals accordingly.
                """
                super().process_run_result(function_output)

                if self.outputs.value_gt_limit.value:
                    print(f"{self.inputs.value.value} > {self.inputs.limit.value}")
                    self.signals.output.true()
                else:
                    print(f"{self.inputs.value.value} <= {self.inputs.limit.value}")
                    self.signals.output.false()

        @Workflow.wrap_as.single_value_node("sqrt")
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
        Workflow.register("static.demo_nodes", "demo")

        n = 5

        bulk_loop = Workflow.create.meta.for_loop(
            Workflow.create.demo.OptionallyAdd,
            n,
            iterate_on=("y",),
        )()

        base = 42
        to_add = list(range(n))
        out = bulk_loop(
            x=base,  # Sent equally to each body node
            Y=to_add,  # Distributed across body nodes
        )

        for output, expectation in zip(out.SUM, [base + v for v in to_add]):
            self.assertAlmostEqual(
                output,
                expectation,
                msg="Output should be list result of each individiual result"
            )

    def test_while_loop(self):
        with self.subTest("Random"):
            random.seed(0)

            @Workflow.wrap_as.single_value_node("random")
            def RandomFloat() -> float:
                return random.random()

            @Workflow.wrap_as.single_value_node("gt")
            def GreaterThan(x: float, threshold: float):
                return x > threshold

            RandomWhile = Workflow.create.meta.while_loop(
                loop_body_class=RandomFloat,
                condition_class=GreaterThan,
                internal_connection_map=[
                    ("RandomFloat", "random", "GreaterThan", "x")
                ],
                outputs_map={"RandomFloat__random": "capped_result"}
            )

            # Define workflow

            wf = Workflow("random_until_small_enough")

            ## Wire together the while loop and its condition

            wf.random_while = RandomWhile()

            ## Give convenient labels
            wf.inputs_map = {"random_while__GreaterThan__threshold": "threshold"}
            wf.outputs_map = {"random_while__capped_result": "capped_result"}

            self.assertAlmostEqual(
                wf(threshold=0.1).capped_result,
                0.014041700164018955,  # For this reason we set the random seed
            )

        with self.subTest("Self-data-loop"):

            AddWhile = Workflow.create.meta.while_loop(
                loop_body_class=Workflow.create.standard.Add,
                condition_class=Workflow.create.standard.LessThan,
                internal_connection_map=[
                    ("Add", "add", "LessThan", "obj"),
                    ("Add", "add", "Add", "obj")
                ],
                inputs_map={
                    "Add__obj": "a",
                    "Add__other": "b",
                    "LessThan__other": "cap",
                },
                outputs_map={"Add__add": "total"}
            )

            wf = Workflow("do_while")
            wf.add_while = AddWhile()

            wf.inputs_map = {
                "add_while__a": "a",
                "add_while__b": "b",
                "add_while__cap": "cap"
            }
            wf.outputs_map = {"add_while__total": "total"}

            out = wf(a=1, b=2, cap=10)
            self.assertEqual(out.total, 11)

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
        wf.register("static.demo_nodes", "demo")

        wf.before_pickling = wf.create.demo.OptionallyAdd(1)
        wf.before_pickling.executor = wf.create.Executor()
        wf()
        wf.before_pickling.future.result(timeout=120)  # Wait for it to finish
        wf.executor_shutdown()

        wf.before_pickling.executor = None
        wf.after_pickling = wf.create.demo.OptionallyAdd(2, y=3)
        wf()


if __name__ == '__main__':
    unittest.main()
