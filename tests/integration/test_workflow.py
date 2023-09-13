import unittest

import numpy as np

from pyiron_contrib.workflow.channels import OutputSignal
from pyiron_contrib.workflow.function import Function
from pyiron_contrib.workflow.workflow import Workflow


class TestTopology(unittest.TestCase):
    def test_manually_constructed_cyclic_graph(self):
        """
        Check that cyclic graphs run.
        """

        @Workflow.wrap_as.single_value_node()
        def numpy_randint(low=0, high=20):
            rand = np.random.randint(low=low, high=high)
            print(f"Generating random number between {low} and {high}...{rand}!")
            return rand

        class GreaterThanLimitSwitch(Function):
            """
            A switch class for sending signal output depending on a '>' check
            applied to input
            """

            def __init__(self, **kwargs):
                super().__init__(
                    self.greater_than,
                    output_labels="value_gt_limit",
                    **kwargs
                )
                self.signals.output.true = OutputSignal("true", self)
                self.signals.output.false = OutputSignal("false", self)

            @staticmethod
            def greater_than(value, limit=10):
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

        @Workflow.wrap_as.single_value_node()
        def numpy_sqrt(value=0):
            sqrt = np.sqrt(value)
            print(f"sqrt({value}) = {sqrt}")
            return sqrt

        wf = Workflow("rand_until_big_then_sqrt", automate_execution=False)

        wf.rand = numpy_randint()

        wf.gt_switch = GreaterThanLimitSwitch()
        wf.gt_switch.inputs.value = wf.rand

        wf.sqrt = numpy_sqrt()
        wf.sqrt.inputs.value = wf.rand

        wf.gt_switch.signals.output.false > wf.rand > wf.gt_switch  # Loop on false
        wf.gt_switch.signals.output.true > wf.sqrt  # On true break to sqrt node
        wf.starting_nodes = [wf.rand]

        wf.run()
        self.assertAlmostEqual(
            np.sqrt(wf.rand.outputs.rand.value), wf.sqrt.outputs.sqrt.value, 6
        )

    def test_for_loop(self):
        n = 5

        bulk_loop = Workflow.create.meta.for_loop(
            Workflow.create.atomistics.Bulk,
            n,
            iterate_on=("a",),
        )()

        out = bulk_loop(
            name="Al",  # Sent equally to each body node
            A=np.linspace(3.9, 4.1, n).tolist(),  # Distributed across body nodes
        )

        self.assertTrue(
            np.allclose(
                [struct.cell.volume for struct in out.STRUCTURE],
                [
                    14.829749999999995,
                    15.407468749999998,
                    15.999999999999998,
                    16.60753125,
                    17.230249999999995
                ]
            )
        )

    def test_while_loop(self):
        with self.subTest("Random"):
            np.random.seed(0)

            @Workflow.wrap_as.single_value_node("random")
            def random(length: int | None = None):
                return np.random.random(length)

            @Workflow.wrap_as.single_value_node("gt")
            def greater_than(x: float, threshold: float):
                return x > threshold

            RandomWhile = Workflow.create.meta.while_loop(
                loop_body_class=random,
                condition_class=greater_than,
                internal_connection_map=[("Random", "random", "GreaterThan", "x")],
                outputs_map={"Random__random": "capped_result"}
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
                0.07103605819788694,  # For this reason we set the random seed
            )

        with self.subTest("Self-data-loop"):

            @Workflow.wrap_as.single_value_node()
            def add(a, b):
                return a + b

            @Workflow.wrap_as.single_value_node()
            def less_than_ten(value):
                return value < 10

            AddWhile = Workflow.create.meta.while_loop(
                loop_body_class=add,
                condition_class=less_than_ten,
                internal_connection_map=[
                    ("Add", "a + b", "LessThanTen", "value"),
                    ("Add", "a + b", "Add", "a")
                ],
                inputs_map={"Add__a": "a", "Add__b": "b"},
                outputs_map={"Add__a + b": "total"}
            )

            wf = Workflow("do_while")
            wf.add_while = AddWhile()

            wf.inputs_map = {
                "add_while__a": "a",
                "add_while__b": "b"
            }
            wf.outputs_map = {"add_while__total": "total"}

            out = wf(a=1, b=2)
            self.assertEqual(out.total, 11)
