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

        wf = Workflow("rand_until_big_then_sqrt")

        wf.rand = numpy_randint(update_on_instantiation=False)

        wf.gt_switch = GreaterThanLimitSwitch(run_on_updates=False)
        wf.gt_switch.inputs.value = wf.rand

        wf.sqrt = numpy_sqrt(run_on_updates=False)
        wf.sqrt.inputs.value = wf.rand

        wf.gt_switch.signals.input.run = wf.rand.signals.output.ran
        wf.sqrt.signals.input.run = wf.gt_switch.signals.output.true
        wf.rand.signals.input.run = wf.gt_switch.signals.output.false

        wf.rand.update()
        self.assertAlmostEqual(
            np.sqrt(wf.rand.outputs.rand.value), wf.sqrt.outputs.sqrt.value, 6
        )
