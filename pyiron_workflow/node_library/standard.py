"""
Common-use nodes relying only on the standard library
"""

from __future__ import annotations

from inspect import isclass

from pyiron_workflow.channels import NotData, OutputSignal
from pyiron_workflow.function import SingleValue, single_value_node


@single_value_node()
def user_input(user_input):
    return user_input


class If(SingleValue):
    """
    Has two extra signal channels: true and false. Evaluates the input as a boolean and
    fires the corresponding output signal after running.
    """

    def __init__(self, **kwargs):
        super().__init__(self.if_, output_labels="truth", **kwargs)
        self.signals.output.true = OutputSignal("true", self)
        self.signals.output.false = OutputSignal("false", self)

    @staticmethod
    def if_(condition):
        if isclass(condition) and issubclass(condition, NotData):
            raise TypeError(f"Logic 'If' node expected data but got NotData as input.")
        return bool(condition)

    def process_run_result(self, function_output):
        """
        Process the output as usual, then fire signals accordingly.
        """
        super().process_run_result(function_output)

        if self.outputs.truth.value:
            self.signals.output.true()
        else:
            self.signals.output.false()


nodes = [
    user_input,
    If,
]
