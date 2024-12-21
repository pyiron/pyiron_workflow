"""
A demo node package for the purpose of testing.
"""

from pyiron_workflow import Workflow


@Workflow.wrap.as_function_node("sum")
def OptionallyAdd(x: int, y: int | None = None) -> int:
    y = 0 if y is None else y
    return x + y
