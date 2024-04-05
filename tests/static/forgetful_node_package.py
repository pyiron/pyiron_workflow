"""
An incorrect node package for the purpose of testing node package registration.
"""

from pyiron_workflow import Workflow


@Workflow.wrap.function_node("sum")
def Add(x: int, y: int) -> int:
    return x + y


# nodes = [Add]  # Oops, we "forgot" to populate a `nodes` list
