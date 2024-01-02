"""
An incorrect node package for the purpose of testing node package registration.
"""

from pyiron_workflow import Workflow


@Workflow.wrap_as.single_value_node("sum")
def Add(x: int, y: int) -> int:
    return x + y


nodes = [Add, 42]  # Not everything here is a node!
