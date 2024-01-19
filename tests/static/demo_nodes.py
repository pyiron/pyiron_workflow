"""
A demo node package for the purpose of testing node package registration.
"""

from typing import Optional

from pyiron_workflow import Workflow


@Workflow.wrap_as.single_value_node("sum")
def OptionallyAdd(x: int, y: Optional[int] = None) -> int:
    y = 0 if y is None else y
    return x + y


@Workflow.wrap_as.macro_node("add_three")
def AddThree(macro, x: int) -> int:
    macro.one = macro.create.standard.Add(x, 1)
    macro.two = macro.create.standard.Add(macro.one, 1)
    macro.three = macro.create.standard.Add(macro.two, 1)
    return macro.three


nodes = [OptionallyAdd, AddThree]
