"""
A demo node package for the purpose of testing node package registration.
"""

from typing import Optional

from pyiron_workflow import Workflow


@Workflow.wrap.as_function_node("sum")
def OptionallyAdd(x: int, y: Optional[int] = None) -> int:
    y = 0 if y is None else y
    return x + y


@Workflow.wrap.as_macro_node("add_three")
def AddThree(self, x: int) -> int:
    self.one = self.create.standard.Add(x, 1)
    self.two = self.create.standard.Add(self.one, 1)
    self.three = self.create.standard.Add(self.two, 1)
    return self.three


@Workflow.wrap.as_function_node("add")
def AddPlusOne(obj, other):
    """The same IO labels as `standard.Add`, but with type hints and a boost."""
    return obj + other + 1


def dynamic(x):
    return x + 1


Dynamic = Workflow.wrap.as_function_node()(dynamic)

nodes = [OptionallyAdd, AddThree, AddPlusOne, Dynamic]
