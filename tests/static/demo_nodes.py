"""
A demo node package for the purpose of testing.
"""

from pyiron_workflow import Workflow


@Workflow.wrap.as_function_node("sum")
def OptionallyAdd(x: int, y: int | None = None) -> int:
    y = 0 if y is None else y
    return x + y


@Workflow.wrap.as_macro_node("add_three")
def AddThree(self, x: int) -> int:
    self.one = self.create.std.Add(x, 1)
    self.two = self.create.std.Add(self.one, 1)
    self.three = self.create.std.Add(self.two, 1)
    return self.three


@Workflow.wrap.as_macro_node("add_six")
def AddSix(self, x: int) -> int:
    self.a = AddThree(x)
    self.b = AddThree(self.a)
    return self.b


@Workflow.wrap.as_function_node("add")
def AddPlusOne(obj, other):
    """The same IO labels as `standard.Add`, but with type hints and a boost."""
    return obj + other + 1


def dynamic(x):
    return x + 1


Dynamic = Workflow.wrap.as_function_node(dynamic)


@Workflow.wrap.as_function_node("y")
def _APrivateNode(x):
    """A node, but named to indicate it is private"""
    return x + 1
