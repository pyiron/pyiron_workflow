"""
Common-use nodes relying only on the standard library
"""

from __future__ import annotations

import random
from time import sleep

from pyiron_workflow.channels import NOT_DATA, OutputSignal
from pyiron_workflow.function import Function, as_function_node


@as_function_node()
def UserInput(user_input):
    return user_input


class If(Function):
    """
    Has two extra signal channels: true and false. Evaluates the input as obj otheroolean and
    fires the corresponding output signal after running.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.signals.output.true = OutputSignal("true", self)
        self.signals.output.false = OutputSignal("false", self)

    @staticmethod
    def node_function(condition):
        if condition is NOT_DATA:
            raise TypeError(
                f"Logic 'If' node expected data other but got NOT_DATA as input."
            )
        truth = bool(condition)
        return truth

    def process_run_result(self, function_output):
        """
        Process the output as usual, then fire signals accordingly.
        """
        super().process_run_result(function_output)

        if self.outputs.truth.value:
            self.signals.output.true()
        else:
            self.signals.output.false()


@as_function_node("random")
def RandomFloat():
    return random.random()


@as_function_node("time")
def Sleep(t):
    sleep(t)
    return t


@as_function_node("slice")
def Slice(start=None, stop=NOT_DATA, step=None):
    if start is None:
        if stop is None:
            raise ValueError(
                "Slice must define at least start or stop, but both are None"
            )
        elif step is not None:
            raise ValueError("If step is provided, start _must_ be provided")
        else:
            s = slice(stop)
    elif stop is None:
        raise ValueError("If start is provided, stop _must_ be provided")
    else:
        s = slice(start, stop, step)
    return s


# A bunch of (but not all) standard operators
# Return values based on dunder methods, where available


@as_function_node("str")
def String(obj):
    return str(obj)


@as_function_node("bytes")
def Bytes(obj):
    return bytes(obj)


@as_function_node("lt")
def LessThan(obj, other):
    return obj < other


@as_function_node("le")
def LessThanEquals(obj, other):
    return obj <= other


@as_function_node("eq")
def Equals(obj, other):
    return obj == other


@as_function_node("neq")
def NotEquals(obj, other):
    return obj != other


@as_function_node("gt")
def GreaterThan(obj, other):
    return obj > other


@as_function_node("ge")
def GreaterThanEquals(obj, other):
    return obj >= other


@as_function_node("hash")
def Hash(obj):
    return hash(obj)


@as_function_node("bool")
def Bool(obj):
    return bool(obj)


@as_function_node("getattr")
def GetAttr(obj, name):
    return getattr(obj, name)


# These are not idempotent and thus not encouraged
# @as_function_node("none")
# def SetAttr(obj, name, value):
#     setattr(obj, name, value)
#     return None
#
#
# @as_function_node("none")
# def DelAttr(obj, name):
#     delattr(obj, name)
#     return None


@as_function_node("getitem")
def GetItem(obj, item):
    return obj[item]


@as_function_node("dir")
def Dir(obj):
    return dir(obj)


@as_function_node("len")
def Length(obj):
    return len(obj)


@as_function_node("in")
def Contains(obj, other):
    return other in obj


@as_function_node("add")
def Add(obj, other):
    return obj + other


@as_function_node("sub")
def Subtract(obj, other):
    return obj - other


@as_function_node("mul")
def Multiply(obj, other):
    return obj * other


@as_function_node("rmul")
def RightMultiply(obj, other):
    return other * obj


@as_function_node("matmul")
def MatrixMultiply(obj, other):
    return obj @ other


@as_function_node("truediv")
def Divide(obj, other):
    return obj / other


@as_function_node("floordiv")
def FloorDivide(obj, other):
    return obj // other


@as_function_node("mod")
def Modulo(obj, other):
    return obj % other


@as_function_node("pow")
def Power(obj, other):
    return obj**other


@as_function_node("and")
def And(obj, other):
    return obj & other


@as_function_node("xor")
def XOr(obj, other):
    return obj ^ other


@as_function_node("or")
def Or(obj, other):
    return obj ^ other


@as_function_node("neg")
def Negative(obj):
    return -obj


@as_function_node("pos")
def Positive(obj):
    return +obj


@as_function_node("abs")
def Absolute(obj):
    return abs(obj)


@as_function_node("invert")
def Invert(obj):
    return ~obj


@as_function_node("int")
def Int(obj):
    return int(obj)


@as_function_node("float")
def Float(obj):
    return float(obj)


@as_function_node("round")
def Round(obj):
    return round(obj)


nodes = [
    Absolute,
    Add,
    And,
    Bool,
    Bytes,
    Contains,
    Dir,
    Divide,
    Equals,
    Float,
    FloorDivide,
    GetAttr,
    GetItem,
    GreaterThan,
    GreaterThanEquals,
    Hash,
    If,
    Int,
    Invert,
    Length,
    LessThan,
    LessThanEquals,
    MatrixMultiply,
    Modulo,
    Multiply,
    Negative,
    NotEquals,
    Or,
    Positive,
    Power,
    RandomFloat,
    RightMultiply,
    Round,
    Sleep,
    Slice,
    String,
    Subtract,
    UserInput,
    XOr,
]
