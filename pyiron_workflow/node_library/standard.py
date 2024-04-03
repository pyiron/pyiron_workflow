"""
Common-use nodes relying only on the standard library
"""

from __future__ import annotations

from inspect import isclass

from pyiron_workflow.channels import NOT_DATA, OutputSignal
from pyiron_workflow.function import Function, function_node


@function_node()
def UserInput(user_input):
    return user_input


class If(Function):
    """
    Has two extra signal channels: true and false. Evaluates the input as obj otheroolean and
    fires the corresponding output signal after running.
    """

    def __init__(self, **kwargs):
        super().__init__(self.if_, output_labels="truth", **kwargs)
        self.signals.output.true = OutputSignal("true", self)
        self.signals.output.false = OutputSignal("false", self)

    @staticmethod
    def if_(condition):
        if condition is NOT_DATA:
            raise TypeError(
                f"Logic 'If' node expected data other but got NOT_DATA as input."
            )
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


@function_node("slice")
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


@function_node("str")
def String(obj):
    return str(obj)


@function_node("bytes")
def Bytes(obj):
    return bytes(obj)


@function_node("lt")
def LessThan(obj, other):
    return obj < other


@function_node("le")
def LessThanEquals(obj, other):
    return obj <= other


@function_node("eq")
def Equals(obj, other):
    return obj == other


@function_node("neq")
def NotEquals(obj, other):
    return obj != other


@function_node("gt")
def GreaterThan(obj, other):
    return obj > other


@function_node("ge")
def GreaterThanEquals(obj, other):
    return obj >= other


@function_node("hash")
def Hash(obj):
    return hash(obj)


@function_node("bool")
def Bool(obj):
    return bool(obj)


@function_node("getattr")
def GetAttr(obj, name):
    return getattr(obj, name)


# These are not idempotent and thus not encouraged
# @function_node("none")
# def SetAttr(obj, name, value):
#     setattr(obj, name, value)
#     return None
#
#
# @function_node("none")
# def DelAttr(obj, name):
#     delattr(obj, name)
#     return None


@function_node("getitem")
def GetItem(obj, item):
    return obj[item]


@function_node("dir")
def Dir(obj):
    return dir(obj)


@function_node("len")
def Length(obj):
    return len(obj)


@function_node("in")
def Contains(obj, other):
    return other in obj


@function_node("add")
def Add(obj, other):
    return obj + other


@function_node("sub")
def Subtract(obj, other):
    return obj - other


@function_node("mul")
def Multiply(obj, other):
    return obj * other


@function_node("rmul")
def RightMultiply(obj, other):
    return other * obj


@function_node("matmul")
def MatrixMultiply(obj, other):
    return obj @ other


@function_node("truediv")
def Divide(obj, other):
    return obj / other


@function_node("floordiv")
def FloorDivide(obj, other):
    return obj // other


@function_node("mod")
def Modulo(obj, other):
    return obj % other


@function_node("pow")
def Power(obj, other):
    return obj**other


@function_node("and")
def And(obj, other):
    return obj & other


@function_node("xor")
def XOr(obj, other):
    return obj ^ other


@function_node("or")
def Or(obj, other):
    return obj ^ other


@function_node("neg")
def Negative(obj):
    return -obj


@function_node("pos")
def Positive(obj):
    return +obj


@function_node("abs")
def Absolute(obj):
    return abs(obj)


@function_node("invert")
def Invert(obj):
    return ~obj


@function_node("int")
def Int(obj):
    return int(obj)


@function_node("float")
def Float(obj):
    return float(obj)


@function_node("round")
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
    RightMultiply,
    Round,
    Slice,
    String,
    Subtract,
    UserInput,
    XOr,
]
