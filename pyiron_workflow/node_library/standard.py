"""
Common-use nodes relying only on the standard library
"""

from __future__ import annotations

from inspect import isclass

from pyiron_workflow.channels import NotData, OutputSignal
from pyiron_workflow.function import SingleValue, single_value_node


@single_value_node()
def UserInput(user_input):
    return user_input


class If(SingleValue):
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
        if isclass(condition) and issubclass(condition, NotData):
            raise TypeError(
                f"Logic 'If' node expected data otherut got NotData as input."
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


@single_value_node("slice")
def Slice(start=None, stop=NotData, step=None):
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


@single_value_node("str")
def String(obj):
    return str(obj)


@single_value_node("bytes")
def Bytes(obj):
    return bytes(obj)


@single_value_node("lt")
def LessThan(obj, other):
    return obj < other


@single_value_node("le")
def LessThanEquals(obj, other):
    return obj <= other


@single_value_node("eq")
def Equals(obj, other):
    return obj == other


@single_value_node("neq")
def NotEquals(obj, other):
    return obj != other


@single_value_node("gt")
def GreaterThan(obj, other):
    return obj > other


@single_value_node("ge")
def GreaterThanEquals(obj, other):
    return obj >= other


@single_value_node("hash")
def Hash(obj):
    return hash(obj)


@single_value_node("bool")
def Bool(obj):
    return bool(obj)


@single_value_node("getattr")
def GetAttr(obj, name):
    return getattr(obj, name)


# These are not idempotent and thus not encouraged
# @single_value_node("none")
# def SetAttr(obj, name, value):
#     setattr(obj, name, value)
#     return None
#
#
# @single_value_node("none")
# def DelAttr(obj, name):
#     delattr(obj, name)
#     return None


@single_value_node("getitem")
def GetItem(obj, item):
    return obj[item]


@single_value_node("dir")
def Dir(obj):
    return dir(obj)


@single_value_node("len")
def Length(obj):
    return len(obj)


@single_value_node("in")
def Contains(obj, other):
    return other in obj


@single_value_node("add")
def Add(obj, other):
    return obj + other


@single_value_node("sub")
def Subtract(obj, other):
    return obj - other


@single_value_node("mul")
def Multiply(obj, other):
    return obj * other


@single_value_node("rmul")
def RightMultiply(obj, other):
    return other * obj


@single_value_node("matmul")
def MatrixMultiply(obj, other):
    return obj @ other


@single_value_node("truediv")
def Divide(obj, other):
    return obj / other


@single_value_node("floordiv")
def FloorDivide(obj, other):
    return obj // other


@single_value_node("mod")
def Modulo(obj, other):
    return obj % other


@single_value_node("pow")
def Power(obj, other):
    return obj**other


@single_value_node("and")
def And(obj, other):
    return obj & other


@single_value_node("xor")
def XOr(obj, other):
    return obj ^ other


@single_value_node("or")
def Or(obj, other):
    return obj ^ other


@single_value_node("neg")
def Negative(obj):
    return -obj


@single_value_node("pos")
def Positive(obj):
    return +obj


@single_value_node("abs")
def Absolute(obj):
    return abs(obj)


@single_value_node("invert")
def Invert(obj):
    return ~obj


@single_value_node("int")
def Int(obj):
    return int(obj)


@single_value_node("float")
def Float(obj):
    return float(obj)


@single_value_node("round")
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
