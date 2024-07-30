"""
Common-use nodes relying only on the standard library
"""

from __future__ import annotations

import random
import os
from pathlib import Path
import shutil
from time import sleep

from pyiron_workflow.channels import NOT_DATA, OutputSignal
from pyiron_workflow.nodes.function import Function, as_function_node


@as_function_node()
def UserInput(user_input):
    """
    Returns the user input as it is.

    Args:
        user_input: Any input provided by the user.

    Returns:
        The same input provided by the user.
    """
    return user_input


class If(Function):
    """
    Has two extra signal channels: true and false. Evaluates the input as a boolean and
    fires the corresponding output signal after running.

    Args:
        **kwargs: Additional keyword arguments for the Function base class.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.signals.output.true = OutputSignal("true", self)
        self.signals.output.false = OutputSignal("false", self)

    @staticmethod
    def node_function(condition):
        """
        Evaluate the condition as a boolean.

        Args:
            condition: The condition to evaluate.

        Returns:
            bool: The boolean value of the condition.

        Raises:
            TypeError: If the condition is NOT_DATA.
        """
        if condition is NOT_DATA:
            raise TypeError(f"Logic 'If' node expected data but got NOT_DATA as input.")
        truth = bool(condition)
        return truth

    @property
    def emitting_channels(self) -> tuple[OutputSignal]:
        if self.outputs.truth.value:
            return (*super().emitting_channels, self.signals.output.true)
        else:
            return (*super().emitting_channels, self.signals.output.false)


@as_function_node("list")
def AppendToList(existing: list | None = None, new_element=NOT_DATA):
    """
    Append a new element to a list.

    Args:
        existing (list | None): The list to append to. Defaults to None, which creates
            an empty list.
        new_element: The new element to append. This is mandatory input as the default
            value of `NOT_DATA` will cause a readiness error.

    Returns:
        list: The input list with the new element appended

    Examples:
        This node is particularly useful for acyclic flows, where you want to bend the
        suggestions of idempotency by looping the node's output back on itself:

        >>> from pyiron_workflow import standard_nodes as std
        >>>
        >>> n = std.AppendToList()
        >>> n.run(new_element="foo")
        ['foo']

        >>> n.run(existing=n, new_element="bar")
        ['foo', 'bar']

        >>> n.run(existing=n, new_element="baz")
        ['foo', 'bar', 'baz']

    """
    existing = [] if existing is None else existing
    if new_element is not NOT_DATA:
        existing.append(new_element)
    return existing


@as_function_node("started_at", "ended_at", "was_empty")
def ChangeDirectory(
    path: str | Path,
    delete_start: bool = False,
) -> tuple[str, str, bool]:
    """
    Change directory.

    Args:
        path (str | Path): Where to go.
        delete_start (bool): Whether to recursively remove the starting location. (Yes,
            this is dangerous, be careful -- but it's useful for going in and out of
            temp directories.)

    Returns:
        str: The absolute path to where we started.

    Examples:
        All the extra input and output allows use to go in and out of a location
        leaving no trace if we created it to start with.
        >>> import os
        >>>
        >>> from pyiron_workflow import standard_nodes as std
        >>>
        >>> go_in = std.ChangeDirectory(path="some_subdirectory")
        >>> in_result = go_in.run()
        >>> print(
        ...     os.getcwd().replace(
        ...         go_in.outputs.started_at.value, "some path..."
        ...     ).replace(
        ...          os.path.sep, "/"  # For doctesting on windows...
        ...     )
        ... )
        some path.../some_subdirectory

        >>> go_out = std.ChangeDirectory(
        ...     path=go_in.outputs.started_at,
        ...     delete_start=go_in.outputs.was_empty
        ... )
        >>> out_result = go_out.run();
        >>> print(os.getcwd() == go_in.outputs.started_at.value)
        True

        >>> print(os.path.isdir("some_subdirectory"))
        False

    """
    started_at = os.getcwd()
    if isinstance(path, Path):
        path = str(path.resolve())
    os.makedirs(path, exist_ok=True)
    was_empty = not any(os.scandir(path))
    os.chdir(path)
    if delete_start:
        shutil.rmtree(started_at)
    return started_at, path, was_empty


@as_function_node("as_int")
def Int(x):
    """
    Casts the input as an integer.

    This is not necessarily uniquely, but at least particularly useful as a wrapper because
    while many numpy types inherit from the root builtin type, `int64` (and related) do not.
    Since python3, python `int` is not even of a fixed value based on a c-type.
    Cf. https://github.com/numpy/numpy/issues/17283
    """
    return int(x)


@as_function_node()
def PureCall(fnc: callable):
    """
    Return a call without any arguments

    Args:
        fnc (callable): The callable object.

    Returns:
        An argument-free call on the callable object.

    Examples:
        This is particularly useful for snagging methods off objects inside a workflow.
        >>> import datetime
        >>>
        >>> from pyiron_workflow import standard_nodes as std
        >>>
        >>> inp = std.UserInput(datetime.date(1977, 5, 25))
        >>> pure_call = std.PureCall(inp.isoformat)
        >>> pure_call()
        '1977-05-25'

        Where we got the method to call using an injected `GetAttr` node.
    """
    return fnc()


@as_function_node("random")
def RandomFloat():
    """
    Generates a random float between 0 and 1.

    Returns:
        float: A random float between 0 and 1.
    """
    return random.random()


@as_function_node("time")
def Sleep(t):
    """
    Sleeps for the given number of seconds.

    Args:
        t (float): Number of seconds to sleep.

    Returns:
        float: The same number of seconds slept.
    """
    sleep(t)
    return t


@as_function_node("slice")
def Slice(start=None, stop=NOT_DATA, step=None):
    """
    Creates a slice object.

    Args:
        start (int | None): The start index. If None (Default), slicing starts from the
            beginning.
        stop (int | None): The stop index. If None or NOT_DATA (Default), slicing goes
            until the end.
        step (int | None): The step index. If None (Default), slicing proceeds with a
            step of 1.

    Returns:
        slice: The created slice object.

    Raises:
        ValueError: If the arguments are not valid for creating a slice.
    """
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


@as_function_node("object")
def SetAttr(obj, key: str, val):
    """
    Sets an attribute on an object.

    Args:
        obj: The object on which to set the attribute.
        key (str): The attribute name.
        val: The value to set for the attribute.

    Returns:
        The object with the attribute set.
    """
    setattr(obj, key, val)
    return obj


@as_function_node("str")
def String(obj):
    """
    Converts an object to its string representation.

    Args:
        obj: The object to convert.

    Returns:
        str: The string representation of the object.
    """
    return str(obj)


@as_function_node("bytes")
def Bytes(obj):
    """
    Converts an object to its bytes representation.

    Args:
        obj: The object to convert.

    Returns:
        bytes: The bytes representation of the object.
    """
    return bytes(obj)


@as_function_node("lt")
def LessThan(obj, other):
    """
    Compares if obj is less than other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is less than other, False otherwise.
    """
    return obj < other


@as_function_node("le")
def LessThanEquals(obj, other):
    """
    Compares if obj is less than or equal to other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is less than or equal to other, False otherwise.
    """
    return obj <= other


@as_function_node("eq")
def Equals(obj, other):
    """
    Compares if obj is equal to other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is equal to other, False otherwise.
    """
    return obj == other


@as_function_node("neq")
def NotEquals(obj, other):
    """
    Compares if obj is not equal to other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is not equal to other, False otherwise.
    """
    return obj != other


@as_function_node("gt")
def GreaterThan(obj, other):
    """
    Compares if obj is greater than other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is greater than other, False otherwise.
    """
    return obj > other


@as_function_node("ge")
def GreaterThanEquals(obj, other):
    """
    Compares if obj is greater than or equal to other.

    Args:
        obj: The first object to compare.
        other: The second object to compare.

    Returns:
        bool: True if obj is greater than or equal to other, False otherwise.
    """
    return obj >= other


@as_function_node("hash")
def Hash(obj):
    """
    Returns the hash value of an object.

    Args:
        obj: The object to hash.

    Returns:
        int: The hash value of the object.
    """
    return hash(obj)


@as_function_node("bool")
def Bool(obj):
    """
    Converts an object to its boolean representation.

    Args:
        obj: The object to convert.

    Returns:
        bool: The boolean representation of the object.
    """
    return bool(obj)


@as_function_node("getattr")
def GetAttr(obj, name):
    """
    Gets an attribute from an object.

    Args:
        obj: The object from which to get the attribute.
        name: The name of the attribute.

    Returns:
        The value of the attribute.
    """
    return getattr(obj, name)


@as_function_node("getitem")
def GetItem(obj, item):
    """
    Gets an item from an object.

    Args:
        obj: The object from which to get the item.
        item: The item to get.

    Returns:
        The value of the item.
    """
    return obj[item]


@as_function_node("dir")
def Dir(obj):
    """
    Returns a list of valid attributes for the object.

    Args:
        obj: The object to inspect.

    Returns:
        list: A list of valid attributes for the object.
    """
    return dir(obj)


@as_function_node("len")
def Length(obj):
    """
    Returns the length of an object.

    Args:
        obj: The object to measure.

    Returns:
        int: The length of the object.
    """
    return len(obj)


@as_function_node("in")
def Contains(obj, other):
    """
    Checks if obj contains other.

    Args:
        obj: The object to check.
        other: The item to check for.

    Returns:
        bool: True if obj contains other, False otherwise.
    """
    return other in obj


@as_function_node("add")
def Add(obj, other):
    """
    Adds obj and other.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of adding obj and other.
    """
    return obj + other


@as_function_node("sub")
def Subtract(obj, other):
    """
    Subtracts other from obj.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of subtracting other from obj.
    """
    return obj - other


@as_function_node("mul")
def Multiply(obj, other):
    """
    Multiplies obj by other.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of multiplying obj by other.
    """
    return obj * other


@as_function_node("rmul")
def RightMultiply(obj, other):
    """
    Multiplies other by obj (reversed operands).

    Args:
        obj: The second operand.
        other: The first operand.

    Returns:
        The result of multiplying other by obj.
    """
    return other * obj


@as_function_node("matmul")
def MatrixMultiply(obj, other):
    """
    Performs matrix multiplication on obj and other.

    Args:
        obj: The first matrix.
        other: The second matrix.

    Returns:
        The result of the matrix multiplication.
    """
    return obj @ other


@as_function_node("truediv")
def Divide(obj, other):
    """
    Divides obj by other.

    Args:
        obj: The numerator.
        other: The denominator.

    Returns:
        The result of dividing obj by other.
    """
    return obj / other


@as_function_node("floordiv")
def FloorDivide(obj, other):
    """
    Performs floor division on obj by other.

    Args:
        obj: The numerator.
        other: The denominator.

    Returns:
        The result of floor division.
    """
    return obj // other


@as_function_node("mod")
def Modulo(obj, other):
    """
    Calculates the modulo of obj by other.

    Args:
        obj: The numerator.
        other: The denominator.

    Returns:
        The result of obj modulo other.
    """
    return obj % other


@as_function_node("pow")
def Power(obj, other):
    """
    Raises obj to the power of other.

    Args:
        obj: The base.
        other: The exponent.

    Returns:
        The result of obj raised to the power of other.
    """
    return obj**other


@as_function_node("and")
def And(obj, other):
    """
    Performs a bitwise AND operation on obj and other.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of the bitwise AND operation.
    """
    return obj & other


@as_function_node("xor")
def XOr(obj, other):
    """
    Performs a bitwise XOR operation on obj and other.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of the bitwise XOR operation.
    """
    return obj ^ other


@as_function_node("or")
def Or(obj, other):
    """
    Performs a bitwise OR operation on obj and other.

    Args:
        obj: The first operand.
        other: The second operand.

    Returns:
        The result of the bitwise OR operation.
    """
    return obj | other


@as_function_node("neg")
def Negative(obj):
    """
    Negates an object.

    Args:
        obj: The object to negate.

    Returns:
        The negated object.
    """
    return -obj


@as_function_node("pos")
def Positive(obj):
    """
    Returns the positive of an object.

    Args:
        obj: The object.

    Returns:
        The positive of the object.
    """
    return +obj


@as_function_node("abs")
def Absolute(obj):
    """
    Returns the absolute value of an object.

    Args:
        obj: The object.

    Returns:
        The absolute value of the object.
    """
    return abs(obj)


@as_function_node("invert")
def Invert(obj):
    """
    Inverts the bits of an object.

    Args:
        obj: The object.

    Returns:
        The inverted object.
    """
    return ~obj


@as_function_node("int")
def Int(obj):
    """
    Converts an object to an integer.

    Args:
        obj: The object to convert.

    Returns:
        int: The integer representation of the object.
    """
    return int(obj)


@as_function_node("float")
def Float(obj):
    """
    Converts an object to a float.

    Args:
        obj: The object to convert.

    Returns:
        float: The float representation of the object.
    """
    return float(obj)


@as_function_node("round")
def Round(obj):
    """
    Rounds a number to the nearest integer.

    Args:
        obj: The number to round.

    Returns:
        int: The rounded number.
    """
    return round(obj)


nodes = [
    Absolute,
    Add,
    And,
    AppendToList,
    Bool,
    Bytes,
    ChangeDirectory,
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
    PureCall,
    RandomFloat,
    RightMultiply,
    Round,
    SetAttr,
    Sleep,
    Slice,
    String,
    Subtract,
    UserInput,
    XOr,
]
