"""
This module provides helper functions for evaluating data relative to type hints, and
type hints relative to each other.
"""

import types
import typing
from collections.abc import Callable

from pint import Quantity
from typeguard import TypeCheckError, check_type


def valid_value(value, type_hint, strict_callables: bool = True) -> bool:
    """
    Check if a value is a valid representation of a type hint.

    Args:
        value: The value to verify.
        type_hint: The type hint against which
        strict_callables (bool): Whether to convert `callable` hints into
            `collections.abc.Callable`. This can be important as the interaction of
            `callable` and `check_type` is very relaxed, e.g. a class that fails to
            define `__call__` will still pass as `callable`. Converting to the formal
            typing hint resolves this and gives more intuitive results. Default is True.

    Returns:
        (bool): Whether the value conforms to the hint.
    """
    value = value.magnitude if isinstance(value, Quantity) else value  # De-unit it
    type_hint = Callable if strict_callables and type_hint is callable else type_hint

    try:
        return isinstance(value, type_hint)
    except TypeError:
        # Subscripted generics cannot be used with class and instance checks
        try:
            # typeguard handles this case
            check_type(value, type_hint)
            return True
        except TypeCheckError:
            # typeguard raises an error on a failed check
            return False


def type_hint_to_tuple(type_hint) -> tuple:
    if isinstance(type_hint, types.UnionType):
        return typing.get_args(type_hint)
    return (type_hint,)


def _get_type_hints(type_hint) -> tuple[type | None, typing.Any]:
    hint = typing.get_origin(type_hint)
    if hint is typing.Annotated:
        return typing.get_origin(type_hint.__origin__), type_hint.__origin__
    else:
        return hint, type_hint


def type_hint_is_as_or_more_specific_than(hint, other) -> bool:
    hint_origin, hint_type = _get_type_hints(hint)
    other_origin, other_type = _get_type_hints(other)
    if {hint_origin, other_origin} & {types.UnionType, typing.Union}:
        # If either hint is a union, turn both into tuples and call recursively
        return all(
            any(
                type_hint_is_as_or_more_specific_than(h, o)
                for o in type_hint_to_tuple(other_type)
            )
            for h in type_hint_to_tuple(hint_type)
        )
    elif hint_origin is None and other_origin is None:
        # Once both are raw classes, just do a subclass test
        try:
            return issubclass(hint_type, other_type)
        except TypeError:
            return hint_type == other_type
    elif other_origin is None and hint_origin is not None:
        # When the hint adds specificity to an empty origin
        return hint_origin == other_type
    elif hint_origin == other_origin:
        # If they both have an origin, break into arguments and treat cases
        hint_args = typing.get_args(hint_type)
        other_args = typing.get_args(other_type)
        if len(hint_args) == 0 and len(other_args) > 0:
            # Failing to specify anything is not being more specific
            return False
        elif hint_origin in [dict, tuple, Callable]:
            # for these origins the order of arguments matters
            if len(other_args) == 0:
                # If the other doesn't specify _any_ arguments, we must be more specific
                return True
            elif len(other_args) == len(hint_args):
                # If they both specify arguments, they should be more specific 1:1
                return all(
                    type_hint_is_as_or_more_specific_than(h, o)
                    for o, h in zip(other_args, hint_args, strict=False)
                )
            else:
                # Otherwise they both specify but a mis-matching number of args
                return False
        else:
            # Otherwise order doesn't matter so make sure the arguments are a subset
            return all(
                any(type_hint_is_as_or_more_specific_than(h, o) for o in other_args)
                for h in hint_args
            )
    else:
        # Lastly, if they both have origins, but different ones, fail
        return False
