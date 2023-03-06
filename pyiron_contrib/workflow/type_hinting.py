import types
import typing
from collections.abc import Callable

from typeguard import check_type


def valid_value(value, type_hint) -> bool:
    try:
        return isinstance(value, type_hint)
    except TypeError:
        # Subscripted generics cannot be used with class and instance checks
        try:
            # typeguard handles this case
            check_type("", value, type_hint)
            return True
        except TypeError:
            # typeguard raises an error on a failed check
            return False


def type_hint_to_tuple(type_hint) -> tuple:
    if isinstance(type_hint, (types.UnionType, typing._UnionGenericAlias)):
        return typing.get_args(type_hint)
    else:
        return type_hint,


def type_hint_is_as_or_more_specific_than(hint, other) -> bool:
    hint_origin = typing.get_origin(hint)
    other_origin = typing.get_origin(other)
    if set([hint_origin, other_origin]) & set([types.UnionType, typing.Union]):
        # If either hint is a union, turn both into tuples and call recursively
        return all(
            [
                any(
                    [
                        type_hint_is_as_or_more_specific_than(h, o)
                        for o in type_hint_to_tuple(other)
                    ]
                )
                for h in type_hint_to_tuple(hint)
            ]
        )
    elif hint_origin is None and other_origin is None:
        # Once both are raw classes, just do a subclass test
        try:
            return issubclass(hint, other)
        except TypeError:
            return hint == other
    elif hint_origin == other_origin:
        hint_args = typing.get_args(hint)
        other_args = typing.get_args(other)
        if len(hint_args) == 0 and len(other_args) > 0:
            # Failing to specify anything is not being more specific
            return False
        elif hint_origin in [dict, tuple, Callable]:
            # If order matters, make sure the arguments match 1:1
            return all(
                [
                    type_hint_is_as_or_more_specific_than(h, o)
                    for o, h in zip(other_args, hint_args)
                ]
            )
        else:
            # Otherwise just make sure the arguments are a subset
            return all(
                [
                    any(
                        [
                            type_hint_is_as_or_more_specific_than(h, o)
                            for o in other_args
                        ]
                    )
                    for h in hint_args
                ]
            )
    else:
        # Otherwise they both have origins, but different ones
        return False