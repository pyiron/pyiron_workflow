from __future__ import annotations

import types
import typing
from abc import ABC
from collections.abc import Callable
from warnings import warn

from typeguard import check_type

if typing.TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class Channel(ABC):
    def __init__(
            self,
            label: str,
            node: Node,
            default: typing.Optional[typing.Any] = None,
            types: typing.Optional[tuple | type[typing.Any]] = None,
            storage_priority: int = 0,
    ):
        self.label = label
        self.node = node
        self.default = default
        self.value = default
        self.types = types
        self.storage_priority = storage_priority
        self.connections = []

    @property
    def ready(self):
        if self.types is not None:
            return self._valid_value(self.value, self.types)
        else:
            return True

    @staticmethod
    def _valid_value(value, type_hint):
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

    def connect(self, *others: Channel):
        for other in others:
            if self._valid_connection(other):
                self.connections.append(other)
                other.connections.append(self)
            else:
                if isinstance(other, Channel):
                    warn(
                        f"{self.label} ({self.__class__.__name__}) and {other.label} "
                        f"({other.__class__.__name__}) were not a valid connection"
                    )
                else:
                    raise TypeError(
                        f"Can only connect two channels, but {self.label} "
                        f"({self.__class__.__name__}) got a {other} ({type(other)})"
                    )

    def _valid_connection(self, other):
        if self._is_IO_pair(other) and not self._already_connected(other):
            if self._both_typed(other):
                return self._output_types_are_subset_of_input_types(other)
            else:
                return True
        else:
            return False

    def _is_IO_pair(self, other: Channel):
        return isinstance(other, Channel) and type(self) != type(other)

    def _already_connected(self, other: Channel):
        return other in self.connections

    def _both_typed(self, other: Channel):
        return self.types is not None and other.types is not None

    def _output_types_are_subset_of_input_types(self, other: Channel):
        out, inp = self._figure_out_who_is_who(other)
        return self._hint_is_as_or_more_specific_than(out.types, inp.types)

    def _figure_out_who_is_who(self, other: Channel) -> (OutputChannel, InputChannel):
        return (self, other) if isinstance(self, OutputChannel) else (other, self)

    @classmethod
    def _hint_is_as_or_more_specific_than(cls, hint, other):
        hint_origin = typing.get_origin(hint)
        other_origin = typing.get_origin(other)
        if set([hint_origin, other_origin]) & set([types.UnionType, typing.Union]):
            # If either hint is a union, turn both into tuples and call recursively
            return all(
                [
                    any(
                        [
                            cls._hint_is_as_or_more_specific_than(h, o)
                            for o in cls._hint_to_tuple(other)
                        ]
                    )
                    for h in cls._hint_to_tuple(hint)
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
                        cls._hint_is_as_or_more_specific_than(h, o)
                        for o, h in zip(other_args, hint_args)
                    ]
                )
            else:
                # Otherwise just make sure the arguments are a subset
                return all(
                    [
                        any(
                            [
                                cls._hint_is_as_or_more_specific_than(h, o)
                                for o in other_args
                            ]
                        )
                        for h in hint_args
                    ]
                )
        else:
            # Otherwise they both have origins, but different ones
            return False

    @staticmethod
    def _hint_to_tuple(type_hint):
        if isinstance(type_hint, (types.UnionType, typing._UnionGenericAlias)):
            return typing.get_args(type_hint)
        else:
            return (type_hint,)

    def disconnect(self, *others: Channel):
        for other in others:
            if other in self.connections:
                self.connections.remove(other)
                other.disconnect(self)

    def disconnect_all(self):
        self.disconnect(*self.connections)

    @property
    def connected(self):
        return len(self.connections) > 0

    def __iter__(self):
        return self.connections.__iter__()

    def __len__(self):
        return len(self.connections)


class InputChannel(Channel):
    def update(self, value):
        self.value = value
        self.node.update()


class OutputChannel(Channel):
    def update(self, value):
        self.value = value
        for inp in self.connections:
            inp.update(self.value)
