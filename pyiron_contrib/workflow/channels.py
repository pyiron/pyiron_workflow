from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from json import dumps
from warnings import warn

from pyiron_contrib.workflow.has_channel import HasChannel
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.type_hinting import (
    valid_value, type_hint_is_as_or_more_specific_than
)

if typing.TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class Channel(HasChannel, HasToDict, ABC):
    """
    Channels facilitate the flow of information (data or control signals) into and
    out of nodes.
    They have a label and belong to a node.

    Input/output channels can be (dis)connected from other output/input channels, and
    store all of their current connections in a list.
    """

    def __init__(
            self,
            label: str,
            node: Node,
    ):
        self.label = label
        self.node = node
        self.connections = []

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def connect(self, *others: Channel):
        pass

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

    def _already_connected(self, other: Channel):
        return other in self.connections

    def __iter__(self):
        return self.connections.__iter__()

    def __len__(self):
        return len(self.connections)

    @property
    def channel(self) -> Channel:
        return self

    def to_dict(self):
        return {
            "label": self.label,
            "connected": self.connected,
            "connections": [f"{c.node.label}.{c.label}" for c in self.connections]
        }


class DataChannel(Channel, ABC):
    """
    Data channels control the flow of data on the graph.
    They store this data in a `value` attribute.
    They may optionally have a type hint.
    They have a `ready` attribute which tells whether their value matches their type
    hint.
    They may optionally have a storage priority (but this doesn't do anything yet).
    (In the future they may optionally have an ontological type.)

    The `value` held by a channel can be manually assigned, but should normally be set
    by the `update` method.
    In neither case is the type hint strictly enforced.
    Input channels will then propagate their value along to their owning node.
    Output channels with then propagate their value to all the input channels they're
    connected to.

    Type hinting is strictly enforced in one situation: when making connections to
    other channels and at least one data channel has a non-None value for its type hint.
    In this case, we insist that the output type hint be _as or more more specific_ than
    the input type hint, to ensure that the input always receives output of a type it
    expects. This behaviour can be disabled and all connections allowed by setting
    `strict_connections = False` on the relevant input channel.

    For simple type hints like `int` or `str`, type hint comparison is trivial.
    However, some hints take arguments, e.g. `dict[str, int]` to specify key and value
    types; `tuple[int, int, str]` to specify a tuple with certain values;
    `typing.Literal['a', 'b', 'c']` to specify particular choices;
    `typing.Callable[[float, float], str]` to specify a callable that takes particular
    argument types and has a return type; etc.
    For hints with the origin `dict`, `tuple`, and `typing.Callable`, the two hints must
    have _exactly the same arguments_ for one two qualify as "as or more specific".
    E.g. `tuple[int, int|float]` is as or more specific than
    `tuple[int|float, int|float]`, but not `tuple[int, int|float, str]`.
    For _all other hints_, we demand that the output hint arguments be a _subset_ of
    the input.
    E.g. `Literal[1, 2]` is as or more specific that both `Literal[1, 2]` and
    `Literal[1, 2, "three"]`.

    Warning:
        Type hinting in python is quite complex, and determining when a hint is
        "more specific" can be tricky. For instance, in python 3.11 you can now type
        hint a tuple with a mixture of fixed elements of fixed type, followed by an
        arbitrary elements of arbitrary type. This and other complex scenarios are not
        yet included in our test suite and behaviour is not guaranteed.

    TODO:
        In direct relation to the above warning, it may be nice to add a flag to
        channels to turn on/off the strict enforcement of type hints when making
        connections.
    """
    def __init__(
            self,
            label: str,
            node: Node,
            default: typing.Optional[typing.Any] = None,
            type_hint: typing.Optional[typing.Any] = None,
            storage_priority: int = 0,
            strict_connections: bool = True,
    ):
        super().__init__(label=label, node=node)
        self.default = default
        self.value = default
        self.type_hint = type_hint
        self.storage_priority = storage_priority
        self.strict_connections = strict_connections
        self._waiting_for_update = False

    @property
    def ready(self):
        if self.type_hint is not None:
            return not self.waiting_for_update and valid_value(
                self.value, self.type_hint
            )
        else:
            return not self.waiting_for_update

    @property
    def waiting_for_update(self):
        return self._waiting_for_update

    def wait_for_update(self):
        self._waiting_for_update = True

    def update(self, value):
        self._waiting_for_update = False
        self.value = value
        self._after_update()

    def _after_update(self):
        pass

    def require_update_after_node_runs(self, wait_now=False):
        if self.label not in self.node.channels_requiring_update_after_run:
            self.node.channels_requiring_update_after_run.append(self.label)
        if wait_now:
            self.wait_for_update()

    def connect(self, *others: DataChannel):
        for other in others:
            if self._valid_connection(other):
                self.connections.append(other)
                other.connections.append(self)
                out, inp = self._figure_out_who_is_who(other)
                inp.update(out.value)
            else:
                if isinstance(other, DataChannel):
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
                out, inp = self._figure_out_who_is_who(other)
                if not inp.strict_connections:
                    return True
                else:
                    return type_hint_is_as_or_more_specific_than(
                        out.type_hint, inp.type_hint
                    )
            else:
                # If either is untyped, don't do type checking
                return True
        else:
            return False

    def _is_IO_pair(self, other: DataChannel):
        return isinstance(other, DataChannel) and not isinstance(other, self.__class__)

    def _both_typed(self, other: DataChannel):
        return self.type_hint is not None and other.type_hint is not None

    def _figure_out_who_is_who(self, other: DataChannel) -> (OutputData, InputData):
        return (self, other) if isinstance(self, OutputData) else (other, self)

    def __str__(self):
        return str(self.value)

    def to_dict(self):
        d = super().to_dict()
        d["value"] = repr(self.value)
        d["ready"] = self.ready
        return d


class InputData(DataChannel):
    def _after_update(self):
        self.node.update()

    def activate_strict_connections(self):
        self.strict_connections = True

    def deactivate_strict_connections(self):
        self.strict_connections = False


class OutputData(DataChannel):
    def _after_update(self):
        for inp in self.connections:
            inp.update(self.value)


class SignalChannel(Channel, ABC):
    """
    Signal channels give the option control execution flow by triggering callback
    functions.

    Output channels can be called to trigger the callback functions of all input
    channels to which they are connected.
    """

    @abstractmethod
    def __call__(self):
        pass

    def connect(self, *others: Channel):
        for other in others:
            if self._valid_connection(other):
                self.connections.append(other)
                other.connections.append(self)
            else:
                if isinstance(other, SignalChannel):
                    warn(
                        f"{self.label} ({self.__class__.__name__}) and {other.label} "
                        f"({other.__class__.__name__}) were not a valid connection"
                    )
                else:
                    raise TypeError(
                        f"Can only connect two signal channels, but {self.label} "
                        f"({self.__class__.__name__}) got a {other} ({type(other)})"
                    )

    def _valid_connection(self, other) -> bool:
        return self._is_IO_pair(other) and not self._already_connected(other)

    def _is_IO_pair(self, other) -> bool:
        return isinstance(other, SignalChannel) \
            and not isinstance(other, self.__class__)


class InputSignal(SignalChannel):
    def __init__(
            self,
            label: str,
            node: Node,
            callback: callable,
    ):
        super().__init__(label=label, node=node)
        self.callback: callable = callback

    def __call__(self):
        self.callback()

    def __str__(self):
        return f"{self.label} runs {self.callback.__name__}"

    def to_dict(self):
        d = super().to_dict()
        d["callback"] = self.callback.__name__
        return d


class OutputSignal(SignalChannel):
    def __call__(self):
        for c in self.connections:
            c()

    def __str__(self):
        return f"{self.label} activates " \
               f"{[f'{c.node.label}.{c.label}' for c in self.connections]}"
