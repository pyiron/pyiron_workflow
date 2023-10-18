"""
Channels are access points for information to flow into and out of nodes.

Data channels carry, unsurprisingly, data.
Connections are only permissible between opposite sub-types, i.e. input-output.
When input channels `fetch()` data in, they set their `value` to the first available
data value among their connections -- i.e. the `value` of the first output channel in
their connections who has something other than `NotData`.
Input data channels will raise an error if a `fetch()` is attempted while their parent
 node is running.

Signal channels are tools for procedurally exposing functionality on nodes.
Input signal channels are connected to a callback function which gets invoked when the
channel is called.
Output signal channels call all the input channels they are connected to when they get
 called themselves.
In this way, signal channels can force behaviour (node method calls) to propagate
forwards through a graph.
They do not hold any data and have no `value` attribute, but rather fire for an effect.
"""

from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from warnings import warn

from pyiron_workflow.has_channel import HasChannel
from pyiron_workflow.has_to_dict import HasToDict
from pyiron_workflow.type_hinting import (
    valid_value,
    type_hint_is_as_or_more_specific_than,
)

if typing.TYPE_CHECKING:
    from pyiron_workflow.node import Node


class ChannelConnectionError(Exception):
    pass


class Channel(HasChannel, HasToDict, ABC):
    """
    Channels facilitate the flow of information (data or control signals) into and
    out of nodes.
    They must have a label and belong to a node.

    Input/output channels can be (dis)connected from other output/input channels of the
    same generic type (i.e. data or signal), and store all of their current connections
    in a list.
    This connection information is reflexive, and is duplicated to be stored on _both_
    channels in the form of a reference to their counterpart in the connection.

    Child classes must define a string representation, `__str__`, and their
    `generic_type` which is a parent of both themselves and their output/input partner.

    Attributes:
        label (str): The name of the channel.
        node (pyiron_workflow.node.Node): The node to which the channel
         belongs.
        connections (list[Channel]): Other channels to which this channel is connected.
    """

    def __init__(
        self,
        label: str,
        node: Node,
    ):
        """
        Make a new channel.

        Args:
            label (str): A name for the channel.
            node (pyiron_workflow.node.Node): The node to which the
             channel belongs.
        """
        self.label: str = label
        self.node: Node = node
        self.connections: list[Channel] = []

    @abstractmethod
    def __str__(self):
        pass

    @property
    @abstractmethod
    def generic_type(self) -> type[Channel]:
        """Input and output class pairs should share this parent class"""

    def _valid_connection(self, other: Channel) -> bool:
        """
        Logic for determining if a connection is valid.

        Connections should have the same generic type, but not the same type -- i.e.
        they should be an input/output pair of some connection type.
        """
        return isinstance(other, self.generic_type) and not isinstance(
            other, self.__class__
        )

    def connect(self, *others: Channel) -> None:
        """
        Form a connection between this and one or more other channels.
        Connections are reflexive, and must occur between input and output channels of
        the same `generic_type` (i.e. data or signal).


        Args:
            *others (Channel): The other channel objects to attempt to connect with.

        Raises:
            (ChannelConnectionError): If the other channel is of the correct generic
                type, but nonetheless not a valid connection.
            (TypeError): If the other channel is not an instance of this channel's
                generic type.
        """
        for other in others:
            if other in self.connections:
                continue
            elif self._valid_connection(other):
                self.connections.append(other)
                other.connections.append(self)
            else:
                if isinstance(other, self.generic_type):
                    raise ChannelConnectionError(
                        f"{self.label} ({self.__class__.__name__}) and {other.label} "
                        f"({other.__class__.__name__}) share a generic type but were "
                        f"not a valid connection. Check channel classes, type hints, "
                        f"etc."
                    )
                else:
                    raise TypeError(
                        f"Can only connect two {self.generic_type.__name__} objects, "
                        f"but {self.label} ({self.__class__.__name__}) got {other} "
                        f"({type(other)})"
                    )

    def disconnect(self, *others: Channel) -> list[tuple[Channel, Channel]]:
        """
        If currently connected to any others, removes this and the other from eachothers
        respective connections lists.

        Args:
            *others (Channel): The other channels to disconnect from.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        destroyed_connections = []
        for other in others:
            if other in self.connections:
                self.connections.remove(other)
                other.disconnect(self)
                destroyed_connections.append((self, other))
            else:
                warn(
                    f"The channel {self.label} was not connected to {other.label}, and"
                    f"thus could not disconnect from it."
                )
        return destroyed_connections

    def disconnect_all(self) -> list[tuple[Channel, Channel]]:
        """
        Disconnect from all other channels currently in the connections list.
        """
        return self.disconnect(*self.connections)

    @property
    def connected(self) -> bool:
        """
        Has at least one connection.
        """
        return len(self.connections) > 0

    def __iter__(self):
        return self.connections.__iter__()

    def __len__(self):
        return len(self.connections)

    @property
    def channel(self) -> Channel:
        return self

    def copy_connections(self, other: Channel) -> None:
        """
        Adds all the connections in another channel to this channel's connections.

        If an exception is encountered, all the new connections are disconnected before
        the exception is raised.
        """
        new_connections = []
        try:
            for connect_to in other.connections:
                # We do them one at a time in case any fail, so we can undo those that
                # worked
                self.connect(connect_to)
                new_connections.append(connect_to)
        except Exception as e:
            self.disconnect(*new_connections)
            raise e

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "connected": self.connected,
            "connections": [f"{c.node.label}.{c.label}" for c in self.connections],
        }


class NotData:
    """
    This class exists purely to initialize data channel values where no default value
    is provided; it lets the channel know that it has _no data in it_ and thus should
    not identify as ready.
    """

    @classmethod
    def __repr__(cls):
        # We use the class directly (not instances of it) where there is not yet data
        # So give it a decent repr, even as just a class
        return cls.__name__


class DataChannel(Channel, ABC):
    """
    Data channels control the flow of data on the graph.
    They store this data in a `value` attribute.
    They may optionally have a type hint.
    They have a `ready` attribute which tells whether their value matches their type
    hint (if one is provided, else `True`).
    (In the future they may optionally have a storage priority.)
    (In the future they may optionally have a storage history limit.)
    (In the future they may optionally have an ontological type.)

    Note that for the sake of computational efficiency, assignments to the `value`
    property are not type-checked; type-checking occurs only for connections where both
    channels have a type hint, and when a value is being copied from another channel
    with the `copy_value` method.

    When type checking channel connections, we insist that the output type hint be
    _as or more specific_ than the input type hint, to ensure that the input always
    receives output of a type it expects. This behaviour can be disabled and all
    connections allowed by setting `strict_connections = False` on the relevant input
    channel.

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

    The data `value` will initialize to an instance of `NotData` by default.
    The channel will identify as `ready` when the value is _not_ an instance of
    `NotData`, and when the value conforms to type hints (if any).

    Warning:
        Type hinting in python is quite complex, and determining when a hint is
        "more specific" can be tricky. For instance, in python 3.11 you can now type
        hint a tuple with a mixture of fixed elements of fixed type, followed by an
        arbitrary elements of arbitrary type. This and other complex scenarios are not
        yet included in our test suite and behaviour is not guaranteed.
    """

    def __init__(
        self,
        label: str,
        node: Node,
        default: typing.Optional[typing.Any] = NotData,
        type_hint: typing.Optional[typing.Any] = None,
    ):
        super().__init__(label=label, node=node)
        self.default = default
        self.value = default
        self.type_hint = type_hint

    @property
    def generic_type(self) -> type[Channel]:
        return DataChannel

    @property
    def ready(self) -> bool:
        """
        Check if the currently stored value satisfies the channel's type hint.

        Returns:
            (bool): Whether the value matches the type hint.
        """
        if self.type_hint is not None:
            return self._value_is_data and valid_value(self.value, self.type_hint)
        else:
            return self._value_is_data

    @property
    def _value_is_data(self):
        return self.value is not NotData

    @property
    def _has_hint(self):
        return self.type_hint is not None

    def _valid_connection(self, other) -> bool:
        if super()._valid_connection(other):
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

    def _both_typed(self, other: DataChannel) -> bool:
        return self._has_hint and other._has_hint

    def _figure_out_who_is_who(self, other: DataChannel) -> (OutputData, InputData):
        return (self, other) if isinstance(self, OutputData) else (other, self)

    def __str__(self):
        return str(self.value)

    def copy_value(self, other: DataChannel) -> None:
        """
        Copies the other channel's value. Unlike normal value assignment, the new value
        (if it is data) must comply with this channel's type hint (if any).
        """
        if (
            self._has_hint
            and other._value_is_data
            and not valid_value(other.value, self.type_hint)
        ):
            raise TypeError(
                f"Channel{self.label} cannot copy value from {other.label} because "
                f"value {other.value} does not match type hint {self.type_hint}"
            )
        self.value = other.value

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["value"] = repr(self.value)
        d["ready"] = self.ready
        return d


class InputData(DataChannel):
    """
    `fetch()` updates input data value to the first available data among connections.

    The `strict_connections` parameter controls whether connections are subject to
    type checking requirements.
    I.e., they may set `strict_connections` to `False` (`True` -- default) at
    instantiation or later with `(de)activate_strict_connections()` to prevent (enable)
    data type checking when making connections with `OutputData` channels.
    """

    def __init__(
        self,
        label: str,
        node: Node,
        default: typing.Optional[typing.Any] = NotData,
        type_hint: typing.Optional[typing.Any] = None,
        strict_connections: bool = True,
    ):
        super().__init__(
            label=label,
            node=node,
            default=default,
            type_hint=type_hint,
        )
        self.strict_connections = strict_connections

    def fetch(self) -> None:
        """
        Sets `value` to the first value among connections that is something other than
        `NotData`; if no such value exists (e.g. because there are no connections or
        because all the connected output channels have `NotData` as their value),
        `value` remains unchanged.

        Raises:
            RuntimeError: If the parent node is `running`.
        """
        if self.node.running:
            raise RuntimeError(
                f"Parent node {self.node.label} of {self.label} is running, so value "
                f"cannot be updated."
            )
        for out in self.connections:
            if out.value is not NotData:
                self.value = out.value
                break

    def activate_strict_connections(self) -> None:
        self.strict_connections = True

    def deactivate_strict_connections(self) -> None:
        self.strict_connections = False


class OutputData(DataChannel):
    pass


class SignalChannel(Channel, ABC):
    """
    Signal channels give the option control execution flow by triggering callback
    functions.

    Output channels can be called to trigger the callback functions of all input
    channels to which they are connected.

    Signal channels support `>` as syntactic sugar for their connections, i.e.
    `some_output > some_input` is equivalent to `some_input.connect(some_output)`.
    (This is also interoperable with `Node` objects, cf. the `Node` docs.)
    """

    @abstractmethod
    def __call__(self) -> None:
        pass

    @property
    def generic_type(self) -> type[Channel]:
        return SignalChannel

    def connect_output_signal(self, signal: OutputSignal):
        self.connect(signal)


class InputSignal(SignalChannel):
    """
    Invokes a callback when called.
    """

    def __init__(
        self,
        label: str,
        node: Node,
        callback: callable,
    ):
        """
        Make a new input signal channel.

        Args:
            label (str): A name for the channel.
            node (pyiron_workflow.node.Node): The node to which the
             channel belongs.
            callback (callable): An argument-free callback to invoke when calling this
                object.
        """
        super().__init__(label=label, node=node)
        self.callback: callable = callback

    def __call__(self) -> None:
        self.callback()

    def __str__(self):
        return f"{self.label} runs {self.callback.__name__}"

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["callback"] = self.callback.__name__
        return d


class OutputSignal(SignalChannel):
    """
    Calls all the input signal objects in its connections list when called.
    """

    def __call__(self) -> None:
        for c in self.connections:
            c()

    def __str__(self):
        return (
            f"{self.label} activates "
            f"{[f'{c.node.label}.{c.label}' for c in self.connections]}"
        )

    def __gt__(self, other: InputSignal | Node):
        other.connect_output_signal(self)
        return True
