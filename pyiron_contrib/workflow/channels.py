"""
Channels are access points for information to flow into and out of nodes.

Data channels carry, unsurprisingly, data.
Input data channels force an update on their owning node when they are updated with new
data, and output data channels update all the input data channels to which they are
connected.
In this way, data channels facilitate forward propagation of data through a graph.
They hold data persistently.

Signal channels are tools for procedurally exposing functionality on nodes.
Input signal channels are connected to a callback function which gets invoked when the
channel is updated.
Output signal channels must be accessed by the owning node directly, and then trigger
all the input signal channels to which they are connected.
In this way, signal channels can force behaviour (node method calls) to propagate
forwards through a graph.
They do not hold any data, but rather fire for an effect.
"""

from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from warnings import warn

from pyiron_contrib.workflow.has_channel import HasChannel
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.type_hinting import (
    valid_value,
    type_hint_is_as_or_more_specific_than,
)

if typing.TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class Channel(HasChannel, HasToDict, ABC):
    """
    Channels facilitate the flow of information (data or control signals) into and
    out of nodes.
    They must have a label and belong to a node.

    Input/output channels can be (dis)connected from other output/input channels, and
    store all of their current connections in a list.
    This connection information is duplicated in that it is stored on _both_ channels
    that form the connection.

    Child classes must define a string representation, `__str__`, and what to do on an
    attempted connection, `connect`.

    Attributes:
        label (str): The name of the channel.
        node (pyiron_contrib.workflow.node.Node): The node to which the channel
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
            node (pyiron_contrib.workflow.node.Node): The node to which the
             channel belongs.
        """
        self.label: str = label
        self.node: Node = node
        self.connections: list[Channel] = []

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def connect(self, *others: Channel) -> None:
        """
        How to handle connections to other channels.

        Args:
            *others (Channel): The other channel objects to attempt to connect with.
        """
        pass

    def disconnect(self, *others: Channel) -> None:
        """
        If currently connected to any others, removes this and the other from eachothers
        respective connections lists.

        Args:
            *others (Channel): The other channels to disconnect from.
        """
        for other in others:
            if other in self.connections:
                self.connections.remove(other)
                other.disconnect(self)

    def disconnect_all(self) -> None:
        """
        Disconnect from all other channels currently in the connections list.
        """
        self.disconnect(*self.connections)

    @property
    def connected(self) -> bool:
        """
        Has at least one connection.
        """
        return len(self.connections) > 0

    def _already_connected(self, other: Channel) -> bool:
        return other in self.connections

    def __iter__(self):
        return self.connections.__iter__()

    def __len__(self):
        return len(self.connections)

    @property
    def channel(self) -> Channel:
        return self

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
    They may optionally have a storage priority (but this doesn't do anything yet).
    (In the future they may optionally have an ontological type.)

    The `value` held by a channel can be manually assigned, but should normally be set
    by the `update` method.
    In neither case is the type hint strictly enforced.

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

    def update(self, value) -> None:
        """
        Store a new value and trigger before- and after-update routines.

        Args:
            value: The value to store.
        """
        self._before_update()
        self.value = value
        self._after_update()

    def _before_update(self) -> None:
        """
        A tool for child classes to do things before the value changed during an update.
        """
        pass

    def _after_update(self) -> None:
        """
        A tool for child classes to do things after the value changed during an update.
        """
        pass

    def connect(self, *others: DataChannel) -> None:
        """
        For all others for which the connection is valid (one input, one output, both
        data channels), adds this to the other's list of connections and the other to
        this list of connections.
        Then the input channel gets updated with the output channel's current value.

        Args:
            *others (DataChannel):

        Raises:
            TypeError: When one of others is not a `DataChannel`
        """
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

    def _valid_connection(self, other) -> bool:
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

    def _is_IO_pair(self, other: DataChannel) -> bool:
        return isinstance(other, DataChannel) and not isinstance(other, self.__class__)

    def _both_typed(self, other: DataChannel) -> bool:
        return self.type_hint is not None and other.type_hint is not None

    def _figure_out_who_is_who(self, other: DataChannel) -> (OutputData, InputData):
        return (self, other) if isinstance(self, OutputData) else (other, self)

    def __str__(self):
        return str(self.value)

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["value"] = repr(self.value)
        d["ready"] = self.ready
        return d


class InputData(DataChannel):
    """
    On `update`, Input channels will then propagate their value along to their owning
    node by invoking its `update` method.

    `InputData` channels may be set to `wait_for_update()`, and they are only `ready`
    when they are not `waiting_for_update`.
    Their parent node can be told to always set them to wait for an update after the
    node runs using `require_update_after_node_runs()`.
    This allows nodes to complete the update of multiple channels before running again.

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
        self.waiting_for_update = False

    def wait_for_update(self) -> None:
        """
        Sets `waiting_for_update` to `True`, which prevents `ready` from returning
        `True` until `update` is called.
        """
        self.waiting_for_update = True

    @property
    def ready(self) -> bool:
        """
        Extends the parent class check for whether the value matches the type hint with
        a check for whether the channel has been told to wait for an update (and not
        been updated since then).

        Returns:
            (bool): True when the stored value matches the type hint and the channel
                has not been told to wait for an update.
        """
        return not self.waiting_for_update and super().ready

    def _before_update(self) -> None:
        if self.node.running:
            raise RuntimeError(
                f"Parent node {self.node.label} of {self.label} is running, so value "
                f"cannot be updated."
            )

    def _after_update(self) -> None:
        self.waiting_for_update = False
        self.node.update()

    def require_update_after_node_runs(self, wait_now=False) -> None:
        """
        Registers this channel with its owning node as one that should have
        `wait_for_update()` applied after each time the node runs.

        Args:
            wait_now (bool): Also call `wait_for_update()` right now. (Default is
                False.)
        """
        if self.label not in self.node.channels_requiring_update_after_run:
            self.node.channels_requiring_update_after_run.append(self.label)
        if wait_now:
            self.wait_for_update()

    def activate_strict_connections(self) -> None:
        self.strict_connections = True

    def deactivate_strict_connections(self) -> None:
        self.strict_connections = False


class OutputData(DataChannel):
    """
    On `update`, Output channels propagate their value to all the input channels to
    which they are connected by invoking their `update` method.
    """

    def _after_update(self) -> None:
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
    def __call__(self) -> None:
        pass

    def connect(self, *others: SignalChannel) -> None:
        """
        For all others for which the connection is valid (one input, one output, both
        data channels), adds this to the other's list of connections and the other to
        this list of connections.

        Args:
            *others (SignalChannel): The other channels to attempt a connection to

        Raises:
            TypeError: When one of others is not a `SignalChannel`
        """
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
        return isinstance(other, SignalChannel) and not isinstance(
            other, self.__class__
        )


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
            node (pyiron_contrib.workflow.node.Node): The node to which the
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
