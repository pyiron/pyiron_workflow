"""
Channels are access points for information to flow into and out of nodes.
They accomplish this by forming connections between each other, and it should be as
easy as possible to form sensible and reliable connections.

Nodes get the attention, but channels are the real heroes.
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

    They must have an identifier (`label: str`) and belong to a parent node
    (`node: pyiron_workflow.node.Node`).

    Non-abstract channel classes should come in input/output pairs and specify the
    a necessary ancestor for instances they can connect to
    (`connection_partner_type: type[Channel]`).

    Channels may form (`connect`/`disconnect`) and store (`connections: list[Channel]`)
    connections with other channels.

    This connection information is reflexive, and is duplicated to be stored on _both_
    channels in the form of a reference to their counterpart in the connection.

    By using the provided methods to modify connections, the reflexive nature of
    these (dis)connections is guaranteed to be handled, and new connections are
    subjected to a validity test.

    In this abstract class the only requirement is that the connecting channels form a
    "conjugate pair" of classes, i.e. they are children of each other's partner class
    (`connection_partner_type: type[Channel]`) -- input/output connects to output/input.

    Iterating over channels yields their connections.

    The length of a channel is the length of its connections.

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
            node (pyiron_workflow.node.Node): The node to which the channel belongs.
        """
        self.label: str = label
        self.node: Node = node
        self.connections: list[Channel] = []

    @abstractmethod
    def __str__(self):
        pass

    @property
    @abstractmethod
    def connection_partner_type(self) -> type[Channel]:
        """
        Input and output class pairs must specify a parent class for their valid
        connection partners.
        """

    @property
    def scoped_label(self) -> str:
        """A label combining the channel's usual label and its node's label"""
        return f"{self.node.label}__{self.label}"

    def _valid_connection(self, other: Channel) -> bool:
        """
        Logic for determining if a connection is valid.

        Connections only allowed to instances with the right parent type -- i.e.
        connection pairs should be an input/output.
        """
        return isinstance(other, self.connection_partner_type)

    def connect(self, *others: Channel) -> None:
        """
        Form a connection between this and one or more other channels.
        Connections are reflexive, and should only occur between input and output
        channels, i.e. they are instances of each others `connection_partner_type`.

        Args:
            *others (Channel): The other channel objects to attempt to connect with.

        Raises:
            (ChannelConnectionError): If the other channel is of the correct type, but
                nonetheless not a valid connection.
            (TypeError): If the other channel is not an instance of this channel's
                partner type.
        """
        for other in others:
            if other in self.connections:
                continue
            elif self._valid_connection(other):
                self.connections.append(other)
                other.connections.append(self)
            else:
                if isinstance(other, self.connection_partner_type):
                    raise ChannelConnectionError(
                        f"{other.label} ({other.__class__.__name__}) has the correct "
                        f"type ({self.connection_partner_type.__name__} to connect "
                        f"with {self.label} ({self.__class__.__name__}), but is not a "
                        f"valid connection. Please check type hints, etc."
                    )
                else:
                    raise TypeError(
                        f"Can only connect to {self.connection_partner_type.__name__} "
                        f"objects, but {self.label} ({self.__class__.__name__}) got "
                        f"{other} ({type(other)})"
                    )

    def disconnect(self, *others: Channel) -> list[tuple[Channel, Channel]]:
        """
        If currently connected to any others, removes this and the other from eachothers
        respective connections lists.

        Args:
            *others (Channel): The other channels to disconnect from.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the (input, output) conjugate
                pairs of channels that no longer participate in a connection.
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

    They store data persistently (`value`).

    This value may have a default (`default`) and the default-default is to be
    `NotData`.

    They may optionally have a type hint (`type_hint`).

    New data and new connections are tested against type hints (if any).

    In addition to the requirement of being a "conjugate pair", if both connecting
    channels have type hints, the output channel must have a type hint that is as or
    more specific than the input channel.

    In addition to connections, these channels can have a single partner
    (`value_receiver: DataChannel`) that is of the _same_ class and obeys type hints as
    though it were the "downstream" (input) partner in a connection.
    Channels with such partners pass any data updates they receive directly to this
    partner (via the `value` setter).
    (This is helpful for passing data between scopes, where we want input at one scope
    to be passed to the input of nodes at a deeper scope, i.e. macro input passing to
    child node input, or vice versa for output.)

    All these type hint tests can be disabled on the input/receiving channel
    (`strict_hints: bool`), and this is recommended for the optimal performance in
    production runs.

    Channels can indicate whether they hold data they are happy with (`ready: bool`),
    which is to say it is data (not `NotData`) and that it conforms to the type hint
    (if one is provided and checking is active).

    Output data facilitates many (but not all) python operators by injecting a new
    node to perform that operation. Where the operator is not supported, we try to
    support using the operator's dunder name as a method, e.g. `==` gives us trouble
    with hashing, but this exploits the dunder method `.__eq__(other)`, so you can call
    `.eq(other)` on output data.
    These new nodes are instructed to run at the end of instantiation, but this fails
    cleanly in case they are not ready. This is intended to accommodate two likely
    scenarios: if you're injecting a node on top of an existing result you probably
    want the injection result to also be immediately available, but if you're injecting
    it at the end of something that hasn't run yet you don't want to see an error.

    TODO:
        - Storage (including priority and history)
        - Ontological hinting

    Some comments on type hinting:
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

    Attributes:
        value: The actual data value held by the node.
        label (str): The label for the channel.
        node (pyiron_workflow.node.Node): The node to which this channel belongs.
        default (typing.Any|None): The default value to initialize to.
            (Default is the class `NotData`.)
        type_hint (typing.Any|None): A type hint for values. (Default is None.)
        strict_hints (bool): Whether to check new values, connections, and partners
            when this node is a value receiver. This can potentially be expensive, so
            consider deactivating strict hints everywhere for production runs. (Default
            is True, raise exceptions when type hints get violated.)
        value_receiver (pyiron_workflow.node.Node|None): Another node of the same class
            whose value will always get updated when this node's value gets updated.
    """

    def __init__(
        self,
        label: str,
        node: Node,
        default: typing.Optional[typing.Any] = NotData,
        type_hint: typing.Optional[typing.Any] = None,
        strict_hints: bool = True,
        value_receiver: typing.Optional[InputData] = None,
    ):
        super().__init__(label=label, node=node)
        self._value = NotData
        self._value_receiver = None
        self.type_hint = type_hint
        self.strict_hints = strict_hints
        self.default = default
        self.value = default  # Implicitly type check your default by assignment
        self.value_receiver = value_receiver

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._type_check_new_value(new_value)
        if self.value_receiver is not None:
            self.value_receiver.value = new_value
        self._value = new_value

    def _type_check_new_value(self, new_value):
        if (
            self.strict_hints
            and new_value is not NotData
            and self._has_hint
            and not valid_value(new_value, self.type_hint)
        ):
            raise TypeError(
                f"The channel {self.label} cannot take the value `{new_value}` because "
                f"it is not compliant with the type hint {self.type_hint}"
            )

    @property
    def value_receiver(self) -> InputData | OutputData | None:
        """
        Another data channel of the same type to whom new values are always pushed
        (without type checking of any sort, not even when forming the couple!)

        Useful for macros, so that the IO of owned nodes and IO at the macro level can
        be kept synchronized.
        """
        return self._value_receiver

    @value_receiver.setter
    def value_receiver(self, new_partner: InputData | OutputData | None):
        if new_partner is not None:
            if not isinstance(new_partner, self.__class__):
                raise TypeError(
                    f"The {self.__class__.__name__} {self.label} got a coupling "
                    f"partner {new_partner} but requires something of the same type"
                )

            if new_partner is self:
                raise ValueError(
                    f"{self.__class__.__name__} {self.label} cannot couple to itself"
                )

            if self._both_typed(new_partner) and new_partner.strict_hints:
                if not type_hint_is_as_or_more_specific_than(
                    self.type_hint, new_partner.type_hint
                ):
                    raise ValueError(
                        f"The channel {self.label} cannot take {new_partner.label} as "
                        f"a value receiver because this type hint ({self.type_hint}) "
                        f"is not as or more specific than the receiving type hint "
                        f"({new_partner.type_hint})."
                    )

            new_partner.value = self.value

        self._value_receiver = new_partner

    @property
    def ready(self) -> bool:
        """
        Check if the currently stored value is data and satisfies the channel's type
        hint (if hint checking is activated).

        Returns:
            (bool): Whether the value is data and matches the type hint.
        """
        return self._value_is_data and (
            valid_value(self.value, self.type_hint) if self._has_hint else True
        )

    @property
    def _value_is_data(self) -> bool:
        return self.value is not NotData

    @property
    def _has_hint(self) -> bool:
        return self.type_hint is not None

    def _valid_connection(self, other: DataChannel) -> bool:
        if super()._valid_connection(other):
            if self._both_typed(other):
                out, inp = self._figure_out_who_is_who(other)
                if not inp.strict_hints:
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

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["value"] = repr(self.value)
        d["ready"] = self.ready
        d["type_hint"] = str(self.type_hint)
        return d

    def activate_strict_hints(self) -> None:
        self.strict_hints = True

    def deactivate_strict_hints(self) -> None:
        self.strict_hints = False


class InputData(DataChannel):
    @property
    def connection_partner_type(self):
        return OutputData

    def fetch(self) -> None:
        """
        Sets `value` to the first value among connections that is something other than
        `NotData`; if no such value exists (e.g. because there are no connections or
        because all the connected output channels have `NotData` as their value),
        `value` remains unchanged.
        I.e., the connection with the highest priority for updating input data is the
        0th connection; build graphs accordingly.

        Raises:
            RuntimeError: If the parent node is `running`.
        """
        for out in self.connections:
            if out.value is not NotData:
                self.value = out.value
                break

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        if self.node.running:
            raise RuntimeError(
                f"Parent node {self.node.label} of {self.label} is running, so value "
                f"cannot be updated."
            )
        self._type_check_new_value(new_value)
        if self.value_receiver is not None:
            self.value_receiver.value = new_value
        self._value = new_value


class OutputData(DataChannel):
    @property
    def connection_partner_type(self):
        return InputData

    @staticmethod
    def _other_label(other):
        return (
            other.channel.scoped_label if isinstance(other, HasChannel) else str(other)
        )

    def get_injected_label(self, injection_class, other=None):
        suffix = f"_{self._other_label(other)}" if other is not None else ""
        return f"{self.scoped_label}_{injection_class.__name__}{suffix}"

    def _get_injection_label(self, injection_class, *args):
        other_labels = "_".join(self._other_label(other) for other in args)
        suffix = f"_{other_labels}" if len(args) > 0 else ""
        return f"{self.scoped_label}_{injection_class.__name__}{suffix}"

    def _node_injection(self, injection_class, *args, inject_self=True):
        """
        Create a new node with the same parent as this channel's node, and feed it
        arguments, or load such a node if it already exists on the parent (based on a
        name dynamically generated from the injected node class and arguments).

        Args:
            injection_class (type[Node]): The new node class to instantiate
            *args: Any arguments for that function node
            inject_self (bool): Whether to pre-pend the args with self. (Default is
                True.)

        Returns:
            (Node): The instantiated or loaded node.
        """
        label = self._get_injection_label(injection_class, *args)
        try:
            # First check if the node already exists
            return self.node.parent.nodes[label]
        except (AttributeError, KeyError):
            # Fall back on creating a new node in case parent is None or node nexists
            node_args = (self, *args) if inject_self else args
            return injection_class(
                *node_args, parent=self.node.parent, label=label, run_after_init=True
            )

    # We don't wrap __all__ the operators, because you might really want the string or
    # hash or whatever of the actual channel. But we do wrap all the dunder methods
    # that should be unambiguously referring to an operation on values

    def __getattr__(self, name):
        from pyiron_workflow.node_library.standard import GetAttr

        return self._node_injection(GetAttr, name)

    def __getitem__(self, item):
        # Break slices into deeper injections, if any slice arguments are channel-like
        if isinstance(item, slice) and any(
            isinstance(slice_input, HasChannel)
            for slice_input in [item.start, item.stop, item.step]
        ):
            from pyiron_workflow.node_library.standard import Slice

            item = self._node_injection(
                Slice, item.start, item.stop, item.step, inject_self=False
            )

        from pyiron_workflow.node_library.standard import GetItem

        return self._node_injection(GetItem, item)

    def __lt__(self, other):
        from pyiron_workflow.node_library.standard import LessThan

        return self._node_injection(LessThan, other)

    def __le__(self, other):
        from pyiron_workflow.node_library.standard import LessThanEquals

        return self._node_injection(LessThanEquals, other)

    def eq(self, other):
        from pyiron_workflow.node_library.standard import Equals

        return self._node_injection(Equals, other)

    def __ne__(self, other):
        from pyiron_workflow.node_library.standard import NotEquals

        return self._node_injection(NotEquals, other)

    def __gt__(self, other):
        from pyiron_workflow.node_library.standard import GreaterThan

        return self._node_injection(GreaterThan, other)

    def __ge__(self, other):
        from pyiron_workflow.node_library.standard import GreaterThanEquals

        return self._node_injection(GreaterThanEquals, other)

    def bool(self):
        from pyiron_workflow.node_library.standard import Bool

        return self._node_injection(Bool)

    def len(self):
        from pyiron_workflow.node_library.standard import Length

        return self._node_injection(Length)

    def contains(self, other):
        from pyiron_workflow.node_library.standard import Contains

        return self._node_injection(Contains, other)

    def __add__(self, other):
        from pyiron_workflow.node_library.standard import Add

        return self._node_injection(Add, other)

    def __sub__(self, other):
        from pyiron_workflow.node_library.standard import Subtract

        return self._node_injection(Subtract, other)

    def __mul__(self, other):
        from pyiron_workflow.node_library.standard import Multiply

        return self._node_injection(Multiply, other)

    def __rmul__(self, other):
        from pyiron_workflow.node_library.standard import RightMultiply

        return self._node_injection(RightMultiply, other)

    def __matmul__(self, other):
        from pyiron_workflow.node_library.standard import MatrixMultiply

        return self._node_injection(MatrixMultiply, other)

    def __truediv__(self, other):
        from pyiron_workflow.node_library.standard import Divide

        return self._node_injection(Divide, other)

    def __floordiv__(self, other):
        from pyiron_workflow.node_library.standard import FloorDivide

        return self._node_injection(FloorDivide, other)

    def __mod__(self, other):
        from pyiron_workflow.node_library.standard import Modulo

        return self._node_injection(Modulo, other)

    def __pow__(self, other):
        from pyiron_workflow.node_library.standard import Power

        return self._node_injection(Power, other)

    def __and__(self, other):
        from pyiron_workflow.node_library.standard import And

        return self._node_injection(And, other)

    def __xor__(self, other):
        from pyiron_workflow.node_library.standard import XOr

        return self._node_injection(XOr, other)

    def __or__(self, other):
        from pyiron_workflow.node_library.standard import Or

        return self._node_injection(Or, other)

    def __neg__(self):
        from pyiron_workflow.node_library.standard import Negative

        return self._node_injection(Negative)

    def __pos__(self):
        from pyiron_workflow.node_library.standard import Positive

        return self._node_injection(Positive)

    def __abs__(self):
        from pyiron_workflow.node_library.standard import Absolute

        return self._node_injection(Absolute)

    def __invert__(self):
        from pyiron_workflow.node_library.standard import Invert

        return self._node_injection(Invert)

    def int(self):
        from pyiron_workflow.node_library.standard import Int

        return self._node_injection(Int)

    def float(self):
        from pyiron_workflow.node_library.standard import Float

        return self._node_injection(Float)

    def __round__(self):
        from pyiron_workflow.node_library.standard import Round

        return self._node_injection(Round)

    # Because we override __getattr__ we need to get and set state for serialization
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        # Update instead of overriding in case some other attributes were added on the
        # main process while a remote process was working away
        self.__dict__.update(**state)


class SignalChannel(Channel, ABC):
    """
    Signal channels give the option control execution flow by triggering callback
    functions when the channel is called.
    Inputs optionally accept an output signal on call, which output signals always
    send when they call their input connections.

    Inputs hold a callback function to call, and outputs call each of their connections.

    Signal channels support `>>` as syntactic sugar for their connections, i.e.
    `some_output >> some_input` is equivalent to `some_input.connect(some_output)`.
    (This is also interoperable with `Node` objects, cf. the `Node` docs.)
    """

    @abstractmethod
    def __call__(self) -> None:
        pass


class InputSignal(SignalChannel):
    @property
    def connection_partner_type(self):
        return OutputSignal

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

    def __call__(self, other: typing.Optional[OutputSignal] = None) -> None:
        self.callback()

    def __str__(self):
        return f"{self.label} runs {self.callback.__name__}"

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["callback"] = self.callback.__name__
        return d

    def _connect_output_signal(self, signal: OutputSignal):
        self.connect(signal)


class AccumulatingInputSignal(InputSignal):
    """
    An input signal that only fires after receiving a signal from _all_ its connections
    instead of after _any_ of its connections.
    """

    def __init__(
        self,
        label: str,
        node: Node,
        callback: callable,
    ):
        super().__init__(label=label, node=node, callback=callback)
        self.received_signals: set[OutputSignal] = set()

    def __call__(self, other: OutputSignal) -> None:
        """
        Fire callback iff you have received at least one signal from each of your
        current connections.

        Resets the collection of received signals when firing.
        """
        self.received_signals.update([other])
        if len(set(self.connections).difference(self.received_signals)) == 0:
            self.reset()
            self.callback()

    def reset(self) -> None:
        """
        Reset the collection of received signals
        """
        self.received_signals = set()

    def __lshift__(self, others):
        others = others if isinstance(others, tuple) else (others,)
        for other in others:
            other._connect_accumulating_input_signal(self)


class OutputSignal(SignalChannel):
    @property
    def connection_partner_type(self):
        return InputSignal

    def __call__(self) -> None:
        for c in self.connections:
            c(self)

    def __str__(self):
        return (
            f"{self.label} activates "
            f"{[f'{c.node.label}.{c.label}' for c in self.connections]}"
        )

    def __rshift__(self, other: InputSignal | Node):
        other._connect_output_signal(self)
        return other

    def _connect_accumulating_input_signal(self, signal: AccumulatingInputSignal):
        self.connect(signal)
