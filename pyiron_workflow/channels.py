"""
Channels are access points for information to flow into and out of nodes.
They accomplish this by forming connections between each other, and it should be as
easy as possible to form sensible and reliable connections.

Nodes get the attention, but channels are the real heroes.
"""

from __future__ import annotations

import inspect
import typing
from abc import ABC, abstractmethod

from pyiron_snippets.singleton import Singleton

from pyiron_workflow.compatibility import Self
from pyiron_workflow.mixin.display_state import HasStateDisplay
from pyiron_workflow.mixin.has_interface_mixins import HasChannel, HasLabel
from pyiron_workflow.type_hinting import (
    type_hint_is_as_or_more_specific_than,
    valid_value,
)

if typing.TYPE_CHECKING:
    from pyiron_workflow.io import HasIO


class ChannelError(Exception):
    pass


class ChannelConnectionError(ChannelError):
    pass


ConjugateType = typing.TypeVar("ConjugateType", bound="Channel")
InputType = typing.TypeVar("InputType", bound="InputChannel")
OutputType = typing.TypeVar("OutputType", bound="OutputChannel")
FlavorType = typing.TypeVar("FlavorType", bound="FlavorChannel")
ReceiverType = typing.TypeVar("ReceiverType", bound="DataChannel")


class Channel(
    HasChannel, HasLabel, HasStateDisplay, typing.Generic[ConjugateType], ABC
):
    """
    Channels facilitate the flow of information (data or control signals) into and
    out of :class:`HasIO` objects (namely nodes).

    They must have an identifier (`label: str`) and belong to an
    `owner: pyiron_workflow.io.HasIO`.


    Channels may form (:meth:`connect`/:meth:`disconnect`) and store
    (:attr:`connections`) connections with other channels.

    This connection information is reflexive, and is duplicated to be stored on _both_
    channels in the form of a reference to their counterpart in the connection.

    By using the provided methods to modify connections, the reflexive nature of
    these (dis)connections is guaranteed to be handled, and new connections are
    subjected to a validity test.

    Child classes must specify a conjugate class in order to enforce connection
    conjugate pairs which have the same "flavor" (e.g. "data" or "signal"), and
    opposite "direction" ("input" vs "output"). And they must define a string
    representation.

    Iterating over channels yields their connections.

    The length of a channel is the length of its connections.

    Attributes:
        label (str): The name of the channel.
        owner (pyiron_workflow.io.HasIO): The channel's owner.
        connections (list[Channel]): Other channels to which this channel is connected.
    """

    def __init__(
        self,
        label: str,
        owner: HasIO,
    ):
        """
        Make a new channel.

        Args:
            label (str): A name for the channel.
            owner (pyiron_workflow.io.HasIO): The channel's owner.
        """
        self._label = label
        self.owner: HasIO = owner
        self.connections: list[ConjugateType] = []

    @abstractmethod
    def __str__(self):
        pass

    @classmethod
    @abstractmethod
    def connection_conjugate(cls) -> type[ConjugateType]:
        """
        The class forming a conjugate pair with this channel class -- i.e. the same
        "flavor" of channel, but opposite in I/O.
        """

    @property
    def scoped_label(self) -> str:
        """A label combining the channel's usual label and its owner's label"""
        return f"{self.owner.label}__{self.label}"

    @property
    def full_label(self) -> str:
        """A label combining the channel's usual label and its owner's lexical path"""
        return f"{self.owner.full_label}.{self.label}"

    def connect(self, *others: ConjugateType) -> None:
        """
        Form a connection between this and one or more other channels.
        Connections are reflexive, and should only occur between input and output
        channels, i.e. they are instances of each others
        :meth:`connection_conjugate()`.

        New connections get _prepended_ to the connection lists, so they appear first
        when searching over connections.

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
            elif isinstance(other, self.connection_conjugate()):
                if self._valid_connection(other):
                    # Prepend new connections
                    # so that connection searches run newest to oldest
                    self.connections.insert(0, other)
                    other.connections.insert(0, self)
                else:
                    raise ChannelConnectionError(
                        self._connection_conjugate_failure_message(other)
                    ) from None
            else:
                raise TypeError(
                    f"Can only connect to {self.connection_conjugate()} "
                    f"objects, but {self.full_label} ({self.__class__}) "
                    f"got {other} ({type(other)})"
                )

    def _valid_connection(self, other: ConjugateType) -> bool:
        """
        Logic for determining if a connection to a conjugate partner is valid.

        Override in child classes as necessary.
        """
        return True

    def _connection_conjugate_failure_message(self, other: ConjugateType) -> str:
        return (
            f"The channel {other.full_label} ({other.__class__}) has the "
            f"correct type ({self.connection_conjugate()}) to connect with "
            f"{self.full_label} ({self.__class__}), but is not a valid "
            f"connection."
        )

    def disconnect(self, *others: ConjugateType) -> list[tuple[Self, ConjugateType]]:
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
        return destroyed_connections

    def disconnect_all(self) -> list[tuple[Self, ConjugateType]]:
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
    def channel(self) -> Self:
        return self

    def copy_connections(self, other: Self) -> None:
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

    def __getstate__(self):
        state = super().__getstate__()
        state["connections"] = []
        # It is the responsibility of the owner's parent to store and restore
        # connections (if any), since these can extend beyond the owner and would thus
        # bloat the data being sent cross-process if the owner is shipped off
        return state

    def display_state(self, state=None, ignore_private=True):
        state = dict(self.__getstate__()) if state is None else state
        state["owner"] = state["owner"].full_label  # JSON can't handle recursion
        state["connections"] = [c.full_label for c in self.connections]
        return super().display_state(state=state, ignore_private=ignore_private)


class FlavorChannel(Channel[FlavorType], ABC):
    """Abstract base for all flavor-specific channels."""


class InputChannel(Channel[OutputType], ABC):
    """Mixin for input channels."""


class OutputChannel(Channel[InputType], ABC):
    """Mixin for output channels."""


class NotData(metaclass=Singleton):
    """
    This class exists purely to initialize data channel values where no default value
    is provided; it lets the channel know that it has _no data in it_ and thus should
    not identify as ready.
    """

    @classmethod
    def __repr__(cls):
        # We use the class directly (not instances of it) where there is not yet data
        # So give it a decent repr, even as just a class
        return "NOT_DATA"

    def __reduce__(self):
        return "NOT_DATA"

    def __bool__(self):
        return False


NOT_DATA = NotData()


class DataChannel(FlavorChannel["DataChannel"], typing.Generic[ReceiverType], ABC):
    """
    Data channels control the flow of data on the graph.

    They store data persistently (:attr:`value`).

    This value may have a default (:attr:`default`) and the default-default is to be
    `NOT_DATA`.

    They may optionally have a type hint (:attr:`type_hint`).

    New data and new connections are tested against type hints (if any).

    In addition to the requirement of being a "conjugate pair", if both connecting
    channels have type hints, the output channel must have a type hint that is as or
    more specific than the input channel.

    In addition to connections, these channels can have a single partner
    (:attr:`value_receiver`) that is of the same data flavor and the same direction
    (i.e. input or output) and obeys type hints as though it were the "downstream"
    (input) partner in a connection.
    Channels with such partners pass any data updates they receive directly to this
    partner (via the :attr:`value` setter).
    (This is helpful for passing data between scopes, where we want input at one scope
    to be passed to the input of owners at a deeper scope, i.e. macro input passing to
    child node input, or vice versa for output.)

    All these type hint tests can be disabled on the input/receiving channel
    (:attr:`strict_hints: bool`), and this is recommended for the optimal performance
    in production runs.

    Channels can indicate whether they hold data they are happy with
    (:attr:`ready: bool`), which is to say it is data (not the singleton `NOT_DATA`)
    and that it conforms to the type hint (if one is provided and checking is active).

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
        value: The actual data value held by the channel.
        label (str): The label for the channel.
        owner (pyiron_workflow.io.HasIO): The channel's owner.
        default (typing.Any|None): The default value to initialize to.
            (Default is the singleton `NOT_DATA`.)
        type_hint (typing.Any|None): A type hint for values. (Default is None.)
        strict_hints (bool): Whether to check new values, connections, and partners
            when this channel is a value receiver. This can potentially be expensive, so
            consider deactivating strict hints everywhere for production runs. (Default
            is True, raise exceptions when type hints get violated.)
        value_receiver (ReceiverType|None): Another channel of the receiver type (i.e.
            also a data flavor and matching input/output type) whose value will always
            get updated when this channel's value gets updated.
    """

    def __init__(
        self,
        label: str,
        owner: HasIO,
        default: typing.Any | None = NOT_DATA,
        type_hint: typing.Any | None = None,
        strict_hints: bool = True,
        value_receiver: ReceiverType | None = None,
    ):
        super().__init__(label=label, owner=owner)
        self._value = NOT_DATA
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
            and new_value is not NOT_DATA
            and self._has_hint
            and not valid_value(new_value, self.type_hint)
        ):
            raise TypeError(
                f"The channel {self.full_label} cannot take the value `{new_value}` "
                f"({type(new_value)}) because it is not compliant with the type hint "
                f"{self.type_hint}"
            )

    @property
    def value_receiver(self) -> ReceiverType | None:
        """
        Another data channel of the same type to whom new values are always pushed
        (without type checking of any sort, not even when forming the couple!)

        Useful for macros, so that the IO of children and IO at the macro level can
        be kept synchronized.
        """
        return self._value_receiver

    @value_receiver.setter
    def value_receiver(self, new_partner: ReceiverType | None):
        if new_partner is not None:
            if not isinstance(new_partner, self.__class__):
                raise TypeError(
                    f"The {self.__class__.__name__} {self.full_label} got a coupling "
                    f"partner {new_partner} but requires something of the same type"
                )

            if new_partner is self:
                raise ValueError(
                    f"{self.__class__.__name__} {self.full_label} cannot couple to "
                    f"itself"
                )

            if (
                self._both_typed(new_partner)
                and new_partner.strict_hints
                and not type_hint_is_as_or_more_specific_than(
                    self.type_hint, new_partner.type_hint
                )
            ):
                raise ValueError(
                    f"The channel {self.full_label} cannot take "
                    f"{new_partner.full_label} as a value receiver because this "
                    f"type hint ({self.type_hint}) is not as or more specific than "
                    f"the receiving type hint ({new_partner.type_hint})."
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
            valid_value(self.value, self.type_hint)
            if self._has_hint and self.strict_hints
            else True
        )

    @property
    def _value_is_data(self) -> bool:
        return self.value is not NOT_DATA

    @property
    def _has_hint(self) -> bool:
        return self.type_hint is not None

    def _valid_connection(self, other: DataChannel) -> bool:
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

    def _connection_conjugate_failure_message(self, other: DataChannel) -> str:
        msg = super()._connection_conjugate_failure_message(other)
        msg += (
            f"Please check type hints, etc. {other.full_label}.type_hint = "
            f"{other.type_hint}; {self.full_label}.type_hint = {self.type_hint}"
        )
        return msg

    def _both_typed(self, other: DataChannel) -> bool:
        return self._has_hint and other._has_hint

    def _figure_out_who_is_who(
        self, other: DataChannel
    ) -> tuple[OutputData, InputData]:
        if isinstance(self, InputData) and isinstance(other, OutputData):
            return other, self
        elif isinstance(self, OutputData) and isinstance(other, InputData):
            return self, other
        else:
            raise ChannelError(
                f"This should be unreachable; data channel conjugate pairs should "
                f"always be input/output, but got {type(self)} for {self.full_label} "
                f"and {type(other)} for {other.full_label}. If you don't believe you "
                f"are responsible for this error, please contact the maintainers via "
                f"GitHub."
            )

    def __str__(self):
        return str(self.value)

    def activate_strict_hints(self) -> None:
        self.strict_hints = True

    def deactivate_strict_hints(self) -> None:
        self.strict_hints = False

    def __getstate__(self):
        state = super().__getstate__()
        state["_value_receiver"] = None
        # Value receivers live in the scope of Macros, so (re)storing them is the
        # owning macro's responsibility
        return state

    def display_state(self, state=None, ignore_private=True):
        state = dict(self.__getstate__()) if state is None else state
        self._make_entry_public(state, "_value", "value")
        return super().display_state(state=state, ignore_private=ignore_private)


class InputData(DataChannel["InputData"], InputChannel["OutputData"]):

    @classmethod
    def connection_conjugate(cls) -> type[OutputData]:
        return OutputData

    def fetch(self) -> None:
        """
        Sets :attr:`value` to the first value among connections (i.e. the most recent)
        that is something other than `NOT_DATA`; if no such value exists (e.g. because
        there are no connections or because all the connected output channels have
        `NOT_DATA` as their value), :attr:`value` remains unchanged.
        I.e., the connection with the highest priority for updating input data is the
        0th connection; build graphs accordingly.

        Raises:
            RuntimeError: If the owner is :attr:`running`.
        """
        for out in self.connections:
            if out.value is not NOT_DATA:
                self.value = out.value
                break

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        if self.owner.data_input_locked():
            raise RuntimeError(
                f"Owner {self.full_label} has its data input locked, "
                f"so value cannot be updated."
            )
        self._type_check_new_value(new_value)
        if self.value_receiver is not None:
            self.value_receiver.value = new_value
        self._value = new_value


class OutputData(DataChannel["OutputData"], OutputChannel["InputData"]):
    @classmethod
    def connection_conjugate(cls) -> type[InputData]:
        return InputData


SignalType = typing.TypeVar("SignalType", bound="SignalChannel")


class SignalChannel(FlavorChannel[SignalType], ABC):
    """
    Signal channels give the option control execution flow by triggering callback
    functions when the channel is called.
    Callbacks must be methods on the owner that require no positional arguments.
    Inputs optionally accept an output signal on call, which output signals always
    send when they call their input connections.

    Inputs hold a callback function to call, and outputs call each of their connections.

    Signal channels support `>>` as syntactic sugar for their connections, i.e.
    `some_output >> some_input` is equivalent to `some_input.connect(some_output)`.
    (This is also interoperable with `HasIO` objects.)
    """

    @abstractmethod
    def __call__(self) -> None:
        pass


class BadCallbackError(ValueError):
    pass


class InputSignal(SignalChannel["OutputSignal"], InputChannel["OutputSignal"]):

    def __init__(
        self,
        label: str,
        owner: HasIO,
        callback: typing.Callable,
    ):
        """
        Make a new input signal channel.

        Args:
            label (str): A name for the channel.
            owner (pyiron_workflow.io.HasIO): The channel's owner.
            callback (callable): An argument-free callback to invoke when calling this
                object. Must be a method on the owner.
        """
        super().__init__(label=label, owner=owner)
        if self._is_method_on_owner(callback) and self._all_args_arg_optional(callback):
            self._callback: str = callback.__name__
        else:
            raise BadCallbackError(
                f"The channel {self.full_label} got an unexpected callback: "
                f"{callback}. "
                f"Lives on owner: {self._is_method_on_owner(callback)}; "
                f"all args are optional: {self._all_args_arg_optional(callback)} "
            )

    @classmethod
    def connection_conjugate(cls) -> type[OutputSignal]:
        return OutputSignal

    def _is_method_on_owner(self, callback):
        try:
            return callback == getattr(self.owner, callback.__name__)
        except AttributeError:
            return False

    def _all_args_arg_optional(self, callback):
        return callable(callback) and not self._has_required_args(callback)

    @staticmethod
    def _has_required_args(func):
        return any(
            (
                param.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                and param.default == inspect.Parameter.empty
            )
            for param in inspect.signature(func).parameters.values()
        )

    @property
    def callback(self) -> typing.Callable:
        return getattr(self.owner, self._callback)

    def __call__(self, other: OutputSignal | None = None) -> None:
        self.callback()

    def __str__(self):
        return f"{self.label} runs {self.callback.__name__}"

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
        owner: HasIO,
        callback: typing.Callable,
    ):
        super().__init__(label=label, owner=owner, callback=callback)
        self.received_signals: set[str] = set()

    def __call__(self, other: OutputSignal | None = None) -> None:
        """
        Fire callback iff you have received at least one signal from each of your
        current connections.

        Resets the collection of received signals when firing.
        """
        if isinstance(other, OutputSignal):
            self.received_signals.update([other.scoped_label])
        if (
            len(
                {c.scoped_label for c in self.connections}.difference(
                    self.received_signals
                )
            )
            == 0
        ):
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


class OutputSignal(SignalChannel["InputSignal"], OutputChannel["InputSignal"]):

    @classmethod
    def connection_conjugate(cls) -> type[InputSignal]:
        return InputSignal

    def __call__(self) -> None:
        for c in self.connections:
            c(self)

    def __str__(self):
        return (
            f"{self.label} activates "
            f"{[f'{c.owner.label}.{c.label}' for c in self.connections]}"
        )

    def __rshift__(self, other: InputSignal | HasIO):
        other._connect_output_signal(self)
        return other

    def _connect_accumulating_input_signal(self, signal: AccumulatingInputSignal):
        self.connect(signal)
