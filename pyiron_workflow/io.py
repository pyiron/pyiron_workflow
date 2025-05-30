"""
Collections of channel objects.

These also support the syntactic sugar of treating value assignments and new
connections on the same footing.
"""

from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import ItemsView, Iterator
from typing import Any, Generic, TypeVar

from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.channels import (
    NOT_DATA,
    AccumulatingInputSignal,
    Channel,
    DataChannel,
    InputData,
    InputSignal,
    InputType,
    OutputData,
    OutputSignal,
    OutputType,
    SignalChannel,
)
from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.display_state import HasStateDisplay
from pyiron_workflow.mixin.has_interface_mixins import (
    HasChannel,
    HasLabel,
    HasRun,
)

OwnedType = TypeVar("OwnedType", bound=Channel)
OwnedConjugate = TypeVar("OwnedConjugate", bound=Channel)


class IO(HasStateDisplay, Generic[OwnedType, OwnedConjugate], ABC):
    """
    IO is a convenience layer for holding and accessing multiple input/output channels.
    It allows key and dot-based access to the underlying channels.
    Channels can also be iterated over, and there are a number of helper functions to
    alter the properties of or check the status of all the channels at once.

    A new channel can be assigned as an attribute of an IO collection, as long as it
    matches the channel's type (e.g. :class:`OutputChannel` for :class:`Outputs`, :class:`InputChannel`
    for :class:`Inputs`, etc...).

    When assigning something to an attribute holding an existing channel, if the
    assigned object is a :class:`Channel`, then an attempt is made to make a :attr:`connection`
    between the two channels, otherwise we fall back on a value assignment that must
    be defined in child classes under `_assign_value_to_existing_channel`.
    This provides syntactic sugar such that both new connections and new values can
    be assigned with a simple `=`.
    """

    channel_dict: DotDict[str, OwnedType]

    def __init__(self, *channels: OwnedType) -> None:
        self.__dict__["channel_dict"] = DotDict(
            {
                channel.label: channel
                for channel in channels
                if isinstance(channel, self._channel_class)
            }
        )

    @property
    @abstractmethod
    def _channel_class(self) -> type[OwnedType]:
        pass

    @abstractmethod
    def _assign_a_non_channel_value(self, channel: OwnedType, value: Any) -> None:
        """What to do when some non-channel value gets assigned to a channel"""
        pass

    def __getattr__(self, item: str) -> OwnedType:
        try:
            return self.channel_dict[item]
        except KeyError as key_error:
            # Raise an attribute error from getattr to make sure hasattr works well!
            raise AttributeError(
                f"Could not find attribute {item} on {self.__class__.__name__} object "
                f"nor in its channels ({self.labels})"
            ) from key_error

    def __setattr__(self, key: str, value: Any) -> None:
        if key in self.channel_dict:
            self._assign_value_to_existing_channel(self.channel_dict[key], value)
        elif isinstance(value, self._channel_class):
            if key != value.label:
                logger.info(
                    f"Assigning a channel with the label {value.label} to the io key "
                    f"{key}"
                )
            self.channel_dict[key] = value
        else:
            raise TypeError(
                f"Can only set Channel object or connect to existing channels, but the "
                f"attribute {key} got assigned {value} of type {type(value)}"
            )

    def _assign_value_to_existing_channel(self, channel: OwnedType, value: Any) -> None:
        if isinstance(value, HasChannel):
            channel.connect(value.channel)
        else:
            self._assign_a_non_channel_value(channel, value)

    def __getitem__(self, item: str) -> OwnedType:
        return self.__getattr__(item)

    def __setitem__(self, key: str, value: Any) -> None:
        self.__setattr__(key, value)

    @property
    def connections(self) -> list[OwnedConjugate]:
        """All the unique connections across all channels"""
        return list(
            {connection for channel in self for connection in channel.connections}
        )

    @property
    def connected(self) -> bool:
        return any(c.connected for c in self)

    @property
    def fully_connected(self) -> bool:
        return all(c.connected for c in self)

    def disconnect(self) -> list[tuple[OwnedType, OwnedConjugate]]:
        """
        Disconnect all connections that owned channels have.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        destroyed_connections = []
        for c in self:
            destroyed_connections.extend(c.disconnect_all())
        return destroyed_connections

    @property
    def labels(self) -> list[str]:
        return list(self.channel_dict.keys())

    def items(self) -> ItemsView[str, OwnedType]:
        return self.channel_dict.items()

    def __iter__(self) -> Iterator[OwnedType]:
        return self.channel_dict.values().__iter__()

    def __len__(self) -> int:
        return len(self.channel_dict)

    def __dir__(self):
        return list(set(super().__dir__() + self.labels))

    def __str__(self) -> str:
        return f"{self.__class__.__name__} {self.labels}"

    def __getstate__(self) -> dict[str, Any]:
        # Compatibility with python <3.11
        return dict(self.__dict__)

    def __setstate__(self, state: dict[str, Any]) -> None:
        # Because we override getattr, we need to use __dict__ assignment directly in
        # __setstate__ the same way we need it in __init__
        self.__dict__["channel_dict"] = state["channel_dict"]

    def display_state(
        self, state: dict[str, Any] | None = None, ignore_private: bool = True
    ) -> dict[str, Any]:
        state = dict(self.__getstate__()) if state is None else state
        for k, v in state["channel_dict"].items():
            state[k] = v
        del state["channel_dict"]
        return super().display_state(state=state, ignore_private=ignore_private)


class InputsIO(IO[InputType, OutputType], ABC):
    pass


class OutputsIO(IO[OutputType, InputType], ABC):
    pass


class DataIO(IO[DataChannel, DataChannel], ABC):
    def _assign_a_non_channel_value(self, channel: DataChannel, value) -> None:
        channel.value = value

    def to_value_dict(self) -> dict[str, Any]:
        return {label: channel.value for label, channel in self.channel_dict.items()}

    def to_list(self) -> list[Any]:
        """A list of channel values (order not guaranteed)"""
        return [channel.value for channel in self.channel_dict.values()]

    @property
    def ready(self) -> bool:
        return all(c.ready for c in self)

    def activate_strict_hints(self):
        [c.activate_strict_hints() for c in self]

    def deactivate_strict_hints(self):
        [c.deactivate_strict_hints() for c in self]


class Inputs(InputsIO, DataIO):
    @property
    def _channel_class(self) -> type[InputData]:
        return InputData

    def fetch(self) -> None:
        for c in self:
            c.fetch()


OutputDataType = TypeVar("OutputDataType", bound=OutputData)


class GenericOutputs(OutputsIO, DataIO, Generic[OutputDataType], ABC):
    @property
    @abstractmethod
    def _channel_class(self) -> type[OutputDataType]:
        pass


class Outputs(GenericOutputs[OutputData]):
    @property
    def _channel_class(self) -> type[OutputData]:
        return OutputData


class SignalIO(IO[SignalChannel, SignalChannel], ABC):
    def _assign_a_non_channel_value(self, channel: SignalChannel, value: Any) -> None:
        raise TypeError(
            f"Tried to assign {value} ({type(value)} to the {channel.full_label}, "
            f"which is already a {type(channel)}. Only other signal channels may be "
            f"connected in this way."
        )


class InputSignals(InputsIO, SignalIO):
    @property
    def _channel_class(self) -> type[InputSignal]:
        return InputSignal

    def disconnect_run(self) -> list[tuple[InputSignal, OutputSignal]]:
        """Disconnect all `run` and `accumulate_and_run` signals, if they exist."""
        disconnected = []
        with contextlib.suppress(AttributeError):
            disconnected += self.run.disconnect_all()
        with contextlib.suppress(AttributeError):
            disconnected += self.accumulate_and_run.disconnect_all()
        return disconnected


class OutputSignals(OutputsIO, SignalIO):
    @property
    def _channel_class(self) -> type[OutputSignal]:
        return OutputSignal


class Signals(HasStateDisplay):
    """
    A meta-container for input and output signal IO containers.

    Attributes:
        input (InputSignals): An empty input signals IO container.
        output (OutputSignals): An empty input signals IO container.
    """

    def __init__(self) -> None:
        self.input: InputSignals = InputSignals()
        self.output: OutputSignals = OutputSignals()

    def disconnect(self) -> list[tuple[SignalChannel, SignalChannel]]:
        """
        Disconnect all connections in input and output signals.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        return self.input.disconnect() + self.output.disconnect()

    def disconnect_run(self) -> list[tuple[InputSignal, OutputSignal]]:
        return self.input.disconnect_run()

    @property
    def connected(self) -> bool:
        return self.input.connected or self.output.connected

    @property
    def fully_connected(self) -> bool:
        return self.input.fully_connected and self.output.fully_connected

    def __str__(self) -> str:
        return f"{str(self.input)}\n{str(self.output)}"


OutputsType = TypeVar("OutputsType", bound=GenericOutputs)


class HasIO(HasStateDisplay, HasLabel, HasRun, Generic[OutputsType], ABC):
    """
    A mixin for classes that provide data and signal IO.

    Child classes must define how to return :class:`Input` and :class:`Output` panels,
    but a standard collections of signals is included relying on the :class:`HasRun`
    interface.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._signals = Signals()
        self._signals.input.run = InputSignal("run", self, self.run)
        self._signals.input.accumulate_and_run = AccumulatingInputSignal(
            "accumulate_and_run", self, self.run
        )
        self._signals.output.ran = OutputSignal("ran", self)
        self._signals.output.failed = OutputSignal("failed", self)

    @property
    @abstractmethod
    def inputs(self) -> Inputs:
        pass

    @property
    @abstractmethod
    def data_input_locked(self):
        """Prevents Inputs from updating when True"""

    @property
    @abstractmethod
    def outputs(self) -> OutputsType:
        pass

    @property
    def signals(self) -> Signals:
        return self._signals

    @property
    def connected(self) -> bool:
        return self.inputs.connected or self.outputs.connected or self.signals.connected

    @property
    def fully_connected(self) -> bool:
        return (
            self.inputs.fully_connected
            and self.outputs.fully_connected
            and self.signals.fully_connected
        )

    def disconnect(self) -> list[tuple[Channel, Channel]]:
        """
        Disconnect all connections belonging to inputs, outputs, and signals channels.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        destroyed_connections = []
        destroyed_connections.extend(self.inputs.disconnect())
        destroyed_connections.extend(self.outputs.disconnect())
        destroyed_connections.extend(self.signals.disconnect())
        return destroyed_connections

    def activate_strict_hints(self) -> None:
        """Enable type hint checks for all data IO"""
        self.inputs.activate_strict_hints()
        self.outputs.activate_strict_hints()

    def deactivate_strict_hints(self) -> None:
        """Disable type hint checks for all data IO"""
        self.inputs.deactivate_strict_hints()
        self.outputs.deactivate_strict_hints()

    def _connect_output_signal(self, signal: OutputSignal) -> None:
        self.signals.input.run.connect(signal)

    def __rshift__(self, other: InputSignal | HasIO) -> InputSignal | HasIO:
        """
        Allows users to connect run and ran signals like: `first >> second`.
        """
        other._connect_output_signal(self.signals.output.ran)
        return other

    def _connect_accumulating_input_signal(
        self, signal: AccumulatingInputSignal
    ) -> None:
        self.signals.output.ran.connect(signal)

    def __lshift__(self, others: tuple[OutputSignal | HasIO, ...]):
        """
        Connect one or more `ran` signals to `accumulate_and_run` signals like:
        `this << some_object, another_object, or_by_channel.signals.output.ran`
        """
        self.signals.input.accumulate_and_run << others

    def set_input_values(self, *args, **kwargs) -> None:
        """
        Match keywords to input channels and update their values.

        Throws a warning if a keyword is provided that cannot be found among the input
        keys.

        Args:
            *args: values assigned to inputs in order of appearance.
            **kwargs: input key - input value (including channels for connection) pairs.

        Raises:
            (ValueError): If more args are received than there are inputs available.
            (ValueError): If there is any overlap between channels receiving values
                from `args` and those from `kwargs`.
            (ValueError): If any of the `kwargs` keys do not match available input
                labels.
        """
        if len(args) > len(self.inputs.labels):
            raise ValueError(
                f"Received {len(args)} args, but only have {len(self.inputs.labels)} "
                f"input channels available"
            )
        keyed_args = dict(zip(self.inputs.labels, args, strict=False))

        if len(set(keyed_args.keys()).intersection(kwargs.keys())) > 0:
            raise ValueError(
                f"n args are interpreted using the first n input channels "
                f"({self.inputs.labels}), but this conflicted with received kwargs "
                f"({list(kwargs.keys())}) -- perhaps the input was ordered differently "
                f"than expected?"
            )

        kwargs.update(keyed_args)

        self._ensure_all_input_keys_present(kwargs.keys(), self.inputs.labels)

        for k, v in kwargs.items():
            self.inputs[k] = v

    @staticmethod
    def _ensure_all_input_keys_present(used_keys, available_keys):
        diff = set(used_keys).difference(available_keys)
        if len(diff) > 0:
            raise ValueError(
                f"{diff} not found among available inputs: {available_keys}"
            )

    def copy_io(
        self,
        other: HasIO,
        connections_fail_hard: bool = True,
        values_fail_hard: bool = False,
    ) -> None:
        """
        Copies connections and values from another object's IO onto this object's IO.
        Other channels with no connections are ignored for copying connections, and all
        data channels without data are ignored for copying data.
        Otherwise, default behaviour is to throw an exception if any of the other
        object's connections fail to copy, but failed value copies are simply ignored
        (e.g. because this object does not have a channel with a commensurate label or
        the value breaks a type hint).
        This error throwing/passing behaviour can be controlled with boolean flags.

        In the case that an exception is thrown, all newly formed connections are broken
        and any new values are reverted to their old state before the exception is
        raised.

        Args:
            other (HasIO): The other object whose IO to copy.
            connections_fail_hard: Whether to raise exceptions encountered when copying
                connections. (Default is True.)
            values_fail_hard (bool): Whether to raise exceptions encountered when
                copying values. (Default is False.)
        """
        new_connections = self._copy_connections(other, fail_hard=connections_fail_hard)
        try:
            self._copy_values(other, fail_hard=values_fail_hard)
        except Exception as e:
            for owned, conjugate in new_connections:
                owned.disconnect(conjugate)
            raise e

    def _copy_connections(
        self,
        other: HasIO,
        fail_hard: bool = True,
    ) -> list[tuple[Channel, Channel]]:
        """
        Copies all the connections in another object to this one.
        Expects all connected channels on the other object to have a counterpart on
        this object -- i.e. the same label, type, and (for data) a type hint compatible
        with all the existing connections being copied.
        This requirement can be optionally relaxed such that any failures encountered
        when attempting to make a connection (i.e. this object has no channel with a
        corresponding label as the other object, or the new connection fails its
        validity check), such that we simply continue past these errors and make as
        many connections as we can while ignoring errors.

        This object may freely have additional channels not present in the other object.
        The other object may have additional channels not present here as long as they
        are not connected.

        If an exception is going to be raised, any connections copied so far are
        disconnected first.

        Args:
            other (HasIO): the object whose connections should be copied.
            fail_hard (bool): Whether to raise an error an exception is encountered
                when trying to reproduce a connection. (Default is True; revert new
                connections then raise the exception.)

        Returns:
            list[tuple[Channel, Channel]]: A list of all the newly created connection
                pairs (for reverting changes).
        """
        new_connections = []
        for my_panel, other_panel in zip(
            self._owned_io_panels, other._owned_io_panels, strict=False
        ):
            for key, channel in other_panel.items():
                for target in channel.connections:
                    try:
                        my_panel[key].connect(target)
                        new_connections.append((my_panel[key], target))
                    except Exception as e:
                        if fail_hard:
                            # If you run into trouble, unwind what you've done
                            for this, that in new_connections:
                                this.disconnect(that)
                            raise ConnectionCopyError(
                                f"{self.label} could not copy connections from "
                                f"{other.label} due to the channel {key} on "
                                f"{other_panel.__class__.__name__}"
                            ) from e
                        else:
                            continue
        return new_connections

    def _copy_values(
        self,
        other: HasIO,
        fail_hard: bool = False,
    ) -> list[tuple[DataChannel, Any]]:
        """
        Copies all data from input and output channels in the other object onto this
        one.
        Ignores other channels that hold non-data.
        Failures to find a corresponding channel on this object (matching label, type,
        and compatible type hint) are ignored by default, but can optionally be made to
        raise an exception.

        If an exception is going to be raised, any values updated so far are
        reverted first.

        Args:
            other (HasIO): the object whose data values should be copied.
            fail_hard (bool): Whether to raise an error an exception is encountered
                when trying to duplicate a value. (Default is False, just keep going
                past other's channels with no compatible label here and past values
                that don't match type hints here.)

        Returns:
            list[tuple[Channel, Any]]: A list of tuples giving channels whose value has
                been updated and what it used to be (for reverting changes).
        """
        # Leverage a separate function because mypy has trouble parsing types
        # if we loop over inputs and outputs at the same time
        return self._copy_panel(
            other, self.inputs, other.inputs, fail_hard=fail_hard
        ) + self._copy_panel(other, self.outputs, other.outputs, fail_hard=fail_hard)

    def _copy_panel(
        self,
        other: HasIO,
        my_panel: DataIO,
        other_panel: DataIO,
        fail_hard: bool = False,
    ) -> list[tuple[DataChannel, Any]]:
        old_values = []
        for key, to_copy in other_panel.items():
            if to_copy.value is not NOT_DATA:
                try:
                    old_value = my_panel[key].value
                    my_panel[key].value = to_copy.value  # Gets hint-checked
                    old_values.append((my_panel[key], old_value))
                except Exception as e:
                    if fail_hard:
                        # If you run into trouble, unwind what you've done
                        for channel, value in old_values:
                            channel.value = value
                        raise ValueCopyError(
                            f"{self.label} could not copy values from "
                            f"{other.label} due to the channel {key} on "
                            f"{other_panel.__class__.__name__}, which holds value "
                            f"{to_copy.value}"
                        ) from e
                    else:
                        continue
        return old_values

    @property
    def _owned_io_panels(self) -> list[IO]:
        return [
            self.inputs,
            self.outputs,
            self.signals.input,
            self.signals.output,
        ]


class ConnectionCopyError(ValueError):
    """Raised when trying to copy IO, but connections cannot be copied"""


class ValueCopyError(ValueError):
    """Raised when trying to copy IO, but values cannot be copied"""
