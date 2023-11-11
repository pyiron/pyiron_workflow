"""
Collections of channel objects.

These also support the syntactic sugar of treating value assignments and new
connections on the same footing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyiron_workflow.channels import (
    Channel,
    DataChannel,
    InputData,
    OutputData,
    SignalChannel,
    InputSignal,
    OutputSignal,
)
from pyiron_workflow.has_channel import HasChannel
from pyiron_workflow.has_to_dict import HasToDict
from pyiron_workflow.util import DotDict, logger


class IO(HasToDict, ABC):
    """
    IO is a convenience layer for holding and accessing multiple input/output channels.
    It allows key and dot-based access to the underlying channels.
    Channels can also be iterated over, and there are a number of helper functions to
    alter the properties of or check the status of all the channels at once.

    A new channel can be assigned as an attribute of an IO collection, as long as it
    matches the channel's type (e.g. `OutputChannel` for `Outputs`, `InputChannel`
    for `Inputs`, etc...).

    When assigning something to an attribute holding an existing channel, if the
    assigned object is a `Channel`, then an attempt is made to make a `connection`
    between the two channels, otherwise we fall back on a value assignment that must
    be defined in child classes under `_assign_value_to_existing_channel`.
    This provides syntactic sugar such that both new connections and new values can
    be assigned with a simple `=`.
    """

    def __init__(self, *channels: Channel):
        self.__dict__["channel_dict"] = DotDict(
            {
                channel.label: channel
                for channel in channels
                if isinstance(channel, self._channel_class)
            }
        )

    @property
    @abstractmethod
    def _channel_class(self) -> type(Channel):
        pass

    @abstractmethod
    def _assign_a_non_channel_value(self, channel: Channel, value) -> None:
        """What to do when some non-channel value gets assigned to a channel"""
        pass

    def __getattr__(self, item) -> Channel:
        try:
            return self.channel_dict[item]
        except KeyError:
            # Raise an attribute error from getattr to make sure hasattr works well!
            raise AttributeError(
                f"Could not find attribute {item} on {self.__class__.__name__} object "
                f"nor in its channels ({self.labels})"
            )

    def __setattr__(self, key, value):
        if key in self.channel_dict.keys():
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

    def _assign_value_to_existing_channel(self, channel: Channel, value) -> None:
        if isinstance(value, HasChannel):
            channel.connect(value.channel)
        else:
            self._assign_a_non_channel_value(channel, value)

    def __getitem__(self, item) -> Channel:
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    @property
    def connections(self) -> list[Channel]:
        """All the unique connections across all channels"""
        return list(
            set([connection for channel in self for connection in channel.connections])
        )

    @property
    def connected(self):
        return any([c.connected for c in self])

    @property
    def fully_connected(self):
        return all([c.connected for c in self])

    def disconnect(self) -> list[tuple[Channel, Channel]]:
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
    def labels(self):
        return list(self.channel_dict.keys())

    def items(self):
        return self.channel_dict.items()

    def __iter__(self):
        return self.channel_dict.values().__iter__()

    def __len__(self):
        return len(self.channel_dict)

    def __dir__(self):
        return set(super().__dir__() + self.labels)

    def __str__(self):
        return f"{self.__class__.__name__} {self.labels}"

    def to_dict(self):
        return {
            "label": self.__class__.__name__,
            "connected": self.connected,
            "fully_connected": self.fully_connected,
            "channels": {l: c.to_dict() for l, c in self.channel_dict.items()},
        }

    def __getstate__(self):
        # Compatibility with python <3.11
        return self.__dict__

    def __setstate__(self, state):
        # Because we override getattr, we need to use __dict__ assignment directly in
        # __setstate__ the same way we need it in __init__
        self.__dict__["channel_dict"] = state["channel_dict"]


class DataIO(IO, ABC):
    def _assign_a_non_channel_value(self, channel: DataChannel, value) -> None:
        channel.value = value

    def to_value_dict(self):
        return {label: channel.value for label, channel in self.channel_dict.items()}

    @property
    def ready(self):
        return all([c.ready for c in self])

    def to_dict(self):
        d = super().to_dict()
        d["ready"] = self.ready
        return d

    def activate_strict_hints(self):
        [c.activate_strict_hints() for c in self]

    def deactivate_strict_hints(self):
        [c.deactivate_strict_hints() for c in self]


class Inputs(DataIO):
    @property
    def _channel_class(self) -> type(InputData):
        return InputData

    def fetch(self):
        for c in self:
            c.fetch()


class Outputs(DataIO):
    @property
    def _channel_class(self) -> type(OutputData):
        return OutputData


class SignalIO(IO, ABC):
    def _assign_a_non_channel_value(self, channel: SignalChannel, value) -> None:
        raise TypeError(
            f"Tried to assign {value} ({type(value)} to the {channel.label}, which is "
            f"already a {type(channel)}. Only other signal channels may be connected "
            f"in this way."
        )


class InputSignals(SignalIO):
    @property
    def _channel_class(self) -> type(InputSignal):
        return InputSignal

    def disconnect_run(self) -> list[tuple[Channel, Channel]]:
        try:
            return self.run.disconnect_all()
        except AttributeError:
            return []


class OutputSignals(SignalIO):
    @property
    def _channel_class(self) -> type(OutputSignal):
        return OutputSignal


class Signals:
    """
    A meta-container for input and output signal IO containers.

    Attributes:
        input (InputSignals): An empty input signals IO container.
        output (OutputSignals): An empty input signals IO container.
    """

    def __init__(self):
        self.input = InputSignals()
        self.output = OutputSignals()

    def disconnect(self) -> list[tuple[Channel, Channel]]:
        """
        Disconnect all connections in input and output signals.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        return self.input.disconnect() + self.output.disconnect()

    def disconnect_run(self) -> list[tuple[Channel, Channel]]:
        return self.input.disconnect_run()

    @property
    def connected(self):
        return self.input.connected or self.output.connected

    @property
    def fully_connected(self):
        return self.input.fully_connected and self.output.fully_connected

    def to_dict(self):
        return {
            "input": self.input.to_dict(),
            "output": self.output.to_dict(),
        }

    def __str__(self):
        return f"{str(self.input)}\n{str(self.output)}"
