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
from pyiron_workflow.mixin.has_interface_mixins import HasChannel

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
            self._assign_a_channel_value(channel, value)
        else:
            self._assign_a_non_channel_value(channel, value)

    def _assign_a_channel_value(self, channel: OwnedType, value: HasChannel) -> None:
        channel.connect(value.channel)

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

    def _assign_a_channel_value(self, channel: OwnedType, value: Any) -> None:
        # Allow the owned input data channel to overwrite its connection
        channel.disconnect_all()
        super()._assign_a_channel_value(channel, value)


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
