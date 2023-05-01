from __future__ import annotations

from abc import ABC, abstractmethod

from pyiron_contrib.workflow.channels import (
    Channel,
    InputData, OutputData,
    InputSignal, OutputSignal
)
from pyiron_contrib.workflow.has_channel import HasChannel
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.util import DotDict


class IO(HasToDict, ABC):
    """
    IO is a convenience layer for holding and accessing multiple input/output channels.
    It allows key and dot-based access to the underlying channels based on their name.
    Channels can also be iterated over, and there are a number of helper functions to
    alter the properties of or check the status of all the channels at once.

    A new channel can be assigned as an attribute of an IO collection, as long as the
    attribute name matches the channel's label and type (i.e. `OutputChannel` for
    `Outputs` and `InputChannel` for `Inputs`).

    When assigning something to an attribute holding an existing channel, if the
    assigned object is a `Channel`, then it is treated like a `connection`, otherwise
    it is treated like a value `update`. I.e.
    >>> some_io.some_existing_channel = 5

    is equivalent to
    >>> some_io.some_existing_channel.update(5)

    and
    >>> some_io.some_existing_channel = some_other_channel

    is equivalent to
    >>> some_io.some_existing_channel.connect(some_other_channel)
    """
    def __init__(self, *channels: Channel):
        self.channel_dict = DotDict(
            {
                channel.label: channel for channel in channels
                if isinstance(channel, self._channel_class)
            }
        )

    @property
    @abstractmethod
    def _channel_class(self) -> Channel:
        pass

    @abstractmethod
    def _set_existing(self, key, value):
        pass

    def __getattr__(self, item):
        return self.channel_dict[item]

    def __setattr__(self, key, value):
        if key in ["channel_dict"]:
            super().__setattr__(key, value)
        elif key in self.channel_dict.keys():
            self._set_existing(key, value)
        elif isinstance(value, self._channel_class):
            if key != value.label:
                raise ValueError(
                    f"Channels can only be assigned to attributes matching their label,"
                    f"but just tried to assign the channel {value.label} to {key}"
                )
            self.channel_dict[key] = value
        else:
            raise TypeError(
                f"Can only set Channel object or connect to existing channels, but the "
                f"attribute {key} got assigned {value} of type {type(value)}"
            )

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    @property
    def connected(self):
        return any([c.connected for c in self])

    @property
    def fully_connected(self):
        return all([c.connected for c in self])

    def disconnect(self):
        for c in self:
            c.disconnect_all()

    @property
    def labels(self):
        return list(self.channel_dict.keys())

    def __iter__(self):
        return self.channel_dict.values().__iter__()

    def __len__(self):
        return len(self.channel_dict)

    def __dir__(self):
        return set(super().__dir__() + self.labels)

    def to_dict(self):
        return {
            "label": self.__class__.__name__,
            "connected": self.connected,
            "fully_connected": self.fully_connected,
            "channels": {l: c.to_dict() for l, c in self.channel_dict.items()}
        }


class DataIO(IO, ABC):
    def _set_existing(self, key, value):
        from pyiron_contrib.workflow.node import SingleValueNode

        if isinstance(value, HasChannel):
            self.channel_dict[key].connect(value.channel)
        elif isinstance(value, SingleValueNode):
            self.channel_dict[key].connect(list(value.outputs.channel_dict.values())[0])
        else:
            self.channel_dict[key].update(value)

    def to_value_dict(self):
        return {label: channel.value for label, channel in self.channel_dict.items()}

    @property
    def ready(self):
        return all([c.ready for c in self])

    def set_storage_priority(self, priority: int):
        for c in self:
            c.storage_priority = priority

    def to_dict(self):
        d = super().to_dict()
        d["ready"] = self.ready
        return d


class Inputs(DataIO):
    @property
    def _channel_class(self) -> InputData:
        return InputData

    def activate_strict_connections(self):
        [c.activate_strict_connections() for c in self]

    def deactivate_strict_connections(self):
        [c.deactivate_strict_connections() for c in self]


class Outputs(DataIO):
    @property
    def _channel_class(self) -> OutputData:
        return OutputData


class SignalIO(IO, ABC):
    def _set_existing(self, key, value):
        if isinstance(value, HasChannel):
            self.channel_dict[key].connect(value.channel)
        else:
            raise TypeError(
                f"Tried to assign {value} ({type(value)} to the {key}, which is already"
                f" a {type(self.channel_dict[key])}. Only other signal channels may be "
                f"connected in this way."
            )


class InputSignals(SignalIO):
    @property
    def _channel_class(self) -> InputSignal:
        return InputSignal


class OutputSignals(SignalIO):
    @property
    def _channel_class(self) -> OutputSignal:
        return OutputSignal


class Signals:
    def __init__(self):
        self.input = InputSignals()
        self.output = OutputSignals()

    def disconnect(self):
        self.input.disconnect()
        self.output.disconnect()

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
