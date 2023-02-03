from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyiron_contrib.workflow.channels import (
    Channel, ChannelTemplate, InputChannel, OutputChannel
)

if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class _IO(ABC):
    def __init__(self, *channels: Channel):
        self._channels = {
            channel.name: channel for channel in channels
            if isinstance(channel, self._channel_class)
            # else raise TypeError(f"Expected {self._channel_class} but {channel.name} had type {type(channel)}")
        }

    def __getattr__(self, item):
        return self._channels[item]

    def __setattr__(self, key, value):
        if key in ["_channels"]:
            super().__setattr__(key, value)
        elif key in self._channels.keys():
            self._channels[key].connect(value)
        elif isinstance(value, self._channel_class):
            if key != value.name:
                raise ValueError(
                    f"Channels can only be assigned to attributes matching their name,"
                    f"but just tried to assign the channel {value.name} to {key}"
                )
            self._channels[key] = value
        else:
            raise TypeError(
                f"Can only set Channel object or connect to existing channels, but the "
                f"attribute {key} got assigned {value} of type {type(value)}"
            )

    @property
    @abstractmethod
    def _channel_class(self) -> type[Channel]:
        pass

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def to_value_dict(self):
        return {name: channel.value for name, channel in self._channels.items()}

    @property
    def connected(self):
        return any([c.connected for c in self._channels.values()])

    @property
    def fully_connected(self):
        return all([c.connected for c in self._channels.values()])

    def disconnect(self):
        for c in self._channels.values():
            c.disconnect_all()

    def set_storage_priority(self, priority: int):
        for c in self._channels.values():
            c.storage_priority = priority


class Input(_IO):
    def __init__(self, node: Node, *channels: ChannelTemplate):
        super().__init__(*[channel.to_input(node) for channel in channels])

    @property
    def _channel_class(self) -> type[InputChannel]:
        return InputChannel

    @property
    def ready(self):
        return all([c.ready for c in self._channels.values()])


class Output(_IO):
    def __init__(self, node: Node, *channels: ChannelTemplate):
        super().__init__(*[channel.to_output(node) for channel in channels])

    @property
    def _channel_class(self) -> type[OutputChannel]:
        return OutputChannel
