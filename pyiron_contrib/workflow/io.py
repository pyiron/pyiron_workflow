from __future__ import annotations

from abc import ABC

from pyiron_contrib.workflow.channels import Channel
from pyiron_contrib.workflow.util import DotDict


class IO(ABC):
    def __init__(self, *channels: Channel):
        self.channel_list = [channel for channel in channels]
        self.channel_dict = DotDict(
            {channel.label: channel for channel in self.channel_list}
        )

    def __getattr__(self, item):
        return self.channel_dict[item]

    def __setattr__(self, key, value):
        if key in ["channel_dict", "channel_list"]:
            super().__setattr__(key, value)
        elif key in self.channel_dict.keys():
            self.channel_dict[key].connect(value)
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

    def to_value_dict(self):
        return {label: channel.value for label, channel in self.channel_dict.items()}

    @property
    def connected(self):
        return any([c.connected for c in self.channel_list])

    @property
    def fully_connected(self):
        return all([c.connected for c in self.channel_list])

    def disconnect(self):
        for c in self.channel_list:
            c.disconnect_all()

    def set_storage_priority(self, priority: int):
        for c in self.channel_list:
            c.storage_priority = priority

    @property
    def labels(self):
        return list(self.channel_dict.keys())

    def __iter__(self):
        return self.channel_list.__iter__()

    def __len__(self):
        return len(self.channel_list)

    @property
    def ready(self):
        return all([c.ready for c in self.channel_list])
