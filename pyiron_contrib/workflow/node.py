from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import ChannelTemplate


class Node:
    input_channels: list[ChannelTemplate] = {}
    output_channels: list[ChannelTemplate] = {}

    def __init__(
            self,
            engine: Engine | callable,
            preprocessor: Optional[Processor] = None,
            postprocessor: Optional[Processor] = None,
            update_automatically: bool = True,
            input_channels: Optional[list[ChannelTemplate]] = None,
            output_channels: Optional[list[ChannelTemplate]] = None,
    ):
        if input_channels is not None:
            self._check_channel_conflict(input_channels, self.input_channels)
            self.input_channels = input_channels
        if output_channels is not None:
            self._check_channel_conflict(output_channels, self.output_channels)
            self.output_channels = output_channels

        self.input = {
            name: inp.to_input(self)
            for name, inp in self.input_channels.items()
        }
        self.output = {
            name: out.to_output(self)
            for name, out in self.output_channels.items()
        }
        self.preprocessor = preprocessor if preprocessor is not None else Passer()
        self.engine = engine
        self.postprocessor = postprocessor if postprocessor is not None else Passer()
        self.update_automatically = update_automatically

    def _check_channel_conflict(self, argument, attribute):
        if argument is not None and len(attribute) > 0:
            raise ValueError(
                "Input/Output channels can be specified at instantiation XOR as "
                "subclass attributes -- not both."
            )

    def update(self):
        if self.update_automatically and self.ready:
            self.run()

    @property
    def ready(self):
        return all([inp.ready for inp in self.input.values()])

    def run(self):
        engine_input = self.preprocessor(
            **{name: channel.value for name, channel in self.input.items()}
        )
        engine_output = self.engine(**engine_input)
        node_output = self.postprocessor(**engine_output)
        self._update_output(node_output)

    def _update_output(self, data: dict):
        if not self._dict_is_subset(data, self.output):
            raise KeyError("Got unrecognized input...")  # And say something useful
        for k, v in data.items():
            self.output[k].update(v)

    @staticmethod
    def _dict_is_subset(candidate: dict, reference: dict):
        return len(set(candidate.keys()).difference(reference.keys())) == 0

    def __call__(self, **kwargs):
        self.run(**kwargs)


class Engine(ABC):
    @abstractmethod
    def run(self, **kwargs):
        pass

    def __call__(self, **kwargs):
        return self.run(**kwargs)


class Processor(ABC):
    @abstractmethod
    def __call__(self, **kwargs) -> dict:
        pass


class Passer(Processor):
    def __call__(self, **kwargs):
        return kwargs


