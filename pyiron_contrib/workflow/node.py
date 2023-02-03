from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from pyiron_contrib.workflow.io import Input, Output

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import ChannelTemplate


class Node:
    # Children may define sub-components at the class level and override __init__ to
    # not accept them
    input_channels: list[ChannelTemplate] = None
    preprocessor: Processor = None
    engine: Engine | callable = None
    postprocessor: Processor = None
    output_channels: list[ChannelTemplate] = None

    def __init__(
            self,
            name: str,
            engine: Engine | callable,
            preprocessor: Optional[Processor] = None,
            postprocessor: Optional[Processor] = None,
            input_channels: Optional[list[ChannelTemplate]] = None,
            output_channels: Optional[list[ChannelTemplate]] = None,
            update_automatically: bool = True,
    ):
        for key, arg in [
            ("input_channels", input_channels),
            ("preprocessor", preprocessor),
            ("engine", engine),
            ("postprocessor", postprocessor),
            ("output_channels", output_channels)
        ]:
            if arg is not None and getattr(self, key) is not None:
                raise ValueError(
                    f"{key} can be defined at the class level XOR passed as an argument"
                    f"to __init__, but {name} just got both."
                )

        self.input = Input(self, *self.input_channels)
        self.preprocessor = preprocessor or self.preprocessor or Passer()
        self.engine = engine
        self.postprocessor = postprocessor or self.postprocessor or Passer()
        self.output = Output(self, *self.output_channels)
        self.update_automatically = update_automatically

    def update(self):
        if self.update_automatically and self.ready:
            self.run()

    @property
    def ready(self):
        return self.input.ready

    @property
    def connected(self):
        return self.input.connected or self.output.connected

    @property
    def fully_connected(self):
        return self.input.fully_connected and self.output.fully_connected

    def run(self):
        engine_input = self.preprocessor(**self.input.to_value_dict())
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


