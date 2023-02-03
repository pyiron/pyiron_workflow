"""
Nodes take input, and when run produce output. Internally they have the structure
pre-processor -> engine -> post-processor. Where the pre- and post-processors handle
both the conversion of IO data to/from what the engine expects *and* handle any
interactions with file storage and the database. The engine just takes arbitrary kwargs
(passed from the pre-processor) and returns an arbitrary dictionary (parsed by the
post-processor). In the simplest case, it is just some python code and the dictionaries
it takes and returns align with the names of the IO.

Nodes should be able to be forced to run, or gently prodded to run but only actually
do so if all their input is ready to go.

Nodes can either be sub-classes, with pre-defined IO fields, processors, and engine,
XOR they can be instantiated with some or all of these passed in at runtime. However,
it should not be possible to mix and match -- either you're instantiating a generic
node and you're free to pass in any of the sub-components, or you're instantiating a
sub-classed node and some or all of these are pre-defined.

After running, nodes trigger the update on their output channels, which will trigger
updates of connected downstream nodes.
"""

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
    preprocessor: callable = None
    engine: callable = None
    postprocessor: callable = None
    output_channels: list[ChannelTemplate] = None

    def __init__(
            self,
            name: str,
            engine: Engine | callable,
            preprocessor: Optional[Processor] = None,
            postprocessor: Optional[Processor] = None,
            input_channels: Optional[list[ChannelTemplate]] = None,
            preprocessor: Optional[callable] = None,
            engine: Optional[callable] = None,
            postprocessor: Optional[callable] = None,
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

    def update(self) -> None:
        if self.update_automatically and self.ready:
            self.run()

    def run(self) -> None:
        engine_input = self.preprocessor(**self.input.to_value_dict())
        engine_output = self.engine(**engine_input)
        node_output = self.postprocessor(**engine_output)
        self._update_output(node_output)

    def _update_output(self, data: dict):
        for k, v in data.items():
            self.output[k].update(v)

    def __call__(self) -> None:
        self.run()

    @property
    def ready(self) -> bool:
        return self.input.ready

    @property
    def connected(self) -> bool:
        return self.input.connected or self.output.connected

    @property
    def fully_connected(self):
        return self.input.fully_connected and self.output.fully_connected


def pass_all(**kwargs) -> dict:
    """Just return everything you get as a dictionary."""
    return kwargs
