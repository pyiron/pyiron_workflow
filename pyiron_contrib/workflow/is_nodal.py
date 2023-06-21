from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyiron_contrib.workflow.io import Signals, InputSignal, OutputSignal

if TYPE_CHECKING:
    from pyiron_base.jobs.job.extension.server.generic import Server

    from pyiron_contrib.workflow.io import Inputs, Outputs


class IsNodal(ABC):
    """
    A mixin class for classes that can represent nodes on a computation graph.
    """

    def __init__(self, label: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.label: str = label
        self.running = False
        self.failed = False
        # TODO: Replace running and failed with a state object
        self._server: Server | None = (
            None  # Or "task_manager" or "executor" -- we'll see what's best
        )
        self.signals = self._build_signal_channels()

    @property
    @abstractmethod
    def inputs(self) -> Inputs:
        pass

    @property
    @abstractmethod
    def outputs(self) -> Outputs:
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def run(self):
        pass

    def _build_signal_channels(self) -> Signals:
        signals = Signals()
        signals.input.run = InputSignal("run", self, self.run)
        signals.output.ran = OutputSignal("ran", self)
        return signals

    @property
    def server(self) -> Server | None:
        return self._server

    @server.setter
    def server(self, server: Server | None):
        self._server = server

    def disconnect(self):
        self.inputs.disconnect()
        self.outputs.disconnect()
        self.signals.disconnect()

    @property
    def ready(self) -> bool:
        return not (self.running or self.failed) and self.inputs.ready

    @property
    def connected(self) -> bool:
        return self.inputs.connected or self.outputs.connected or self.signals.connected

    @property
    def fully_connected(self):
        return (
            self.inputs.fully_connected
            and self.outputs.fully_connected
            and self.signals.fully_connected
        )
