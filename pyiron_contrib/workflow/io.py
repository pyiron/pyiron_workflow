from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class ChannelTemplate:
    def __init__(
            self,
            default: Optional[Any] = None,
            types: Optional[tuple] = None
    ):
        self.default = default
        self.types = types

    def _to_IOChannel(self, node: Node, class_: type[IOChannel]) -> IOChannel:
        return class_(
            node=node,
            default=deepcopy(self.default),
            types=self.types,
        )

    def to_input(self, node: Node) -> InputChannel:
        return self._to_IOChannel(node, InputChannel)

    def to_output(self, node: Node) -> OutputChannel:
        return self._to_IOChannel(node, OutputChannel)


class IOChannel(ABC):
    def __init__(
            self,
            node: Node,
            default: Optional[Any] = None,
            types: Optional[tuple] = None
    ):
        self.node = node
        self.default = default
        self.value = default
        self.types = None if types is None else self._types_to_tuple(types)
        self.connections = []

    @staticmethod
    def _types_to_tuple(types):
        if isinstance(types, tuple):
            return types
        elif isinstance(types, list):
            return tuple(types)
        else:
            return (types,)

    @property
    def ready(self):
        if self.types is not None:
            return isinstance(self.value, self.types)
        else:
            return True

    @abstractmethod
    def connect(self, other: IOChannel):
        pass

    def disconnect(self, other: IOChannel):
        if other in self.connections:
            self.connections.remove(other)
            other.disconnect(self)

    @staticmethod
    def _valid_connection(output: OutputChannel, input: InputChannel):
        already_connected = output in input.connections
        if output.types is not None and input.types is not None:
            out_types_are_subset = len(set(output.types).difference(input.types)) == 0
            return out_types_are_subset and not already_connected
        else:
            return not already_connected


class InputChannel(IOChannel):
    def update(self, value):
        self.value = value
        self.node.update()

    def connect(self, other: OutputChannel):
        if self._valid_connection(other, self):
            self.connections.append(other)
            other.connections.append(self)


class OutputChannel(IOChannel):
    def update(self, value):
        self.value = value
        for inp in self.connections:
            inp.update(self.value)

    def connect(self, other: InputChannel):
        if self._valid_connection(self, other):
            self.connections.append(other)
            other.connect(self)
