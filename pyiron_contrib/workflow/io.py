from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class IOChannel:
    def __init__(
            self,
            default: Optional[Any] = None,
            types: Optional[tuple] = None
    ):
        self.default = default
        self.types = types

    def _to_IO(self, node: Node, class_: type[IO]) -> IO:
        return class_(
            node=node,
            default=deepcopy(self.default),
            types=self.types,
        )

    def to_input(self, node: Node) -> Input:
        return self._to_IO(node, Input)

    def to_output(self, node: Node) -> Output:
        return self._to_IO(node, Output)


class IO(ABC):
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
    def connect(self, other: IO):
        pass

    def disconnect(self, other: IO):
        if other in self.connections:
            self.connections.remove(other)
            other.disconnect(self)

    @staticmethod
    def _valid_connection(output: Output, input: Input):
        already_connected = output in input.connections
        if output.types is not None and input.types is not None:
            out_types_are_subset = len(set(output.types).difference(input.types)) == 0
            return out_types_are_subset and not already_connected
        else:
            return not already_connected


class Input(IO):
    def update(self, value):
        self.value = value
        self.node.update()

    def connect(self, other: Output):
        if self._valid_connection(other, self):
            self.connections.append(other)
            other.connections.append(self)


class Output(IO):
    def update(self, value):
        self.value = value
        for inp in self.connections:
            inp.update(self.value)

    def connect(self, other: Input):
        if self._valid_connection(self, other):
            self.connections.append(other)
            other.connect(self)
