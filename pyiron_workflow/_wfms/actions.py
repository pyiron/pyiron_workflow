from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Protocol, TypeAlias

if TYPE_CHECKING:
    import flowrep as fr

    from pyiron_workflow._wfms.datatypes import (
        EdgeTuple,
        InputPort,
        MutableDag,
        Node,
        OutputPort,
    )


class GraphAction(Protocol):
    def inverse(self) -> GraphAction: ...


@dataclasses.dataclass(frozen=True)
class AddInput:
    port: InputPort

    def inverse(self) -> RemoveInput:
        return RemoveInput(self.port)


@dataclasses.dataclass(frozen=True)
class RemoveInput:
    port: InputPort

    def inverse(self) -> AddInput:
        return AddInput(self.port)


@dataclasses.dataclass(frozen=True)
class AddOutput:
    port: OutputPort

    def inverse(self) -> RemoveOutput:
        return RemoveOutput(self.port)


@dataclasses.dataclass(frozen=True)
class RemoveOutput:
    port: OutputPort

    def inverse(self) -> AddOutput:
        return AddOutput(self.port)


@dataclasses.dataclass(frozen=True)
class ReplacePort:
    old_port: InputPort | OutputPort
    new_port: InputPort | OutputPort

    def inverse(self) -> ReplacePort:
        return ReplacePort(self.new_port, self.old_port)


@dataclasses.dataclass(frozen=True)
class AddNode:
    node: Node

    def inverse(self) -> RemoveNode:
        return RemoveNode(self.node)


@dataclasses.dataclass(frozen=True)
class RemoveNode:
    node: Node

    def inverse(self) -> AddNode:
        return AddNode(self.node)


@dataclasses.dataclass(frozen=True)
class AddEdge:
    edge: EdgeTuple

    def inverse(self) -> RemoveEdge:
        return RemoveEdge(self.edge)


@dataclasses.dataclass(frozen=True)
class RemoveEdge:
    edge: EdgeTuple

    def inverse(self) -> AddEdge:
        return AddEdge(self.edge)


@dataclasses.dataclass(frozen=True)
class RenameNode:
    node: Node
    old_label: fr.schemas.Label
    new_label: fr.schemas.Label

    def inverse(self) -> RenameNode:
        return RenameNode(self.node, self.new_label, self.old_label)


@dataclasses.dataclass(frozen=True)
class MoveNode:
    node: Node
    from_graph: MutableDag
    to_graph: MutableDag
    old_label: fr.schemas.Label
    new_label: fr.schemas.Label

    def inverse(self) -> MoveNode:
        return MoveNode(
            node=self.node,
            from_graph=self.to_graph,
            to_graph=self.from_graph,
            old_label=self.new_label,
            new_label=self.old_label,
        )


GraphDiff: TypeAlias = list[GraphAction]
