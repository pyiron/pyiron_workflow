from __future__ import annotations

import collections
from collections.abc import MutableMapping
from typing import TypeAlias

from flowrep.api import schemas as frs
from semantikon import datastructure as sds

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import (
    Graph,
    InputPort,
    Node,
    NodeMap,
    OutputPort,
    PortMap,
    PortType,
    RecipeType,
)


class MutablePortMap(
    PortMap[PortType, "Workflow"], MutableMapping[frs.Label, PortType]
):
    def __setitem__(self, key: frs.Label, value: PortType):
        if value.owner is not self.owner:
            raise ValueError(
                f"Port {key} already has owner {value.owner.lexical_path!r} and cannot "
                f"be assigned to a port map with owner {self.owner!r}"
            )
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: frs.Label):
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: frs.Label, value: PortType):
        self.__setitem__(key, value)


class MutableNodeMap(NodeMap, MutableMapping[frs.Label, Node]):
    def __setitem__(self, key: frs.Label, value: Node):
        value.owner = self._pwf_lexical_map__owner
        self._pwf_lexical_map__data[key] = value

    def __delitem__(self, key: frs.Label):
        value = self._pwf_lexical_map__data[key]
        value.owner = None
        del self._pwf_lexical_map__data[key]

    def __setattr__(self, key: frs.Label, value: Node):
        self.__setitem__(key, value)


GraphAction: TypeAlias = tuple  # TODO, but probably needs an enum for action type
GraphDiff: TypeAlias = list[GraphAction]


class Workflow(Graph[frs.LiveWorkflow]):
    """This is the key mutable one"""

    undo_stack: collections.deque[GraphDiff]
    redo_stack: collections.deque[GraphDiff]

    def __init__(
        self,
        label: frs.Label,
        *,
        history_limit: int = 10,
        undo_limit: int = 10,
    ):
        # Add a super call later if needed
        self._label = label
        self._owner = None
        self.executor = None
        self.current_run = None
        self.run_history = collections.deque(maxlen=history_limit)
        self._nodes = MutableNodeMap(self)
        self._inputs = MutablePortMap[InputPort](self)
        self._outputs = MutablePortMap[OutputPort](self)
        self.undo_stack = collections.deque(maxlen=undo_limit)
        self.redo_stack = collections.deque(maxlen=undo_limit)

    @property
    def inputs(self) -> MutablePortMap[InputPort]:
        return self._inputs

    @property
    def outputs(self) -> MutablePortMap[OutputPort]:
        return self._outputs

    @property
    def recipe(self) -> RecipeType:
        raise NotImplementedError()

    def generate_flowrep_live_node(self) -> frs.LiveWorkflow:
        raise NotImplementedError()

    def evaluate(self, run: execution.Run[frs.LiveWorkflow]) -> None:
        raise NotImplementedError()

    @property
    def nodes(self) -> MutableNodeMap:
        return self._nodes

    @property
    def undo_limit(self) -> int | None:
        return self.undo_stack.maxlen

    @undo_limit.setter
    def undo_limit(self, value: int) -> None:
        self.undo_stack = collections.deque(self.undo_stack, maxlen=value)
        self.redo_stack = collections.deque(self.redo_stack, maxlen=value)

    def to_locked_macro(self) -> Macro:
        raise NotImplementedError()

    def create_input(
        self,
        label: frs.Label,
        type_hint: type | None = None,
        type_metadata: sds.TypeMetadata | None = None,
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def rename_input(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def create_output(
        self, label: frs.Label, type_hint: type | None = None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def add_port_hint(
        self, port: InputPort | OutputPort, hint: type | None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_port_hint(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_hint(port, None)

    def add_port_metadata(
        self, port: InputPort | OutputPort, metadata: sds.TypeMetadata | None
    ) -> GraphDiff:
        raise NotImplementedError()

    def remove_port_metadata(self, port: InputPort | OutputPort) -> GraphDiff:
        return self.add_port_metadata(port, None)

    def rename_output(self, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def add_node(self, *nodes: Node) -> GraphDiff:
        raise NotImplementedError()

    def remove_node(self, *nodes: Node) -> GraphDiff:
        raise NotImplementedError()

    def rename_node(self, node: Node, label: frs.Label) -> GraphDiff:
        raise NotImplementedError()

    def connect(self, *edges) -> GraphDiff:
        raise NotImplementedError()

    def disconnect(self, *edges) -> GraphDiff:
        raise NotImplementedError()

    def group(self, *nodes) -> GraphDiff:
        raise NotImplementedError()

    def ungroup(self, *nodes) -> GraphDiff:
        raise NotImplementedError()

    def _undo_diff(self, diff: GraphDiff) -> GraphDiff:
        # For user-facing undo, and for rolling back failed change attempts
        raise NotImplementedError()

    def undo(self, steps: int = 1) -> list[GraphDiff]:
        # Iteratively pop from the undo stack and send to the private _undo_diff
        raise NotImplementedError()

    def _redo_diff(self, diff: GraphDiff) -> GraphDiff:
        raise NotImplementedError()

    def redo(self, steps: int = 1) -> list[GraphDiff]:
        raise NotImplementedError()


class Macro(Graph[frs.LiveWorkflow]):  # Not implemented
    function_metadata: sds.FunctionMetadata | None

    def to_unlocked_workflow(self) -> Workflow:
        raise NotImplementedError()
