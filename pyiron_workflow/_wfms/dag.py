from __future__ import annotations

import collections
from collections.abc import MutableMapping
from typing import TypeAlias

from flowrep import wfms as fr_wfms
from flowrep.api import schemas as frs
from pyiron_snippets import retrieve
from semantikon import datastructure as sds

from pyiron_workflow._wfms import atomic, execution, flowcontrol
from pyiron_workflow._wfms.datatypes import (
    Graph,
    InputPort,
    Node,
    NodeMap,
    OutputPort,
    PortMap,
    PortType,
    RecipeType,
    StaticNode,
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


class Workflow(Node[frs.LiveWorkflow], Graph):
    """This is the key mutable one"""

    undo_stack: collections.deque[GraphDiff]
    redo_stack: collections.deque[GraphDiff]

    def __init__(
        self,
        label: frs.Label,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
        undo_limit: int = 10,
    ):
        # Add a super call later if needed
        self._label = label
        self._owner = owner
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

    def evaluate(
        self, run: execution.Run[frs.LiveWorkflow], config: execution.RunConfig
    ) -> None:
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


class Macro(StaticNode[frs.LiveWorkflow], Graph):

    def __init__(
        self,
        label: frs.Label,
        recipe: frs.WorkflowNode,
        *,
        owner: Graph | None = None,
        history_limit: int = 10,
    ):
        super().__init__(label, recipe, owner=owner, history_limit=history_limit)

        if reference := recipe.reference:
            fqn = reference.info.fully_qualified_name
            function = retrieve.import_from_string(fqn)
            self._function_metadata = getattr(function, "_semantikon_metadata", None)
        else:
            self._function_metadata = None

        self._nodes = NodeMap(
            self,
            *(
                recipe2static(label, recipe, owner=self)
                for label, recipe in recipe.nodes.items()
            ),
        )

    @classmethod
    def _result_type(cls) -> type[frs.LiveWorkflow]:
        return frs.LiveWorkflow

    @property
    def function_metadata(self) -> sds.FunctionMetadata | None:
        return self._function_metadata

    @property
    def input_edges(self) -> frs.InputEdges:
        return self._recipe.input_edges

    @property
    def edges(self) -> frs.Edges:
        return self._recipe.edges

    @property
    def output_edges(self) -> frs.OutputEdges:
        return self._recipe.output_edges

    @property
    def nodes(self) -> NodeMap:
        return self._nodes

    def evaluate(
        self, run: execution.Run[frs.LiveWorkflow], config: execution.RunConfig
    ) -> None:
        recipe = run.result.recipe
        result = run.result

        order = fr_wfms._topo_sort_children(recipe)
        # TODO: order by topological layer; parallelize with intra-layer multithreading

        for label in order:
            node = self.nodes[label]
            input_data = fr_wfms._gather_child_inputs(label, recipe, result)
            sub_run = execution.run(node, config, **input_data)
            run.steps.append(sub_run)
            result.nodes[label] = sub_run.result

        fr_wfms._populate_workflow_outputs(result, recipe)

    def to_unlocked_workflow(self) -> Workflow:
        raise NotImplementedError()


def recipe2static(
    label: frs.Label,
    recipe: RecipeType,
    owner: Graph | None = None,
) -> StaticNode:
    if isinstance(recipe, frs.AtomicNode):
        return atomic.Atomic(label, recipe, owner=owner)
    elif isinstance(recipe, frs.ForEachNode):
        return flowcontrol.ForEach(label, recipe, owner=owner)
    elif isinstance(recipe, frs.IfNode):
        return flowcontrol.If(label, recipe, owner=owner)
    elif isinstance(recipe, frs.TryNode):
        return flowcontrol.Try(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WhileNode):
        return flowcontrol.While(label, recipe, owner=owner)
    elif isinstance(recipe, frs.WorkflowNode):
        return Macro(label, recipe, owner=owner)
    else:
        raise TypeError(f"Unknown recipe type: {recipe}. Expected one of {RecipeType}.")
