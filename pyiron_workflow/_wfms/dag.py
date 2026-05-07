from __future__ import annotations

import collections
from collections.abc import MutableMapping
from typing import Any, TypeAlias

import semantikon
from flowrep import wfms as fr_wfms
from flowrep.api import schemas as frs
from pyiron_snippets import retrieve

from pyiron_workflow._wfms import constructors, execution
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
        type_metadata: semantikon.TypeMetadata | None = None,
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
        self, port: InputPort | OutputPort, metadata: semantikon.TypeMetadata | None
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
                constructors.recipe2static(label, recipe, owner=self)
                for label, recipe in recipe.nodes.items()
            ),
        )

    @classmethod
    def _result_type(cls) -> type[frs.LiveWorkflow]:
        return frs.LiveWorkflow

    @property
    def function_metadata(self) -> semantikon.FunctionMetadata | None:
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

        layers = topo_sort_nodes(self.nodes, self.edges)

        for layer in layers:
            # TODO: Optionally multithread inside a given layer
            for label in layer:
                node = self.nodes[label]
                input_data = gather_target_inputs(label, result)
                sub_run = execution.run(node, config, **input_data)
                run.steps.append(execution.Step(label, sub_run))
                result.nodes[label] = sub_run.result

        fr_wfms._populate_workflow_outputs(result, recipe)

    def to_unlocked_workflow(self) -> Workflow:
        raise NotImplementedError()


def topo_sort_nodes(nodes: NodeMap, edges: frs.Edges) -> list[list[frs.Label]]:
    """
    Kahn's algorithm over sibling edges, grouped into independent layers.

    Each layer contains nodes whose dependencies all live in earlier layers, so
    members of a layer may be executed concurrently. Deterministic tie-breaking
    by label within each layer.
    """
    in_degree: dict[frs.Label, int] = dict.fromkeys(nodes, 0)
    successors: dict[frs.Label, list[frs.Label]] = {label: [] for label in nodes}

    for target, source in edges.items():
        in_degree[target.node] += 1
        successors[source.node].append(target.node)

    current_layer = sorted(label for label in nodes if in_degree[label] == 0)
    layers: list[list[frs.Label]] = []
    processed = 0
    while current_layer:
        layers.append(current_layer)
        processed += len(current_layer)
        next_layer: list[str] = []
        for label in current_layer:
            for succ in successors.get(label, []):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    next_layer.append(succ)
        current_layer = sorted(next_layer)

    if processed != len(nodes):  # pragma: no cover
        raise ValueError(
            "Cycle detected in workflow edges. This should have been caught by the "
            "underlying recipe validation. Please raise a GitHub issue reporting "
            "how you got here!"
        )
    return layers


def gather_target_inputs(
    target_label: frs.Label,
    owner: frs.Composite,
) -> dict[str, Any]:
    """
    Resolve input values for a target node from graph input ports and sibling
    output ports according to the graph recipe edges.

    Ports not covered by any edge are omitted — the child's own defaults (if any)
    will be used downstream.
    """
    owner_recipe = owner.recipe
    target_recipe = owner_recipe.nodes[target_label]
    inputs: dict[str, Any] = {}

    for port in target_recipe.inputs:
        th = frs.TargetHandle(node=target_label, port=port)

        if th in owner_recipe.input_edges:
            owner_source = owner_recipe.input_edges[th]
            inputs[port] = owner.input_ports[owner_source.port].get_data()
        elif th in owner_recipe.edges:
            sibling_source = owner_recipe.edges[th]
            sibling = owner.nodes[sibling_source.node]
            inputs[port] = sibling.output_ports[sibling_source.port].value
        # else: port has a default on the child, _call_atomic will handle it

    return inputs
