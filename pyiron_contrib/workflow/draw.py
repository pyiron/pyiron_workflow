"""
Functions for drawing the graph.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

import graphviz

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import Channel as WorkflowChannel
    from pyiron_contrib.workflow.io import DataIO, SignalIO
    from pyiron_contrib.workflow.node import Node as WorkflowNode


def directed_graph(name, label, rankdir="TB"):
    """A shortcut method for instantiating the type of graphviz graph we want"""
    digraph = graphviz.graphs.Digraph(name=name)
    digraph.attr(label=label, compound="true", rankdir=rankdir)
    return digraph


class WorkflowGraphvizMap(ABC):
    @property
    @abstractmethod
    def parent(self) -> WorkflowGraphvizMap | None:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def label(self) -> str:
        pass

    @property
    @abstractmethod
    def graph(self) -> graphviz.graphs.Digraph:
        pass


class Channel(WorkflowGraphvizMap):
    def __init__(
            self,
            parent: _IO,
            channel: WorkflowChannel,
            shape: str = "oval",
    ):
        self.channel = channel
        self._parent = parent
        self._name = self.parent.name + self.channel.label
        self._label = self._build_label()
        self.channel: WorkflowChannel = channel

        self.graph.node(name=self.name, label=self.label, shape=shape)

    def _build_label(self):
        label = self.channel.label
        try:
            if self.channel.type_hint is not None:
                label += ": " + self.channel.type_hint.__name__
        except AttributeError:
            pass  # Signals have no type
        return label

    @property
    def parent(self) -> _IO | None:
        return self._parent

    @property
    def name(self) -> str:
        return self._name

    @property
    def label(self) -> str:
        return self._label

    @property
    def graph(self) -> graphviz.graphs.Digraph:
        return self.parent.graph


class _IO(WorkflowGraphvizMap, ABC):
    def __init__(self, parent: Node):
        self._parent = parent
        self.node = self.parent.node
        self.data_io, self.signals_io = self._get_node_io()
        self._name = self.parent.name + self.data_io.__class__.__name__
        self._label = self.data_io.__class__.__name__
        self._graph = directed_graph(self.name, self.label, rankdir="TB")

        self.channels = [
            Channel(self, channel, shape="cds") for channel in self.signals_io
        ] + [
            Channel(self, channel, shape="oval") for channel in self.data_io
        ]

        self.parent.graph.subgraph(self.graph)

    @abstractmethod
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        pass

    @property
    def parent(self) -> Node:
        return self._parent

    @property
    def name(self) -> str:
        return self._name

    @property
    def label(self) -> str:
        return self._label

    @property
    def graph(self) -> graphviz.graphs.Digraph:
        return self._graph

    def __len__(self):
        return len(self.channels)


class Inputs(_IO):
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        return self.node.inputs, self.node.signals.input


class Outputs(_IO):
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        return self.node.outputs, self.node.signals.output


class Node(WorkflowGraphvizMap):
    def __init__(
            self,
            node: WorkflowNode,
            parent: Optional[Node] = None,
            granularity: int = 0,
    ):
        self.node = node
        self._parent = parent
        self._name = self.build_node_name()
        self._label = self.node.label + ": " + self.node.__class__.__name__
        self._graph = directed_graph(self.name, self.label, rankdir="LR")

        self.inputs = Inputs(self)
        self.outputs = Outputs(self)
        self.graph.edge(
            self.inputs.channels[0].name,
            self.outputs.channels[0].name,
            style="invis"
        )

        if granularity > 0:
            try:
                self.nodes = [
                    Node(node, self, granularity - 1)
                    for node in self.node.nodes.values()
                ]
            except AttributeError:
                # Only composite nodes have their own nodes attribute
                self.nodes = []

        # TODO: Connect nodes
        # Nodes have channels, channels have channel, channel has connections
        # TODO: Map nodes IO to IO

        if self.parent is not None:
            self.parent.graph.subgraph(self.graph)

    def build_node_name(self, suffix=""):
        if self.parent is not None:
            # Recursively prepend parent labels to get a totally unique label string
            # (inside the scope of this graph)
            return self.parent.build_node_name(suffix=suffix + self.node.label)
        else:
            return "cluster" + self.node.label + suffix

    @property
    def parent(self) -> Node | None:
        return self._parent

    @property
    def name(self) -> str:
        return self._name

    @property
    def label(self) -> str:
        return self._label

    @property
    def graph(self) -> graphviz.graphs.Digraph:
        return self._graph
