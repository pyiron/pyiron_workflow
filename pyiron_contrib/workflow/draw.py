"""
Functions for drawing the graph.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

import graphviz
from matplotlib.colors import to_hex, to_rgb

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import Channel as WorkflowChannel
    from pyiron_contrib.workflow.io import DataIO, SignalIO
    from pyiron_contrib.workflow.node import Node as WorkflowNode


def directed_graph(name, label, rankdir, color_start, color_end, gradient_angle):
    """A shortcut method for instantiating the type of graphviz graph we want"""
    digraph = graphviz.graphs.Digraph(name=name)
    digraph.attr(
        label=label,
        compound="true",
        rankdir=rankdir,
        style="filled",
        fillcolor=f"{color_start}:{color_end}",
        gradientangle=gradient_angle
    )
    return digraph


def blend_colours(color_a, color_b, fraction_a=0.5):
    """Blends two hex code colours together"""
    return to_hex(
        tuple(
            fraction_a * a + (1 - fraction_a) * b
            for (a, b) in zip(to_rgb(color_a), to_rgb(color_b))
        )
    )


def lighten_hex_color(color, lightness=0.7):
    """Blends the given hex code color with pure white."""
    return blend_colours("#ffffff", color, fraction_a=lightness)


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

    @property
    @abstractmethod
    def color(self) -> str:
        pass


class _Channel(WorkflowGraphvizMap, ABC):
    def __init__(self, parent: _IO, channel: WorkflowChannel):
        self.channel = channel
        self._parent = parent
        self._name = self.parent.name + self.channel.label
        self._label = self._build_label()
        self.channel: WorkflowChannel = channel

        self.graph.node(
            name=self.name,
            label=self.label,
            shape=self.shape,
            color=self.color,
            style="filled"
        )

    @property
    @abstractmethod
    def shape(self) -> str:
        pass

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


class DataChannel(_Channel):
    @property
    def color(self) -> str:
        return "#ff7f0e"

    @property
    def shape(self) -> str:
        return "oval"


class SignalChannel(_Channel):
    @property
    def color(self) -> str:
        return "#1f77b4"

    @property
    def shape(self) -> str:
        return "cds"


class _IO(WorkflowGraphvizMap, ABC):
    def __init__(self, parent: Node):
        self._parent = parent
        self.node = self.parent.node
        self.data_io, self.signals_io = self._get_node_io()
        self._name = self.parent.name + self.data_io.__class__.__name__
        self._label = self.data_io.__class__.__name__
        self._graph = directed_graph(
            self.name,
            self.label,
            rankdir="TB",
            color_start=self.color,
            color_end=lighten_hex_color(self.color),
            gradient_angle=self.gradient_angle
        )

        self.channels = [
            SignalChannel(self, channel) for channel in self.signals_io
        ] + [
            DataChannel(self, channel) for channel in self.data_io
        ]

        self.parent.graph.subgraph(self.graph)

    @abstractmethod
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        pass

    @property
    @abstractmethod
    def gradient_angle(self) -> str:
        """Background fill colour angle in degrees"""
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

    @property
    def color(self) -> str:
        return "#7f7f7f"

    def __len__(self):
        return len(self.channels)


class Inputs(_IO):
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        return self.node.inputs, self.node.signals.input

    @property
    def gradient_angle(self) -> str:
        return "0"


class Outputs(_IO):
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        return self.node.outputs, self.node.signals.output

    @property
    def gradient_angle(self) -> str:
        return "180"


class Node(WorkflowGraphvizMap):
    def __init__(
            self,
            node: WorkflowNode,
            parent: Optional[Node] = None,
            depth: int = 1
    ):
        self.node = node
        self._parent = parent
        self._name = self.build_node_name()
        self._label = self.node.label + ": " + self.node.__class__.__name__
        self._graph = directed_graph(
            self.name,
            self.label,
            rankdir="LR",
            color_start=self.color,
            color_end=lighten_hex_color(self.color),
            gradient_angle="90"
        )

        self.inputs = Inputs(self)
        self.outputs = Outputs(self)
        self.graph.edge(
            self.inputs.channels[0].name,
            self.outputs.channels[0].name,
            style="invis"
        )

        if depth > 0:
            try:
                self._connect_owned_nodes(depth)
            except AttributeError:
                # Only composite nodes have their own nodes attribute
                pass

        if self.parent is not None:
            self.parent.graph.subgraph(self.graph)

    def _gradient_channel_color(self, start_channel, end_channel):
        return f"{start_channel.color};0.5:{end_channel.color};0.5"

    def _connect_owned_nodes(self, depth):
        nodes = [
            Node(node, self, depth - 1)
            for node in self.node.nodes.values()
        ]
        internal_inputs = [
            channel for node in nodes for channel in node.inputs.channels
        ]
        internal_outputs = [
            channel for node in nodes for channel in node.outputs.channels
        ]

        # Loop to check for internal node output --> internal node input connections
        for output_channel in internal_outputs:
            for input_channel in internal_inputs:
                if input_channel.channel in output_channel.channel.connections:
                    self.graph.edge(
                        output_channel.name,
                        input_channel.name,
                        color=self._gradient_channel_color(
                            output_channel, input_channel
                        )
                    )

        # Loop to check for macro input --> internal node input connections
        self._connect_matching(self.inputs.channels, internal_inputs)
        # Loop to check for macro input --> internal node input connections
        self._connect_matching(internal_outputs, self.outputs.channels)

    def _connect_matching(
            self,
            sources: list[_Channel],
            destinations: list[_Channel]
    ):
        """
        Draw an edge between two graph channels whose workflow channels are the same
        """
        for source in sources:
            for destination in destinations:
                if source.channel is destination.channel:
                    self.graph.edge(
                        source.name,
                        destination.name,
                        color=self._gradient_channel_color(source, destination)
                    )

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

    @property
    def color(self) -> str:
        return self.node.color
