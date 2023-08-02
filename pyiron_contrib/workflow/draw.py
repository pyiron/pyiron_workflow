"""
Functions for drawing the graph.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal, Optional, TYPE_CHECKING

import graphviz

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import Channel as WorkflowChannel
    from pyiron_contrib.workflow.io import DataIO, SignalIO
    from pyiron_contrib.workflow.node import Node as WorkflowNode

IN_ALPHA = "66"
OUT_ALPHA = "aa"
DATA_COLOR_BASE = "#ebba34"
DATA_COLOR = {"in": DATA_COLOR_BASE + IN_ALPHA, "out": DATA_COLOR_BASE + OUT_ALPHA}
SIGNAL_COLOR_BASE = "#3452ed"
SIGNAL_COLOR = {
    "in": SIGNAL_COLOR_BASE + IN_ALPHA, "out": SIGNAL_COLOR_BASE + OUT_ALPHA
}
DATA_SHAPE = "oval"
SIGNAL_SHAPE = "cds"

IO_COLOR_OUTSIDE = "gray"
IO_COLOR_INSIDE = "white"
IO_GRADIENT_ANGLE = "0"

NODE_COLOR_START = "blue"
NODE_COLOR_END = "white"
NODE_GRADIENT_ANGLE = "90"


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
            shape: str,
            color: str = "white",
    ):
        self.channel = channel
        self._parent = parent
        self._name = self.parent.name + self.channel.label
        self._label = self._build_label()
        self.channel: WorkflowChannel = channel
        self._color = color

        self.graph.node(
            name=self.name,
            label=self.label,
            shape=shape,
            color=self.color,
            style="filled"
        )

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

    @property
    def color(self):
        return self._color


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
            color_start=self.color_start,
            color_end=self.color_end,
            gradient_angle=IO_GRADIENT_ANGLE
        )

        self.channels = [
            Channel(
                self,
                channel,
                SIGNAL_SHAPE,
                SIGNAL_COLOR[self.in_or_out]
            ) for channel in self.signals_io
        ] + [
            Channel(
                self,
                channel,
                DATA_SHAPE,
                DATA_COLOR[self.in_or_out]
            ) for channel in self.data_io
        ]

        self.parent.graph.subgraph(self.graph)

    @abstractmethod
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        pass

    @property
    @abstractmethod
    def in_or_out(self) -> Literal["in", "out"]:
        pass

    @property
    @abstractmethod
    def color_start(self) -> str:
        pass

    @property
    @abstractmethod
    def color_end(self) -> str:
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

    @property
    def in_or_out(self) -> Literal["in"]:
        return "in"

    @property
    def color_start(self) -> str:
        return IO_COLOR_OUTSIDE

    @property
    def color_end(self) -> str:
        return IO_COLOR_INSIDE


class Outputs(_IO):
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        return self.node.outputs, self.node.signals.output

    @property
    def in_or_out(self) -> Literal["out"]:
        return "out"

    @property
    def color_start(self) -> str:
        return IO_COLOR_INSIDE

    @property
    def color_end(self) -> str:
        return IO_COLOR_OUTSIDE


class Node(WorkflowGraphvizMap):
    def __init__(
            self,
            node: WorkflowNode,
            parent: Optional[Node] = None,
            granularity: int = 1
    ):
        self.node = node
        self._parent = parent
        self._name = self.build_node_name()
        self._label = self.node.label + ": " + self.node.__class__.__name__
        self._graph = directed_graph(
            self.name,
            self.label,
            rankdir="LR",
            color_start=NODE_COLOR_START,
            color_end=NODE_COLOR_END,
            gradient_angle=NODE_GRADIENT_ANGLE
        )

        self.inputs = Inputs(self)
        self.outputs = Outputs(self)
        self.graph.edge(
            self.inputs.channels[0].name,
            self.outputs.channels[0].name,
            style="invis"
        )

        if granularity > 0:
            try:
                self._connect_owned_nodes(granularity)
            except AttributeError:
                # Only composite nodes have their own nodes attribute
                pass

        if self.parent is not None:
            self.parent.graph.subgraph(self.graph)

    def _gradient_channel_color(self, start_channel, end_channel):
        return f"{start_channel.color};0.5:{end_channel.color};0.5"

    def _connect_owned_nodes(self, granularity):
        nodes = [
            Node(node, self, granularity - 1)
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
            sources: list[Channel],
            destinations: list[Channel]
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
