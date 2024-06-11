"""
Functions for drawing the graph.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal, Optional, TYPE_CHECKING

import graphviz
from pyiron_snippets.colors import SeabornColors

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel as WorkflowChannel
    from pyiron_workflow.io import DataIO, SignalIO
    from pyiron_workflow.node import Node as WorkflowNode


def directed_graph(
    name, label, rankdir, color_start, color_end, gradient_angle, size=None
):
    """A shortcut method for instantiating the type of graphviz graph we want"""
    digraph = graphviz.graphs.Digraph(name=name)
    digraph.attr(
        label=label,
        compound="true",
        rankdir=rankdir,
        style="filled",
        fillcolor=f"{color_start}:{color_end}",
        color=f"{color_start}:{color_end}",
        gradientangle=gradient_angle,
        fontname="helvetica",
        size=size,
    )
    return digraph


def reverse_rankdir(rankdir: Literal["LR", "TB"]):
    if rankdir == "LR":
        return "TB"
    elif rankdir == "TB":
        return "LR"
    else:
        raise ValueError(f"Expected rankdir of 'LR' or 'TB' but got {rankdir}")


def _to_hex(rgb: tuple[int, int, int]) -> str:
    """RGB [0,255] to hex color codes; no alpha values."""
    return "#{:02x}{:02x}{:02x}".format(*tuple(int(c) for c in rgb))


def _to_rgb(hex_: str) -> tuple[int, int, int]:
    """Hex to RGB color codes; no alpha values."""
    hex_ = hex_.lstrip("#")
    return tuple(int(hex_[i : i + 2], 16) for i in (0, 2, 4))


def blend_colours(color_a, color_b, fraction_a=0.5):
    """Blends two hex code colours together"""
    return _to_hex(
        tuple(
            fraction_a * a + (1 - fraction_a) * b
            for (a, b) in zip(_to_rgb(color_a), _to_rgb(color_b))
        )
    )


def lighten_hex_color(color, lightness=0.7):
    """Blends the given hex code color with pure white."""
    return blend_colours(SeabornColors.white, color, fraction_a=lightness)


class WorkflowGraphvizMap(ABC):
    """
    A parent class defining the interface for the graphviz representation of all our
    workflow objects.
    """

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
    """
    An abstract representation for channel objects, which are "nodes" in graphviz
    parlance.
    """

    def __init__(self, parent: _IO, channel: WorkflowChannel, local_name: str):
        self.channel = channel
        self._parent = parent
        self._name = self.parent.name + local_name
        self._label = local_name + self._build_label_suffix()
        self.channel: WorkflowChannel = channel

        self.graph.node(
            name=self.name,
            label=self.label,
            shape=self.shape,
            color=self.color,
            style="filled",
            fontname="helvetica",
        )

    @property
    @abstractmethod
    def shape(self) -> str:
        pass

    def _build_label_suffix(self):
        suffix = ""
        try:
            if self.channel.type_hint is not None:
                suffix += ": " + self.channel.type_hint.__name__
        except AttributeError:
            pass  # Signals have no type
        return suffix

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
        orange = "#EDB22C"
        return orange

    @property
    def shape(self) -> str:
        return "oval"


class SignalChannel(_Channel):
    @property
    def color(self) -> str:
        blue = "#21BFD8"
        return blue

    @property
    def shape(self) -> str:
        return "cds"


class _IO(WorkflowGraphvizMap, ABC):
    """
    An abstract class for IO panels, which are represented as a "subgraph" in graphviz
    parlance.
    """

    def __init__(self, parent: Node):
        self._parent = parent
        self.node: WorkflowNode = self.parent.node
        self.data_io, self.signals_io = self._get_node_io()
        self._name = self.parent.name + self.data_io.__class__.__name__
        self._label = self.data_io.__class__.__name__
        self._graph = directed_graph(
            self.name,
            self.label,
            rankdir=reverse_rankdir(self.parent.rankdir),
            color_start=self.color,
            color_end=lighten_hex_color(self.color),
            gradient_angle=self.gradient_angle,
        )

        self.channels = [
            SignalChannel(self, channel, panel_label)
            for panel_label, channel in self.signals_io.items()
        ] + [
            DataChannel(self, channel, panel_label)
            for panel_label, channel in self.data_io.items()
        ]

        self.parent.graph.subgraph(self.graph)

    @abstractmethod
    def _get_node_io(self) -> tuple[DataIO, SignalIO]:
        pass

    @property
    @abstractmethod
    def gradient_angle(self) -> str:
        """Background fill colour angle in degrees"""

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
        gray = "#A5A4A5"
        return gray

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
    """
    A wrapper class to connect graphviz to our workflow nodes. The nodes are
    represented by a "graph" or "subgraph" in graphviz parlance (depending on whether
    the node being visualized is the top-most node or not).

    Visualized nodes show their label and type, and IO panels with label and type.
    Colors and shapes are exploited to differentiate various node classes, input/output,
    and data/signal channels.

    If the node is composite in nature and the `depth` argument is at least `1`, owned
    children are also visualized (recursively with `depth = depth - 1`) inside the scope
    of this node.

    Args:
        node (pyiron_workflow.node.Node): The node to visualize.
        parent (Optional[pyiron_workflow.draw.Node]): The visualization that
            owns this visualization (if any).
        depth (int): How deeply to decompose any child nodes beyond showing their IO.
        rankdir ("LR" | "TB"): Use left-right or top-bottom graphviz `rankdir`.
        size (tuple[int | float, int | float] | None): The size of the diagram, in
            inches(?); respects ratio by scaling until at least one dimension matches
            the requested size. (Default is None, automatically size.)
    """

    def __init__(
        self,
        node: WorkflowNode,
        parent: Optional[Node] = None,
        depth: int = 1,
        rankdir: Literal["LR", "TB"] = "LR",
        size: Optional[str] = None,
    ):
        self.node = node
        self._parent = parent
        self._name = self.build_node_name()
        self._label = self.node.label + ": " + self.node.__class__.__name__
        self.rankdir: Literal["LR", "TB"] = rankdir
        self._graph = directed_graph(
            self.name,
            self.label,
            rankdir=self.rankdir,
            color_start=lighten_hex_color(self.color),
            color_end=lighten_hex_color(self.color),
            gradient_angle="0",
            size=size,
        )

        self.inputs = Inputs(self)
        self.outputs = Outputs(self)
        self.graph.edge(
            self.inputs.channels[0].name, self.outputs.channels[0].name, style="invis"
        )

        if depth > 0:
            from pyiron_workflow.nodes.composite import Composite

            # Janky in-line import to avoid circular imports but only look for children
            # where they exist (since nodes sometimes now actually do something on
            # failed attribute access, i.e. use it as delayed access on their output)
            if isinstance(self.node, Composite):
                self._connect_owned_nodes(depth)

        if self.parent is not None:
            self.parent.graph.subgraph(self.graph)

    def _channel_bicolor(self, start_channel, end_channel):
        return f"{start_channel.color};0.5:{end_channel.color};0.5"

    def _connect_owned_nodes(self, depth):
        nodes = [Node(node, self, depth - 1) for node in self.node.children.values()]
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
                        color=self._channel_bicolor(output_channel, input_channel),
                    )

        # Connect channels that are by-reference copies of each other
        # i.e. for Workflow IO to child IO
        self._connect_matching(self.inputs.channels, internal_inputs)
        self._connect_matching(internal_outputs, self.outputs.channels)

        # Connect channels that are value-linked
        # i.e. for Macro IO to child IO
        self._connect_linked(self.inputs.channels, internal_inputs)
        self._connect_linked(internal_outputs, self.outputs.channels)

    def _connect_matching(self, sources: list[_Channel], destinations: list[_Channel]):
        """
        Draw an edge between two graph channels whose workflow channels are the same
        """
        for source in sources:
            for destination in destinations:
                if source.channel is destination.channel:
                    self.graph.edge(
                        source.name,
                        destination.name,
                        color=self._channel_bicolor(source, destination),
                    )

    def _connect_linked(self, sources: list[_Channel], destinations: list[_Channel]):
        """
        Draw an edge between two graph channels values are linked
        """
        for source in sources:
            for destination in destinations:
                if (
                    hasattr(source.channel, "value_receiver")
                    and source.channel.value_receiver is destination.channel
                ):
                    self.graph.edge(
                        source.name,
                        destination.name,
                        color=self._channel_bicolor(source, destination),
                        style="dashed",
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
