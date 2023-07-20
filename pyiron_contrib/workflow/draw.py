"""
Functions for drawing the graph.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

import graphviz

if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


def _channel_name(node, channel):
    return node.label + channel.label


def _channel_label(channel):
    label = channel.label
    try:
        if channel.type_hint is not None:
            label += ": " + channel.type_hint.__name__
    except AttributeError:
        pass  # Signals have no type
    return label


def _make_channel_node(parent_graph, node, channel, shape="oval"):
    parent_graph.node(
        _channel_name(node, channel),
        _channel_label(channel),
        shape=shape
    )


def _io_name(node, io):
    return "cluster" + node.label + io.__class__.__name__


def _make_io_panel(parent_graph, node, data_io, signals_io):
    with parent_graph.subgraph(name=_io_name(node, data_io)) as io_graph:
        io_graph.attr(compound="true", label=data_io.__class__.__name__, rankdir="TB")
        for data_channel in data_io:
            _make_channel_node(io_graph, node, data_channel)
        for signal_channel in signals_io:
            _make_channel_node(io_graph, node, signal_channel, shape="cds")
    return io_graph


def _node_name(node, suffix=""):
    if node.parent is not None:
        # Recursively prepend parent labels to get a totally unique label string
        # (inside the scope of this graph)
        return _node_name(node.parent, suffix=suffix + node.label)
    else:
        return "cluster" + node.label + suffix


def _node_label(node):
    return node.label + ": " + node.__class__.__name__


def draw_node(node: Node, parent_graph: Optional[graphviz.graphs.Digraph] = None):
    if parent_graph is None:
        parent_graph = graphviz.graphs.Digraph(node.label)
        parent_graph.attr(compound="true", rankdir="TB")

    with parent_graph.subgraph(name=_node_name(node)) as node_graph:
        node_graph.attr(compount="true", label=_node_label(node), rankdir="LR")

        _make_io_panel(node_graph, node, node.inputs, node.signals.input)
        _make_io_panel(node_graph, node, node.outputs, node.signals.output)

        # Make inputs and outputs groups ordered by (invisibly) drawing a connection
        # Exploit the fact that all nodes have `run` and `ran` signal channels
        node_graph.edge(
            _channel_name(node, node.signals.input[node.signals.input.labels[0]]),
            _channel_name(node, node.signals.output[node.signals.output.labels[0]]),
            ltail=_io_name(node, node.inputs),
            lhead=_io_name(node, node.outputs),
            style="invis"
        )

    return parent_graph
