"""
A submodule for getting our node classes talking nicely with an external tool for
topological analysis. Such analyses are useful for automating execution flows based on
data flow dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from toposort import toposort_flatten, CircularDependencyError

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


def nodes_to_data_digraph(*nodes: Node) -> dict[str, set[str]]:
    """
    Maps a set of nodes to a digraph of their data dependency in the format of label
    keys and set of label values for upstream nodes.

    Returns:
        dict[str, set[str]]: A dictionary of nodes and the nodes they depend on for
            data.

    Raises:
        ValueError: When a node appears in its own input.
        ValueError: If the nodes do not all have the same parent.
        ValueError: If one of the nodes has an upstream data connection whose node has
            a different parent.
    """
    digraph = {}

    parent = nodes[0].parent
    if not all(n.parent is parent for n in nodes):
        raise ValueError(
            "Nodes in a data digraph must all be siblings -- i.e. have the same "
            "`parent` attribute."
        )

    for node in nodes:
        node_dependencies = []
        for channel in node.inputs:
            locally_scoped_dependencies = []
            for upstream in channel.connections:
                if upstream.node.parent is parent:
                    locally_scoped_dependencies.append(upstream.node.label)
                else:
                    raise ValueError(
                        f"Nodes in a data digraph must all be siblings, but the "
                        f"{channel.label} channel of {node.label} has a connection to "
                        f"the {upstream.label} channel of {upstream.node.label} with "
                        f"parents {node.parent} and {upstream.node.parent}, "
                        f"respectively"
                    )
            node_dependencies.extend(locally_scoped_dependencies)
        node_dependencies = set(node_dependencies)
        if node.label in node_dependencies:
            # the toposort library has a
            # [known issue](https://gitlab.com/ericvsmith/toposort/-/issues/3)
            # That self-dependency isn't caught, so we catch it manually here.
            raise ValueError(
                f"Detected a cycle in the data flow topology, unable to automate "
                f"the execution of non-DAGs: {node.label} appears in its own input."
            )
        digraph[node.label] = node_dependencies

    return digraph


def nodes_to_execution_order(*nodes: Node) -> list[str]:
    """
    Given a set of nodes that all have the same parent, returns a list of corresponding
    node labels giving an execution order that guarantees the executing node always has
    data from all its upstream nodes.

    Args:
        *nodes (Node): The nodes whose data topology to analyze

    Returns:
        (list[str]): The labels in safe execution order.

    Raises:
        CircularDependencyError: If the data dependency is not a Directed Acyclic Graph
    """
    try:
        # Topological sorting ensures that all input dependencies have been
        # executed before the node depending on them gets run
        # The flattened part is just that we don't care about topological
        # generations that are mutually independent (inefficient but easier for now)
        execution_order = toposort_flatten(
            nodes_to_data_digraph(*nodes)
        )
    except CircularDependencyError as e:
        raise ValueError(
            f"Detected a cycle in the data flow topology, unable to automate the "
            f"execution of non-DAGs: cycles found among {e.data}"
        )
    return execution_order
