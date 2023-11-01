"""
A submodule for getting our node classes talking nicely with an external tool for
topological analysis. Such analyses are useful for automating execution flows based on
data flow dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from toposort import toposort_flatten, CircularDependencyError

if TYPE_CHECKING:
    from pyiron_workflow.channels import SignalChannel
    from pyiron_workflow.node import Node


class CircularDataFlowError(ValueError):
    pass


def nodes_to_data_digraph(nodes: dict[str, Node]) -> dict[str, set[str]]:
    """
    Maps a set of nodes to a digraph of their data dependency in the format of label
    keys and set of label values for upstream nodes.

    Args:
        nodes (dict[str, Node]): A label-keyed dictionary of nodes to convert into a
            string-based dictionary of digraph connections based on data flow.

    Returns:
        dict[str, set[str]]: A dictionary of nodes and the nodes they depend on for
            data.

    Raises:
        CircularDataFlowError: When a node appears in its own input.
        ValueError: If the nodes do not all have the same parent.
        ValueError: If one of the nodes has an upstream data connection whose node has
            a different parent.
    """
    digraph = {}

    parent = next(iter(nodes.values())).parent  # Just grab any one
    if not all(n.parent is parent for n in nodes.values()):
        raise ValueError(
            "Nodes in a data digraph must all be siblings -- i.e. have the same "
            "`parent` attribute."
        )

    for node in nodes.values():
        node_dependencies = []
        for channel in node.inputs:
            locally_scoped_dependencies = []
            for upstream in channel.connections:
                try:
                    upstream_node = nodes[upstream.node.label]
                except KeyError as e:
                    raise KeyError(
                        f"The {channel.label} channel of {node.label} has a connection "
                        f"to {upstream.label} channel of {upstream.node.label}, but "
                        f"{upstream.node.label} was not found among nodes. All nodes "
                        f"in the data flow dependency tree must be included."
                    )
                if upstream_node is not upstream.node:
                    raise ValueError(
                        f"The {channel.label} channel of {node.label} has a connection "
                        f"to {upstream.label} channel of {upstream.node.label}, but "
                        f"that channel's node is not the same as the nodes passed "
                        f"here. All nodes in the data flow dependency tree must be "
                        f"included."
                    )
                locally_scoped_dependencies.append(upstream.node.label)
            node_dependencies.extend(locally_scoped_dependencies)
        node_dependencies = set(node_dependencies)
        if node.label in node_dependencies:
            # the toposort library has a
            # [known issue](https://gitlab.com/ericvsmith/toposort/-/issues/3)
            # That self-dependency isn't caught, so we catch it manually here.
            raise CircularDataFlowError(
                f"Detected a cycle in the data flow topology, unable to automate "
                f"the execution of non-DAGs: {node.label} appears in its own input."
            )
        digraph[node.label] = node_dependencies

    return digraph


def nodes_to_execution_order(nodes: dict[str, Node]) -> list[str]:
    """
    Given a set of nodes that all have the same parent, returns a list of corresponding
    node labels giving an execution order that guarantees the executing node always has
    data from all its upstream nodes.

    Args:
        nodes (dict[str, Node]): A label-keyed dictionary of nodes from whom to build
            an execution order based on topological analysis of data flow.

    Returns:
        (list[str]): The labels in safe execution order.

    Raises:
        ValueError: If the data dependency is not a Directed Acyclic Graph
    """
    try:
        # Topological sorting ensures that all input dependencies have been
        # executed before the node depending on them gets run
        # The flattened part is just that we don't care about topological
        # generations that are mutually independent (inefficient but easier for now)
        execution_order = toposort_flatten(nodes_to_data_digraph(nodes))
    except CircularDependencyError as e:
        raise CircularDataFlowError(
            f"Detected a cycle in the data flow topology, unable to automate the "
            f"execution of non-DAGs: cycles found among {e.data}"
        ) from e
    return execution_order


def set_run_connections_according_to_linear_dag(
    nodes: dict[str, Node]
) -> tuple[list[tuple[SignalChannel, SignalChannel]], Node]:
    """
    Given a set of nodes that all have the same parent, have no upstream data
    connections outside the nodes provided, and have acyclic data flow, disconnects all
    their `run` and `ran` signals, then sets these signals to a linear execution that
    guarantees downstream nodes are always executed after upstream nodes. Returns one
    of the upstream-most nodes.

    In the event an exception is encountered, any disconnected connections are repaired
    before it is raised.

    Args:
        nodes (dict[str, Node]): A dictionary of node labels and the node the label is
            from, whose connections will be set according to data flow.

    Returns:
        (list[tuple[SignalChannel, SignalChannel]]): Any `run`/`ran` pairs that were
            disconnected.
        (Node): The 0th node in the execution order, i.e. on that has no
            dependencies.
    """
    disconnected_pairs = []
    for node in nodes.values():
        disconnected_pairs.extend(node.signals.disconnect_run())
        disconnected_pairs.extend(node.signals.output.ran.disconnect_all())

    try:
        # This is the most primitive sort of topological exploitation we can do
        # It is not efficient if the nodes have executors and can run in parallel
        execution_order = nodes_to_execution_order(nodes)

        for i, label in enumerate(execution_order[:-1]):
            next_node = execution_order[i + 1]
            nodes[label] > nodes[next_node]

        return disconnected_pairs, nodes[execution_order[0]]
    except Exception as e:
        # Restore whatever you broke
        for c1, c2 in disconnected_pairs:
            c1.connect(c2)
        raise e


def get_nodes_in_data_tree(node: Node) -> set[Node]:
    """
    Get a set of all nodes from this one and upstream through data connections.
    """
    try:
        nodes = set([node])
        for channel in node.inputs:
            for connection in channel.connections:
                nodes = nodes.union(get_nodes_in_data_tree(connection.node))
        return nodes
    except RecursionError:
        raise CircularDataFlowError(
            f"Detected a cycle in the data flow topology, unable to automate the "
            f"execution of non-DAGs: finding the upstream nodes for {node.label} hit a "
            f"recursion error."
        )
