"""
A submodule for getting our node classes talking nicely with an external tool for
topological analysis. Such analyses are useful for automating execution flows based on
data flow dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from toposort import toposort, toposort_flatten, CircularDependencyError

if TYPE_CHECKING:
    from pyiron_workflow.channels import SignalChannel
    from pyiron_workflow.node import Node


class CircularDataFlowError(ValueError):
    # Helpful for tests, so we can make sure we're getting exactly the failure we want
    # Also lets us wrap other libraries circular dependency errors (i.e. toposort's)
    # in language that makes more sense for us
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
        node_identifiers = "\n".join([n.full_label for n in nodes.values()])
        raise ValueError(
            f"Nodes in a data digraph must all be siblings -- i.e. have the same "
            f"`parent` attribute. Some of these do not: {node_identifiers}"
        )

    for node in nodes.values():
        node_dependencies = []
        for channel in node.inputs:
            locally_scoped_dependencies = []
            for upstream in channel.connections:
                try:
                    upstream_node = nodes[upstream.owner.label]
                except KeyError as e:
                    raise KeyError(
                        f"The channel {channel.full_label} has a connection to the "
                        f"upstream channel {upstream.full_label}, but the upstream "
                        f"owner {upstream.owner.label} was not found among nodes. "
                        f"All nodes in the data flow dependency tree must be included."
                    )
                if upstream_node is not upstream.owner:
                    raise ValueError(
                        f"The channel {channel.full_label} has a connection to the "
                        f"upstream channel {upstream.full_label}, but that channel's "
                        f"node is not the same as the nodes passed  here. All nodes in "
                        f"the data flow dependency tree must be included."
                    )
                locally_scoped_dependencies.append(upstream.owner.label)
            node_dependencies.extend(locally_scoped_dependencies)
        node_dependencies = set(node_dependencies)
        if node.label in node_dependencies:
            # the toposort library has a
            # [known issue](https://gitlab.com/ericvsmith/toposort/-/issues/3)
            # That self-dependency isn't caught, so we catch it manually here.
            raise CircularDataFlowError(
                f"Detected a cycle in the data flow topology, unable to automate "
                f"the execution of non-DAGs: {node.full_label} appears in its own "
                f"input."
            )
        digraph[node.label] = node_dependencies

    return digraph


def _set_new_run_connections_with_fallback_recovery(
    connection_creator: callable[[dict[str, Node]], list[Node]], nodes: dict[str, Node]
):
    """
    Given a function that takes a dictionary of unconnected nodes, connects their
    execution graph, and returns the new starting nodes, this wrapper makes sure that
    all the initial connections are broken, that these broken connections get returned
    (if wiring new connections works) / that these broken connections get re-instated
    (if an error is encountered).
    """
    disconnected_pairs = []
    for node in nodes.values():
        disconnected_pairs.extend(node.signals.disconnect_run())
        disconnected_pairs.extend(node.signals.output.ran.disconnect_all())

    try:
        return disconnected_pairs, connection_creator(nodes)
    except Exception as e:
        # Restore whatever you broke
        for c1, c2 in disconnected_pairs:
            c1.connect(c2)
        raise e


def _raise_wrapped_circular_error(e):
    """
    A convenience wrapper that converts toposort's circular dependency error into
    language that makes more sense in our context.
    """
    raise CircularDataFlowError(
        f"Detected a cycle in the data flow topology, unable to automate the "
        f"execution of non-DAGs: cycles found among {e.data}"
    ) from e


def _set_run_connections_according_to_linear_dag(nodes: dict[str, Node]) -> list[Node]:
    """
    This is the most primitive sort of topological exploitation we can do.
    It is not efficient if the nodes have executors and can run in parallel.
    """
    try:
        execution_order = toposort_flatten(nodes_to_data_digraph(nodes))
    except CircularDependencyError as e:
        _raise_wrapped_circular_error(e)

    for i, label in enumerate(execution_order[:-1]):
        next_node = execution_order[i + 1]
        nodes[label] >> nodes[next_node]

    return [nodes[execution_order[0]]]


def set_run_connections_according_to_linear_dag(
    nodes: dict[str, Node]
) -> tuple[list[tuple[SignalChannel, SignalChannel]], list[Node]]:
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
        (list[Node]): The 0th node in the execution order, i.e. on that has no
            dependencies wrapped in a list.
    """
    return _set_new_run_connections_with_fallback_recovery(
        _set_run_connections_according_to_linear_dag, nodes
    )


def _set_run_connections_according_to_dag(nodes: dict[str, Node]) -> list[Node]:
    """
    More sophisticated sorting, so that each node has an "and" execution dependency on
    all its directly-upstream data dependencies.
    """
    try:
        execution_layer_sets = list(toposort(nodes_to_data_digraph(nodes)))
        # Note: toposort only catches circular dependency errors after walking through
        #       everything in the generator, so we need to force the generator into a
        #       list to ensure that we catch these
    except CircularDependencyError as e:
        _raise_wrapped_circular_error(e)

    for node in nodes.values():
        upstream_connections = [con for inp in node.inputs for con in inp.connections]
        upstream_nodes = set([c.owner for c in upstream_connections])
        upstream_rans = [n.signals.output.ran for n in upstream_nodes]
        node.signals.input.accumulate_and_run.connect(*upstream_rans)
    # Note: We can be super fast-and-loose here because the `nodes_to_data_digraph` call
    #       above did all our safety checks for us

    return [nodes[label] for label in execution_layer_sets[0]]


def set_run_connections_according_to_dag(
    nodes: dict[str, Node]
) -> tuple[list[tuple[SignalChannel, SignalChannel]], list[Node]]:
    """
    Given a set of nodes that all have the same parent, have no upstream data
    connections outside the nodes provided, and have acyclic data flow, disconnects all
    their `run` and `ran` signals, then sets these signals so that each node has its
    accumulating `and_run` signals connected to all of its up-data-stream dependencies.
    Returns the upstream-most nodes that have no data dependencies.

    In the event an exception is encountered, any disconnected connections are repaired
    before it is raised.

    Args:
        nodes (dict[str, Node]): A dictionary of node labels and the node the label is
            from, whose connections will be set according to data flow.

    Returns:
        (list[tuple[SignalChannel, SignalChannel]]): Any `run`/`ran` pairs that were
            disconnected.
        (list[Node]): The upstream-most nodes, i.e. those that have no dependencies.
    """
    return _set_new_run_connections_with_fallback_recovery(
        _set_run_connections_according_to_dag, nodes
    )


def get_nodes_in_data_tree(node: Node) -> set[Node]:
    """
    Get a set of all nodes from this one and upstream through data connections.
    """
    try:
        nodes = set([node])
        for channel in node.inputs:
            for connection in channel.connections:
                nodes = nodes.union(get_nodes_in_data_tree(connection.owner))
        return nodes
    except RecursionError:
        raise CircularDataFlowError(
            f"Detected a cycle in the data flow topology, unable to automate the "
            f"execution of non-DAGs: finding the upstream nodes for {node.label} hit a "
            f"recursion error."
        )
