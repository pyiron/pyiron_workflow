"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from time import sleep
from typing import TYPE_CHECKING

import typeguard
from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.create import HasCreator
from pyiron_workflow.mixin.lexical import LexicalParent
from pyiron_workflow.node import Node
from pyiron_workflow.topology import (
    get_nodes_in_data_tree,
    set_run_connections_according_to_dag,
    set_run_connections_according_to_linear_dag,
)

if TYPE_CHECKING:
    from pyiron_workflow.channels import (
        InputSignal,
        OutputSignal,
    )
    from pyiron_workflow.storage import BackendIdentifier, StorageInterface


def _get_graph_as_dict(composite: Composite) -> dict:
    if not isinstance(composite, Composite):
        return composite
    return {
        "object": composite,
        "nodes": {n.full_label: _get_graph_as_dict(n) for n in composite},
        "edges": {
            "data": {
                (out.full_label, inp.full_label): (out, inp)
                for n in composite
                for out in n.outputs
                for inp in out.connections
            },
            "signal": {
                (out.full_label, inp.full_label): (out, inp)
                for n in composite
                for out in n.signals.output
                for inp in out.connections
            },
        },
    }


class FailedChildError(RuntimeError):
    """Raise when one or more child nodes raise exceptions."""


class Composite(LexicalParent[Node], HasCreator, Node, ABC):
    """
    A base class for nodes that have internal graph structure -- i.e. they hold a
    collection of child nodes and their computation is to execute that graph.

    Attributes:
        strict_naming (bool): When true, repeated assignment of a new node to an
         existing node label will raise an error, otherwise the label gets appended
         with an index and the assignment proceeds. (Default is true: disallow assigning
         to existing labels.)
        create (Creator): A tool for adding new nodes to this subgraph.
        provenance_by_completion (list[str]): The child nodes (by label) in the order
            that they completed on the last :meth:`run` call.
        provenance_by_execution (list[str]): The child nodes (by label) in the order
            that they started executing on the last :meth:`run` call.
        running_children (list[str]): The names of children who are currently running.
        signal_queue (list[tuple[OutputSignal, InputSignal]]): Pending signal event
            pairs from child execution flow connections.
        starting_nodes (None | list[pyiron_workflow.node.Node]): A subset
         of the owned nodes to be used on running. Only necessary if the execution graph
         has been manually specified with `run` signals. (Default is an empty list.)
        wrap (Wrappers): A tool for accessing node-creating decorators

    """

    def __init__(
        self,
        *args,
        label: str | None = None,
        parent: Composite | None = None,
        delete_existing_savefiles: bool = False,
        autoload: BackendIdentifier | StorageInterface | None = None,
        autorun: bool = False,
        checkpoint: BackendIdentifier | StorageInterface | None = None,
        strict_naming: bool = True,
        **kwargs,
    ):
        self.starting_nodes: list[Node] = []
        self.provenance_by_execution: list[str] = []
        self.provenance_by_completion: list[str] = []
        self.running_children: list[str] = []
        self.signal_queue: list[tuple[OutputSignal, InputSignal]] = []
        self._child_sleep_interval = 0.01  # How long to wait when the signal_queue is
        # empty but the running_children list is not

        super().__init__(
            *args,
            label=label,
            parent=parent,
            delete_existing_savefiles=delete_existing_savefiles,
            autoload=autoload,
            autorun=autorun,
            checkpoint=checkpoint,
            strict_naming=strict_naming,
            **kwargs,
        )

    @classmethod
    def child_type(cls) -> type[Node]:
        return Node

    def activate_strict_hints(self):
        """Recursively activate strict type hints."""
        super().activate_strict_hints()
        for node in self:
            node.activate_strict_hints()

    def deactivate_strict_hints(self):
        """Recursively de-activate strict type hints."""
        super().deactivate_strict_hints()
        for node in self:
            node.deactivate_strict_hints()

    @property
    def use_cache(self) -> bool:
        """
        Composite nodes determine the cache usage by the cache usage of all children
        recursively.

        Setting this property at the composite level sets it for all children
        recursively.
        """
        return all(c.use_cache for c in self.children.values())

    @use_cache.setter
    def use_cache(self, value: bool):
        for c in self.children.values():
            c.use_cache = value

    @property
    def cache_hit(self) -> bool:
        return not any(c.running for c in self.children.values()) and super().cache_hit

    def _on_cache_miss(self) -> None:
        super()._on_cache_miss()
        # Reset provenance and run status trackers
        self.provenance_by_execution = []
        self.provenance_by_completion = []
        self.running_children = [n.label for n in self if n.running]
        self.signal_queue = []

    def _on_run(self):
        if len(self.running_children) > 0:  # Start from a broken process
            for label in self.running_children:
                if self.children[label]._is_using_wrapped_excutorlib_executor():
                    self.running_children.remove(label)
                self.children[label].run()
                # Running children will find serialized result and proceed,
                # or raise an error because they're already running
        else:  # Start fresh
            for node in self.starting_nodes:
                node.run()

        self._run_while_children_or_signals_exist()

        return self

    def _run_while_children_or_signals_exist(self):
        errors = {}
        while len(self.running_children) > 0 or len(self.signal_queue) > 0:
            try:
                firing, receiving = self.signal_queue.pop(0)
                try:
                    receiving(firing)
                except Exception as e:
                    errors[receiving.full_label] = e
            except IndexError:
                # The signal queue is empty, but there is still someone running...
                sleep(self._child_sleep_interval)

        if len(errors) == 1:
            raise FailedChildError(
                f"{self.full_label} encountered error in child: {errors}"
            ) from next(iter(errors.values()))
        elif len(errors) > 1:
            raise FailedChildError(
                f"{self.full_label} encountered multiple errors in children: {errors}"
            ) from None

    def register_child_starting(self, child: Node) -> None:
        """
        To be called by children when they start their run cycle.

        Args:
            child [Node]: The child that is finished and would like to fire its `ran`
                signal. Should always be a child of `self`, but this is not explicitly
                verified at runtime.
        """
        self.provenance_by_execution.append(child.label)
        self.running_children.append(child.label)

    def register_child_finished(self, child: Node) -> None:
        """
        To be called by children when they are finished their run.

        Args:
            child [Node]: The child that is finished and would like to fire its `ran`
                signal. Should always be a child of `self`, but this is not explicitly
                verified at runtime.
        """
        try:
            self.running_children.remove(child.label)
            self.provenance_by_completion.append(child.label)
        except ValueError as e:
            raise KeyError(
                f"No element {child.label} to remove while {self.running_children}, "
                f"{self.provenance_by_execution}, {self.provenance_by_completion}"
            ) from e

    def register_child_emitting(self, child: Node) -> None:
        """
        To be called by children when they want to emit their signals.

        Args:
            child [Node]: The child that is finished and would like to fire its `ran`
                signal (and possibly others). Should always be a child of `self`, but
                this is not explicitly verified at runtime.
        """
        for firing in child.emitting_channels:
            for receiving in firing.connections:
                self.signal_queue.append((firing, receiving))

    @property
    def run_args(self) -> tuple[tuple, dict]:
        return (), {}

    def process_run_result(self, run_output):
        if run_output is not self:
            self._parse_remotely_executed_self(run_output)
        return self._outputs_to_run_return()

    def _parse_remotely_executed_self(self, other_self):
        # Un-parent existing nodes before ditching them
        for node in self:
            node._parent = None
            node._detached_parent_path = None
        other_self.running = False  # It's done now
        state = self._get_state_from_remote_other(other_self)
        self.__setstate__(state)

    def _get_state_from_remote_other(self, other_self):
        state = other_self.__getstate__()
        state.pop("_executor")  # Got overridden to None for __getstate__, so keep local
        state.pop("_parent")  # Got overridden to None for __getstate__, so keep local
        state.pop("_detached_parent_path")
        return state

    def disconnect_run(self) -> list[tuple[InputSignal, OutputSignal]]:
        """
        Disconnect all `signals.input.run` connections on all child nodes.

        Returns:
            list[tuple[InputSignal, OutputSignal]]: Any disconnected pairs.
        """
        disconnected_pairs = []
        for node in self.children.values():
            disconnected_pairs.extend(node.signals.disconnect_run())
        return disconnected_pairs

    def set_run_signals_to_dag_execution(self):
        """
        Disconnects all `signals.input.run` connections among children and attempts to
        reconnect these according to the DAG flow of the data. On success, sets the
        starting nodes to just be the upstream-most node in this linear DAG flow.
        """
        if len(self.children) > 0:
            _, upstream_most_nodes = set_run_connections_according_to_dag(self.children)
            self.starting_nodes = upstream_most_nodes

    def add_child(
        self,
        child: Node,
        label: str | None = None,
        strict_naming: bool | None = None,
    ) -> Node:
        """Add the node instance to this subgraph."""
        self.clear_cache()  # Reset cache after graph change
        return super().add_child(child, label=label, strict_naming=strict_naming)

    def push_child(self, child: Node | str, *args, **kwargs):
        """
        Run a child node in a "push" configuration.

        Args:
            child (Node|str): The child node to push.
            *args: Additional positional arguments passed to the child node.
            **kwargs: Additional keyword arguments passed to the child node.

        Returns:
            (Any | Future): The result of running the node, or a futures object (if
                running on an executor).
        """
        typeguard.check_type(child, Node | str)

        problem: str | None = None
        if isinstance(child, Node):
            if child.parent is not self:
                problem = child.full_label
            else:
                child_node = child
        elif isinstance(child, str):
            if child not in self.child_labels:
                problem = child
            else:
                child_node = self.children[child]
        if problem is not None:
            raise ValueError(
                f"Child {problem} not found among {self.full_label}'s children: "
                f"{self.child_labels}"
            )
        return child_node.run(*args, **kwargs)

    def remove_child(self, child: Node | str) -> Node:
        """
        Remove a child from the :attr:`children` collection, disconnecting it and
        setting its :attr:`parent` to None.

        Args:
            child (Node|str): The child (or its label) to remove.

        Returns:
            (Node): The (now disconnected and de-parented) (former) child node.
        """
        child = super().remove_child(child)
        child.disconnect()
        if child in self.starting_nodes:
            self.starting_nodes.remove(child)
        self.clear_cache()  # Reset cache after graph change
        return child

    def executor_shutdown(self, wait=True, *, cancel_futures=False):
        """
        Invoke shutdown on the executor (if present), and recursively invoke shutdown
        for children.
        """
        super().executor_shutdown(wait=wait, cancel_futures=cancel_futures)
        for node in self:
            node.executor_shutdown(wait=wait, cancel_futures=cancel_futures)

    def __setattr__(self, key: str, node: Node):
        if isinstance(node, Composite) and key in ["_parent", "parent"]:
            # This is an edge case for assigning a node to an attribute
            super().__setattr__(key, node)
        elif isinstance(node, Node):
            self.add_child(node, label=key)
        else:
            super().__setattr__(key, node)

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.brown

    @property
    def graph_as_dict(self) -> dict:
        """
        A nested dictionary representation of the computation graph using full labels
        as keys and objects as values.
        """
        return _get_graph_as_dict(self)

    def _get_connections_as_strings(
        self, panel_getter: Callable
    ) -> list[tuple[tuple[str, str], tuple[str, str]]]:
        """
        Connections between children in string representation based on labels.

        The string representation helps storage, and having it as a property ensures
        the name is protected.
        """
        return [
            ((inp.owner.label, inp.label), (out.owner.label, out.label))
            for child in self
            for inp in panel_getter(child)
            for out in inp.connections
        ]

    @staticmethod
    def _get_data_inputs(node: Node):
        return node.inputs

    @staticmethod
    def _get_signals_input(node: Node):
        return node.signals.input

    @property
    def _child_data_connections(self) -> list[tuple[tuple[str, str], tuple[str, str]]]:
        return self._get_connections_as_strings(self._get_data_inputs)

    @property
    def _child_signal_connections(
        self,
    ) -> list[tuple[tuple[str, str], tuple[str, str]]]:
        return self._get_connections_as_strings(self._get_signals_input)

    @property
    def _starting_node_labels(self):
        # As a property so it appears in `__dir__` and thus is guaranteed to not
        # conflict with a child node name in the state
        return tuple(n.label for n in self.starting_nodes)

    def __getstate__(self):
        state = super().__getstate__()
        # Store connections as strings
        state["_child_data_connections"] = self._child_data_connections
        state["_child_signal_connections"] = self._child_signal_connections

        # Also remove the starting node instances
        del state["starting_nodes"]
        state["_starting_node_labels"] = self._starting_node_labels

        return state

    def __setstate__(self, state):
        # Purge child connection info from the state
        child_data_connections = state.pop("_child_data_connections")
        child_signal_connections = state.pop("_child_signal_connections")
        # Restore starting nodes
        state["starting_nodes"] = [
            state[label] for label in state.pop("_starting_node_labels")
        ]

        super().__setstate__(state)

        # Nodes don't store connection information, so restore it to them
        self._restore_data_connections_from_strings(child_data_connections)
        self._restore_signal_connections_from_strings(child_signal_connections)

    @staticmethod
    def _restore_connections_from_strings(
        nodes: dict[str, Node] | DotDict[str, Node],
        connections: list[tuple[tuple[str, str], tuple[str, str]]],
        input_panel_getter: Callable,
        output_panel_getter: Callable,
    ) -> None:
        """
        Set connections among a dictionary of nodes.

        This is useful for recreating node connections after (de)serialization of the
        individual nodes, which don't know about their connections (that information is
        the responsibility of their parent `Composite`).

        Args:
            nodes (dict[Node]): The nodes to connect.
            connections (list[tuple[tuple[str, str], tuple[str, str]]]): Connections
                among these nodes in the format ((input node label, input channel label
                ), (output node label, output channel label)).
        """
        for (inp_node, inp), (out_node, out) in connections:
            input_panel_getter(nodes[inp_node])[inp].connect(
                output_panel_getter(nodes[out_node])[out]
            )

    @staticmethod
    def _get_data_outputs(node: Node):
        return node.outputs

    @staticmethod
    def _get_signals_output(node: Node):
        return node.signals.output

    def _restore_data_connections_from_strings(
        self, connections: list[tuple[tuple[str, str], tuple[str, str]]]
    ) -> None:
        self._restore_connections_from_strings(
            self.children,
            connections,
            self._get_data_inputs,
            self._get_data_outputs,
        )

    def _restore_signal_connections_from_strings(
        self, connections: list[tuple[tuple[str, str], tuple[str, str]]]
    ) -> None:
        self._restore_connections_from_strings(
            self.children,
            connections,
            self._get_signals_input,
            self._get_signals_output,
        )

    @property
    def import_ready(self) -> bool:
        return super().import_ready and all(node.import_ready for node in self)

    def report_import_readiness(self, tabs=0, report_so_far=""):
        report = super().report_import_readiness(tabs=tabs, report_so_far=report_so_far)
        for node in self:
            report = node.report_import_readiness(tabs=tabs + 1, report_so_far=report)
        return report

    def run_data_tree_for_child(self, node: Node) -> None:
        """
        Use topological analysis to build a tree of all upstream dependencies and run
        them.

        This method is called by a child node when it needs to run its data tree and has
        a parent. The parent (this composite) handles the execution of the data tree.

        Args:
            node (Node): The child node that initiated the data tree run.
        """

        data_tree_nodes = get_nodes_in_data_tree(node)
        for n in data_tree_nodes:
            if n.executor is not None:
                raise ValueError(
                    f"Running the data tree is pull-paradigm action, and is "
                    f"incompatible with using executors. While running "
                    f"{node.full_label}, an executor request was found on "
                    f"{n.full_label}"
                )

        nodes = {n.label: n for n in data_tree_nodes}

        disconnected_pairs, starters = set_run_connections_according_to_linear_dag(
            nodes
        )
        data_tree_starters = list(set(starters).intersection(data_tree_nodes))

        original_starting_nodes = self.starting_nodes
        # We need these for state recovery later, even if we crash

        try:
            if len(data_tree_starters) > 1 or data_tree_starters[0] is not node:
                node.signals.disconnect_run()
                # Don't let anything upstream trigger _this_ node

                self.starting_nodes = data_tree_starters
                self.run()
            # Otherwise the requested node is the only one in the data tree, so there's
            # nothing upstream to run.
        finally:
            # No matter what, restore the original connections and labels afterwards
            for n in nodes.values():
                n.signals.disconnect_run()
            for c1, c2 in disconnected_pairs:
                c1.connect(c2)
            self.starting_nodes = original_starting_nodes
