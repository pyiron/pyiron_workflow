"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from time import sleep
from typing import TYPE_CHECKING, Literal

from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.create import HasCreator
from pyiron_workflow.mixin.lexical import LexicalParent
from pyiron_workflow.node import Node
from pyiron_workflow.topology import set_run_connections_according_to_dag

if TYPE_CHECKING:
    from pyiron_workflow.channels import (
        InputSignal,
        OutputSignal,
    )
    from pyiron_workflow.storage import StorageInterface


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

    Promises (in addition parent class promises):

    - The class offers access...
        - To the node-izing :mod:`pyiron_workflow` decorators
        - To a creator for other :mod:`pyiron_workflow` objects (namely nodes)
            - From the class level, this simply creates these objects
            - From the instance level, created nodes get the instance as their parent
    - Child nodes...
        - Can be added by...
            - Passing a node instance to the adding method
            - Setting the composite instance as the node's parent at node instantiation
            - Assigning a node instance as an attribute
        - Can be accessed by...
            - Attribute access using their node label
            - Attribute or item access in the child nodes collection
            - Iterating over the composite instance
        - Can be removed by method
        - Each have a unique label (within the scope of this composite)
            - WARNING: _Unless_ you go in and manually change the `.label` of a child!
        - Have no other parent
        - Can be replaced in-place with another node that has commensurate IO
        - Have their working directory nested inside the composite's
        - Are disallowed from having a label that conflicts with any of the parent's
            other methods or attributes
    - The length of a composite instance is its number of child nodes
    - Running the composite...
        - Runs the child nodes (either using manually specified execution signals, or
            leveraging a helper tool that automates this process for data DAGs --
            details are left to child classes)
        - Returns a dot-dictionary of output IO
    - Composite IO is some subset of the child nodes IO
        - Default channel labels indicate both child and child's channel labels
        - Default behaviour is to expose all unconnected child nodes' IO


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

    Methods:
        add_child(node: Node): Add the node instance to this subgraph.
        remove_child(node: Node): Break all connections the node has, remove_child it from this
         subgraph, and set its parent to `None`.
        (de)activate_strict_hints(): Recursively (de)activate strict type hints.
        replace_child(owned_node: Node | str, replacement: Node | type[Node]): Replaces an
            owned node with a new node, as long as the new node's IO is commensurate
            with the node being replaced.
    """

    def __init__(
        self,
        *args,
        label: str | None = None,
        parent: Composite | None = None,
        delete_existing_savefiles: bool = False,
        autoload: Literal["pickle"] | StorageInterface | None = None,
        autorun: bool = False,
        checkpoint: Literal["pickle"] | StorageInterface | None = None,
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
        super().activate_strict_hints()
        for node in self:
            node.activate_strict_hints()

    def deactivate_strict_hints(self):
        super().deactivate_strict_hints()
        for node in self:
            node.deactivate_strict_hints()

    def _on_run(self):
        # Reset provenance and run status trackers
        self.provenance_by_execution = []
        self.provenance_by_completion = []
        self.running_children = [n.label for n in self if n.running]
        self.signal_queue = []

        if len(self.running_children) > 0:  # Start from a broken process
            for label in self.running_children:
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
    def _run_args(self) -> tuple[tuple, dict]:
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
        state.pop("executor")  # Got overridden to None for __getstate__, so keep local
        state.pop("_parent")  # Got overridden to None for __getstate__, so keep local
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
        self._cached_inputs = None  # Reset cache after graph change
        return super().add_child(child, label=label, strict_naming=strict_naming)

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
        self._cached_inputs = None  # Reset cache after graph change
        return child

    def replace_child(
        self, owned_node: Node | str, replacement: Node | type[Node]
    ) -> tuple[Node, Node]:
        """
        Replaces a node currently owned with a new node instance.
        The replacement must not belong to any other parent or have any connections.
        The IO of the new node must be a perfect superset of the replaced node, i.e.
        channel labels need to match precisely, but additional channels may be present.
        After replacement, the new node will have the old node's connections, label,
        and belong to this composite.
        The labels are swapped, such that the replaced node gets the name of its
        replacement (which might be silly, but is useful in case you want to revert the
        change and swap the replaced node back in!)

        If replacement fails for some reason, the replacement and replacing node are
        both returned to their original state, and the composite is left unchanged.

        Args:
            owned_node (Node|str): The node to replace or its label.
            replacement (Node | type[Node]): The node or class to replace it with. (If
                a class is passed, it has all the same requirements on IO compatibility
                and simply gets instantiated.)

        Returns:
            (Node, Node): The node that got removed and the new one that replaced it.
        """
        owned_node_instance = (
            self.children[owned_node] if isinstance(owned_node, str) else owned_node
        )

        if owned_node_instance.parent is not self:
            raise ValueError(
                f"The node being replaced should be a child of this composite, but "
                f"another parent was found: {owned_node_instance.parent}"
            )

        if isinstance(replacement, Node):
            if replacement.parent is not None:
                raise ValueError(
                    f"Replacement node must have no parent, but got "
                    f"{replacement.parent}"
                )
            if replacement.connected:
                raise ValueError("Replacement node must not have any connections")
            replacement_node = replacement
        elif issubclass(replacement, Node):
            replacement_node = replacement(label=owned_node_instance.label)
        else:
            raise TypeError(
                f"Expected replacement node to be a node instance or node subclass, but "
                f"got {replacement}"
            )

        replacement_node.copy_io(
            owned_node_instance
        )  # If the replacement is incompatible, we'll
        # fail here before we've changed the parent at all. Since the replacement was
        # first guaranteed to be an unconnected orphan, there is not yet any permanent
        # damage
        is_starting_node = owned_node_instance in self.starting_nodes
        # In case the replaced node interfaces with the composite's IO, catch value
        # links
        inbound_links = [
            (
                sending_channel,
                replacement_node.inputs[sending_channel.value_receiver.label],
            )
            for sending_channel in self.inputs
            if sending_channel.value_receiver in owned_node_instance.inputs
        ]
        outbound_links = [
            (
                replacement_node.outputs[sending_channel.label],
                sending_channel.value_receiver,
            )
            for sending_channel in owned_node_instance.outputs
            if sending_channel.value_receiver in self.outputs
        ]
        self.remove_child(owned_node_instance)
        replacement_node.label, owned_node_instance.label = (
            owned_node_instance.label,
            replacement_node.label,
        )
        self.add_child(replacement_node)
        if is_starting_node:
            self.starting_nodes.append(replacement_node)
        for sending_channel, receiving_channel in inbound_links + outbound_links:
            sending_channel.value_receiver = receiving_channel

        # Clear caches
        self._cached_inputs = None
        replacement_node._cached_inputs = None

        return owned_node_instance, replacement_node

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
        elif isinstance(node, type) and issubclass(node, Node) and key in self.children:
            # When a class is assigned to an existing node, try a replacement
            self.replace_child(key, node)
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
