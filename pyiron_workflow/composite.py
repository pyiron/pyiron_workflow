"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import wraps
from typing import Literal, Optional, TYPE_CHECKING

from bidict import bidict

from pyiron_workflow.create import Creator, Wrappers
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.node import Node
from pyiron_workflow.node_package import NodePackage
from pyiron_workflow.semantics import SemanticParent
from pyiron_workflow.topology import set_run_connections_according_to_dag
from pyiron_workflow.snippets.colors import SeabornColors
from pyiron_workflow.snippets.dotdict import DotDict

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel, InputData, OutputData


class Composite(Node, SemanticParent, ABC):
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
    - Composite IO...
        - Is some subset of the child nodes IO
            - Default channel labels indicate both child and child's channel labels
            - Default behaviour is to expose all unconnected child nodes' IO
        - Bijective maps can be used to...
            - Rename IO
            - Force a child node's IO to appear
            - Force a child node's IO to _not_ appear

    Attributes:
        inputs/outputs_map (bidict|None): Maps in the form
         `{"node_label__channel_label": "some_better_name"}` that expose canonically
         named channels of child nodes under a new name. This can be used both for re-
         naming regular IO (i.e. unconnected child channels), as well as forcing the
         exposure of irregular IO (i.e. child channels that are already internally
         connected to some other child channel). Non-`None` values provided at input
         can be in regular dictionary form, but get re-cast as a clean bidict to ensure
         the bijective nature of the maps (i.e. there is a 1:1 connection between any
         IO exposed at the :class:`Composite` level and the underlying channels).
        children (bidict.bidict[pyiron_workflow.node.Node]): The owned nodes that
         form the composite subgraph.
        strict_naming (bool): When true, repeated assignment of a new node to an
         existing node label will raise an error, otherwise the label gets appended
         with an index and the assignment proceeds. (Default is true: disallow assigning
         to existing labels.)
        create (Creator): A tool for adding new nodes to this subgraph.
        starting_nodes (None | list[pyiron_workflow.node.Node]): A subset
         of the owned nodes to be used on running. Only necessary if the execution graph
         has been manually specified with `run` signals. (Default is an empty list.)
        wrap_as (Wrappers): A tool for accessing node-creating decorators

    Methods:
        add_child(node: Node): Add the node instance to this subgraph.
        remove_child(node: Node): Break all connections the node has, remove_child it from this
         subgraph, and set its parent to `None`.
        (de)activate_strict_hints(): Recursively (de)activate strict type hints.
        replace_child(owned_node: Node | str, replacement: Node | type[Node]): Replaces an
            owned node with a new node, as long as the new node's IO is commensurate
            with the node being replaced.
        register(): A short-cut to registering a new node package with the node creator.
    """

    wrap_as = Wrappers()
    create = Creator()

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            label=label,
            parent=parent,
            save_after_run=save_after_run,
            storage_backend=storage_backend,
            strict_naming=strict_naming,
            **kwargs,
        )
        self._inputs_map = None
        self._outputs_map = None
        self.inputs_map = inputs_map
        self.outputs_map = outputs_map
        self.starting_nodes: list[Node] = []

    @property
    def inputs_map(self) -> bidict | None:
        self._deduplicate_nones(self._inputs_map)
        return self._inputs_map

    @inputs_map.setter
    def inputs_map(self, new_map: dict | bidict | None):
        self._deduplicate_nones(new_map)
        if new_map is not None:
            new_map = bidict(new_map)
        self._inputs_map = new_map

    @property
    def outputs_map(self) -> bidict | None:
        self._deduplicate_nones(self._outputs_map)
        return self._outputs_map

    @outputs_map.setter
    def outputs_map(self, new_map: dict | bidict | None):
        self._deduplicate_nones(new_map)
        if new_map is not None:
            new_map = bidict(new_map)
        self._outputs_map = new_map

    @staticmethod
    def _deduplicate_nones(some_map: dict | bidict | None) -> dict | bidict | None:
        if some_map is not None:
            for k, v in some_map.items():
                if v is None:
                    some_map[k] = (None, f"{k} disabled")

    def activate_strict_hints(self):
        super().activate_strict_hints()
        for node in self:
            node.activate_strict_hints()

    def deactivate_strict_hints(self):
        super().deactivate_strict_hints()
        for node in self:
            node.deactivate_strict_hints()

    def to_dict(self):
        return {
            "label": self.label,
            "nodes": {n.label: n.to_dict() for n in self.children.values()},
        }

    @property
    def on_run(self):
        return self.run_graph

    @staticmethod
    def run_graph(_composite: Composite):
        for node in _composite.starting_nodes:
            node.run()
        return _composite

    @property
    def run_args(self) -> dict:
        return {"_composite": self}

    def process_run_result(self, run_output):
        if run_output is not self:
            self._parse_remotely_executed_self(run_output)
        return DotDict(self.outputs.to_value_dict())

    def _parse_remotely_executed_self(self, other_self):
        # Un-parent existing nodes before ditching them
        for node in self:
            node._parent = None
        other_self.running = False  # It's done now
        self.__setstate__(other_self.__getstate__())

    def disconnect_run(self) -> list[tuple[Channel, Channel]]:
        """
        Disconnect all `signals.input.run` connections on all child nodes.

        Returns:
            list[tuple[Channel, Channel]]: Any disconnected pairs.
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
        _, upstream_most_nodes = set_run_connections_according_to_dag(self.children)
        self.starting_nodes = upstream_most_nodes

    def _build_io(
        self,
        i_or_o: Literal["inputs", "outputs"],
        key_map: dict[str, str | None] | None,
    ) -> Inputs | Outputs:
        """
        Build an IO panel for exposing child node IO to the outside world at the level
        of the composite node's IO.

        Args:
            target [Literal["inputs", "outputs"]]: Whether this is I or O.
            key_map [dict[str, str]|None]: A map between the default convention for
                mapping child IO to composite IO (`"{node.label}__{channel.label}"`) and
                whatever label you actually want to expose to the composite user. Also
                allows non-standards channel exposure, i.e. exposing
                internally-connected channels (which would not normally be exposed) by
                providing a string-to-string map, or suppressing unconnected channels
                (which normally would be exposed) by providing a string-None map.

        Returns:
            (Inputs|Outputs): The populated panel.
        """
        key_map = {} if key_map is None else key_map
        io = Inputs() if i_or_o == "inputs" else Outputs()
        for node in self.children.values():
            panel = getattr(node, i_or_o)
            for channel in panel:
                try:
                    io_panel_key = key_map[channel.scoped_label]
                    if not isinstance(io_panel_key, tuple):
                        # Tuples indicate that the channel has been deactivated
                        # This is a necessary misdirection to keep the bidict working,
                        # as we can't simply map _multiple_ keys to `None`
                        io[io_panel_key] = self._get_linking_channel(
                            channel, io_panel_key
                        )
                except KeyError:
                    if not channel.connected:
                        io[channel.scoped_label] = self._get_linking_channel(
                            channel, channel.scoped_label
                        )
        return io

    @abstractmethod
    def _get_linking_channel(
        self,
        child_reference_channel: InputData | OutputData,
        composite_io_key: str,
    ) -> InputData | OutputData:
        """
        Returns the channel that will be the link between the provided child channel,
        and the composite's IO at the given key.

        The returned channel should be fully compatible with the provided child channel,
        i.e. same type, same type hint... (For instance, the child channel itself is a
        valid return, which would create a composite IO panel that works by reference.)

        Args:
            child_reference_channel (InputData | OutputData): The child channel
            composite_io_key (str): The key under which this channel will be stored on
                the composite's IO.

        Returns:
            (Channel): A channel with the same type, type hint, etc. as the reference
                channel passed in.
        """
        pass

    def _build_inputs(self) -> Inputs:
        return self._build_io("inputs", self.inputs_map)

    def _build_outputs(self) -> Outputs:
        return self._build_io("outputs", self.outputs_map)

    def add_child(
        self,
        child: Node,
        label: Optional[str] = None,
        strict_naming: Optional[bool] = None,
    ) -> Node:
        if not isinstance(child, Node):
            raise TypeError(
                f"Only new {Node.__name__} instances may be added, but got "
                f"{type(child)}."
            )
        return super().add_child(child, label=label, strict_naming=strict_naming)

    def remove_child(self, child: Node | str) -> list[tuple[Channel, Channel]]:
        """
        Remove a child from the :attr:`children` collection, disconnecting it and
        setting its :attr:`parent` to None.

        Args:
            child (Node|str): The child (or its label) to remove.

        Returns:
            (list[tuple[Channel, Channel]]): Any connections that node had.
        """
        child = super().remove_child(child)
        disconnected = child.disconnect()
        if child in self.starting_nodes:
            self.starting_nodes.remove(child)
        return disconnected

    def replace_child(
        self, owned_node: Node | str, replacement: Node | type[Node]
    ) -> Node:
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
            (Node): The node that got removed
        """
        if isinstance(owned_node, str):
            owned_node = self.children[owned_node]

        if owned_node.parent is not self:
            raise ValueError(
                f"The node being replaced should be a child of this composite, but "
                f"another parent was found: {owned_node.parent}"
            )

        if isinstance(replacement, Node):
            if replacement.parent is not None:
                raise ValueError(
                    f"Replacement node must have no parent, but got "
                    f"{replacement.parent}"
                )
            if replacement.connected:
                raise ValueError("Replacement node must not have any connections")
        elif issubclass(replacement, Node):
            replacement = replacement(label=owned_node.label)
        else:
            raise TypeError(
                f"Expected replacement node to be a node instance or node subclass, but "
                f"got {replacement}"
            )

        replacement.copy_io(owned_node)  # If the replacement is incompatible, we'll
        # fail here before we've changed the parent at all. Since the replacement was
        # first guaranteed to be an unconnected orphan, there is not yet any permanent
        # damage
        is_starting_node = owned_node in self.starting_nodes
        self.remove_child(owned_node)
        replacement.label, owned_node.label = owned_node.label, replacement.label
        self.add_child(replacement)
        if is_starting_node:
            self.starting_nodes.append(replacement)

        # Finally, make sure the IO is constructible with this new node, which will
        # catch things like incompatible IO maps
        try:
            # Make sure node-level IO is pointing to the new node and that macro-level
            # IO gets safely reconstructed
            self._rebuild_data_io()
        except Exception as e:
            # If IO can't be successfully rebuilt using this node, revert changes and
            # raise the exception
            self.replace_child(replacement, owned_node)  # Guaranteed to work since
            # replacement in the other direction was already a success
            raise e

        return owned_node

    def _rebuild_data_io(self):
        """
        Try to rebuild the IO.

        If an error is encountered, revert back to the existing IO then raise it.
        """
        old_inputs = self.inputs
        old_outputs = self.outputs
        connection_changes = []  # For reversion if there's an error
        try:
            self._inputs = self._build_inputs()
            self._outputs = self._build_outputs()
            for old, new in [(old_inputs, self.inputs), (old_outputs, self.outputs)]:
                for old_channel in old:
                    if old_channel.connected:
                        # If the old channel was connected to stuff, we'd better still
                        # have a corresponding channel and be able to copy these, or we
                        # should fail hard.
                        # But, if it wasn't connected, we don't even care whether or not
                        # we still have a corresponding channel to copy to
                        new_channel = new[old_channel.label]
                        new_channel.copy_connections(old_channel)
                        swapped_conenctions = old_channel.disconnect_all()  # Purge old
                        connection_changes.append(
                            (new_channel, old_channel, swapped_conenctions)
                        )
        except Exception as e:
            for new_channel, old_channel, swapped_conenctions in connection_changes:
                new_channel.disconnect(*swapped_conenctions)
                old_channel.connect(*swapped_conenctions)
            self._inputs = old_inputs
            self._outputs = old_outputs
            e.message = (
                f"Unable to rebuild IO for {self.label}; reverting to old IO."
                f"{e.message}"
            )
            raise e

    @classmethod
    @wraps(Creator.register)
    def register(cls, package_identifier: str, domain: Optional[str] = None) -> None:
        cls.create.register(package_identifier=package_identifier, domain=domain)

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
            # We either defer to the setter with super, or directly assign the private
            # variable (as requested in the setter)
            super().__setattr__(key, node)
        elif isinstance(node, Node):
            self.add_child(node, label=key)
        elif (
            isinstance(node, type)
            and issubclass(node, Node)
            and key in self.children.keys()
        ):
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
    def package_requirements(self) -> set[str]:
        """
        A list of node package identifiers for children.
        """
        return set(n.package_identifier for n in self)

    def to_storage(self, storage):
        storage["nodes"] = list(self.children.keys())
        for label, node in self.children.items():
            node.to_storage(storage.create_group(label))

        storage["inputs_map"] = self.inputs_map
        storage["outputs_map"] = self.outputs_map

        super().to_storage(storage)

    def from_storage(self, storage):
        from pyiron_contrib.tinybase.storage import GenericStorage

        self.inputs_map = (
            storage["inputs_map"].to_object()
            if isinstance(storage["inputs_map"], GenericStorage)
            else storage["inputs_map"]
        )
        self.outputs_map = (
            storage["outputs_map"].to_object()
            if isinstance(storage["outputs_map"], GenericStorage)
            else storage["outputs_map"]
        )
        self._rebuild_data_io()  # To apply any map that was saved

        super().from_storage(storage)

    def tidy_working_directory(self):
        for node in self:
            node.tidy_working_directory()
        super().tidy_working_directory()

    def _get_connections_as_strings(
        self, panel_getter: callable
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

        # Transform the IO maps into a datatype that plays well with h5io
        # (Bidict implements a custom reconstructor, which hurts us)
        state["_inputs_map"] = (
            None if self._inputs_map is None else dict(self._inputs_map)
        )
        state["_outputs_map"] = (
            None if self._outputs_map is None else dict(self._outputs_map)
        )

        # Also remove the starting node instances
        del state["starting_nodes"]
        state["_starting_node_labels"] = self._starting_node_labels

        return state

    def __setstate__(self, state):
        # Purge child connection info from the state
        child_data_connections = state.pop("_child_data_connections")
        child_signal_connections = state.pop("_child_signal_connections")

        # Transform the IO maps back into the right class (bidict)
        state["_inputs_map"] = (
            None if state["_inputs_map"] is None else bidict(state["_inputs_map"])
        )
        state["_outputs_map"] = (
            None if state["_outputs_map"] is None else bidict(state["_outputs_map"])
        )

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
        input_panel_getter: callable,
        output_panel_getter: callable,
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
