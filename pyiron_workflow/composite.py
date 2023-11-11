"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial, wraps
from typing import Literal, Optional, TYPE_CHECKING

from bidict import bidict

from pyiron_workflow.interfaces import Creator, Wrappers
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.node import Node
from pyiron_workflow.node_package import NodePackage
from pyiron_workflow.topology import set_run_connections_according_to_linear_dag
from pyiron_workflow.util import logger, DotDict, SeabornColors

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel, InputData, OutputData


class Composite(Node, ABC):
    """
    A base class for nodes that have internal graph structure -- i.e. they hold a
    collection of child nodes and their computation is to execute that graph.

    Promises (in addition parent class promises):
    - The class offers access...
        - To the node-izing `pyiron_workflow` decorators
        - To a creator for other `pyiron_workflow` objects (namely nodes)
            - From the class level, this simply creates these objects
            - From the instance level, created nodes get the instance as their parent
    - Child nodes...
        - Can be added by...
            - Creating them from the creator on a composite _instance_
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
         IO exposed at the `Composite` level and the underlying channels).
        nodes (DotDict[pyiron_workflow.node.Node]): The owned nodes that
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
        add(node: Node): Add the node instance to this subgraph.
        remove(node: Node): Break all connections the node has, remove it from this
         subgraph, and set its parent to `None`.
        (de)activate_strict_hints(): Recursively (de)activate strict type hints.
        replace(owned_node: Node | str, replacement: Node | type[Node]): Replaces an
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
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
        **kwargs,
    ):
        super().__init__(*args, label=label, parent=parent, **kwargs)
        self.strict_naming: bool = strict_naming
        self._inputs_map = None
        self._outputs_map = None
        self.inputs_map = inputs_map
        self.outputs_map = outputs_map
        self.nodes: DotDict[str, Node] = DotDict()
        self.starting_nodes: list[Node] = []
        self._creator = self.create
        self.create = self._owned_creator  # Override the create method from the class

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

    @property
    def _owned_creator(self):
        """
        A misdirection so that the `create` method behaves differently on the class
        and on instances (in the latter case, created nodes should get the instance as
        their parent).
        """
        return OwnedCreator(self, self._creator)

    def to_dict(self):
        return {
            "label": self.label,
            "nodes": {n.label: n.to_dict() for n in self.nodes.values()},
        }

    @property
    def on_run(self):
        return self.run_graph

    @staticmethod
    def run_graph(_nodes: dict[Node], _starting_nodes: list[Node]):
        for node in _starting_nodes:
            node.run()
        return _nodes

    @property
    def run_args(self) -> dict:
        return {"_nodes": self.nodes, "_starting_nodes": self.starting_nodes}

    def process_run_result(self, run_output):
        if run_output is not self.nodes:
            # Then we probably ran on a parallel process and have an unpacked future
            self._update_children(run_output)
        return DotDict(self.outputs.to_value_dict())

    def _update_children(self, children_from_another_process: DotDict[str, Node]):
        """
        If you receive a new dictionary of children, e.g. from unpacking a futures
        object of your own children you sent off to another process for computation,
        replace your own nodes with them, and set yourself as their parent.
        """
        for child in children_from_another_process.values():
            child._parent = self
        self.nodes = children_from_another_process

    def disconnect_run(self) -> list[tuple[Channel, Channel]]:
        """
        Disconnect all `signals.input.run` connections on all child nodes.

        Returns:
            list[tuple[Channel, Channel]]: Any disconnected pairs.
        """
        disconnected_pairs = []
        for node in self.nodes.values():
            disconnected_pairs.extend(node.signals.disconnect_run())
        return disconnected_pairs

    def set_run_signals_to_dag_execution(self):
        """
        Disconnects all `signals.input.run` connections among children and attempts to
        reconnect these according to the DAG flow of the data. On success, sets the
        starting nodes to just be the upstream-most node in this linear DAG flow.
        """
        _, upstream_most_node = set_run_connections_according_to_linear_dag(self.nodes)
        self.starting_nodes = [upstream_most_node]

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
        for node in self.nodes.values():
            panel = getattr(node, i_or_o)
            for channel_label in panel.labels:
                channel = panel[channel_label]
                default_key = f"{node.label}__{channel_label}"
                try:
                    io_panel_key = key_map[default_key]
                    if not isinstance(io_panel_key, tuple):
                        # Tuples indicate that the channel has been deactivated
                        io[io_panel_key] = self._get_linking_channel(
                            channel, io_panel_key
                        )
                except KeyError:
                    if not channel.connected:
                        io[default_key] = self._get_linking_channel(
                            channel, default_key
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

    def add(self, node: Node, label: Optional[str] = None) -> None:
        """
        Assign a node to the parent. Optionally provide a new label for that node.

        Args:
            node (pyiron_workflow.node.Node): The node to add.
            label (Optional[str]): The label for this node.

        Raises:
            TypeError: If the
        """
        if not isinstance(node, Node):
            raise TypeError(
                f"Only new node instances may be added, but got {type(node)}."
            )
        self._ensure_node_has_no_other_parent(node)

        if not (label in self.nodes.keys() and self.nodes[label] is node):
            # Otherwise you're just passing the same node to the same key!

            label = self._get_unique_label(node.label if label is None else label)
            self._ensure_node_is_not_duplicated(node, label)

            self.nodes[label] = node
            node.label = label
            node._parent = self
        return node

    def _get_unique_label(self, label):
        if label in self.__dir__():
            if isinstance(getattr(self, label), Node):
                if self.strict_naming:
                    raise AttributeError(
                        f"{label} is already the label for a node. Please remove it "
                        f"before assigning another node to this label."
                    )
                else:
                    label = self._add_suffix_to_label(label)
            else:
                raise AttributeError(
                    f"{label} is an attribute or method of the {self.__class__} class, "
                    f"and cannot be used as a node label."
                )
        return label

    def _add_suffix_to_label(self, label):
        i = 0
        new_label = label
        while new_label in self.nodes.keys():
            new_label = f"{label}{i}"
            i += 1
        if new_label != label:
            logger.info(
                f"{label} is already a node; appending an index to the "
                f"node label instead: {new_label}"
            )
        return new_label

    def _ensure_node_has_no_other_parent(self, node: Node):
        if node.parent is not None and node.parent is not self:
            raise ValueError(
                f"The node ({node.label}) already belongs to the parent "
                f"{node.parent.label}. Please remove it there before trying to "
                f"add it to this parent ({self.label})."
            )

    def _ensure_node_is_not_duplicated(self, node: Node, label: str):
        if (
            node.parent is self
            and label != node.label
            and self.nodes[node.label] is node
        ):
            logger.info(
                f"Reassigning the node {node.label} to the label {label} when "
                f"adding it to the parent {self.label}."
            )
            del self.nodes[node.label]

    def remove(self, node: Node | str) -> list[tuple[Channel, Channel]]:
        """
        Remove a node from the `nodes` collection, disconnecting it and setting its
        `parent` to None.

        Args:
            node (Node|str): The node (or its label) to remove.

        Returns:
            (list[tuple[Channel, Channel]]): Any connections that node had.
        """
        node = self.nodes[node] if isinstance(node, str) else node
        node._parent = None
        disconnected = node.disconnect()
        if node in self.starting_nodes:
            self.starting_nodes.remove(node)
        del self.nodes[node.label]
        return disconnected

    def replace(self, owned_node: Node | str, replacement: Node | type[Node]) -> Node:
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
            owned_node = self.nodes[owned_node]

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
        self.remove(owned_node)
        replacement.label, owned_node.label = owned_node.label, replacement.label
        self.add(replacement)
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
            self.replace(replacement, owned_node)  # Guaranteed to work since
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
    def register(cls, domain: str, package_identifier: str) -> None:
        cls.create.register(domain=domain, package_identifier=package_identifier)

    def __setattr__(self, key: str, node: Node):
        if isinstance(node, Node) and key != "_parent":
            self.add(node, label=key)
        elif (
            isinstance(node, type)
            and issubclass(node, Node)
            and key in self.nodes.keys()
        ):
            # When a class is assigned to an existing node, try a replacement
            self.replace(key, node)
        else:
            super().__setattr__(key, node)

    def __getattr__(self, key):
        try:
            return self.nodes[key]
        except KeyError:
            # Raise an attribute error from getattr to make sure hasattr works well!
            raise AttributeError(
                f"Could not find attribute {key} on {self.label} "
                f"({self.__class__.__name__}) or in its nodes ({self.nodes.keys()})"
            )

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def __iter__(self):
        return self.nodes.values().__iter__()

    def __len__(self):
        return len(self.nodes)

    def __dir__(self):
        return set(super().__dir__() + list(self.nodes.keys()))

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.brown


class OwnedCreator:
    """
    A creator that overrides the `parent` arg of all accessed nodes to its own parent.

    Necessary so that `Workflow.create.Function(...)` returns an unowned function node,
    while `some_workflow_instance.create.Function(...)` returns a function node owned
    by the workflow instance.
    """

    def __init__(self, parent: Composite, creator: Creator):
        self._parent = parent
        self._creator = creator

    def __getattr__(self, item):
        value = getattr(self._creator, item)

        try:
            is_node_class = issubclass(value, Node)
        except TypeError:
            # issubclass complains if the value isn't even a class
            is_node_class = False

        if is_node_class:
            value = partial(value, parent=self._parent)
        elif isinstance(value, NodePackage):
            value = OwnedNodePackage(self._parent, value)

        return value

    def __getstate__(self):
        # Compatibility with python <3.11
        return self.__dict__

    def __setstate__(self, state):
        # Because we override getattr, we need to use __dict__ assignment directly in
        # __setstate__
        self.__dict__["_parent"] = state["_parent"]
        self.__dict__["_creator"] = state["_creator"]


class OwnedNodePackage:
    """
    A wrapper for node packages so that accessed node classes can have their parent
    value automatically filled.
    """

    def __init__(self, parent: Composite, node_package: NodePackage):
        self._parent = parent
        self._node_package = node_package

    def __getattr__(self, item):
        value = getattr(self._node_package, item)
        if issubclass(value, Node):
            value = partial(value, parent=self._parent)
        return value

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
