"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial
from typing import Literal, Optional, TYPE_CHECKING

from bidict import bidict
from toposort import toposort_flatten, CircularDependencyError

from pyiron_workflow.interfaces import Creator, Wrappers
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.node import Node
from pyiron_workflow.node_package import NodePackage
from pyiron_workflow.util import logger, DotDict, SeabornColors

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel, InputData, OutputData


class Composite(Node, ABC):
    """
    A base class for nodes that have internal structure -- i.e. they hold a sub-graph.

    Item and attribute access is modified to give access to owned nodes.
    Adding a node with the `add` functionality or by direct attribute assignment sets
    this object as the parent of that node.

    Guarantees that each owned node is unique, and does not belong to any other parents.

    Offers a class method (`wrap_as`) to give easy access to the node-creating
    decorators.

    Offers a creator (the `create` method) which allows instantiation of other workflow
    objects.
    This method behaves _differently_ on the composite class and its instances -- on
    instances, any created nodes get their `parent` attribute automatically set to the
    composite instance being used.

    Specifies the required `on_run()` to call `run()` on a subset of owned
    `starting_nodes`nodes to kick-start computation on the owned sub-graph.
    Both the specification of these starting nodes and specifying execution signals to
    propagate execution through the graph is left to the user/child classes.
    In the case of non-cyclic workflows (i.e. DAGs in terms of data flow), both
    starting nodes and execution flow can be specified by invoking ``

    The `run()` method (and `update()`, and calling the workflow) return a new
    dot-accessible dictionary of keys and values created from the composite output IO
    panel.

    Does not specify `input` and `output` as demanded by the parent class; this
    requirement is still passed on to children.

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

    TODO:
        Wrap node registration at the class level so we don't need to do
        `X.create.register` but can just do `X.register`
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
        self.nodes: DotDict[str:Node] = DotDict()
        self.starting_nodes: list[Node] = []
        self._creator = self.create
        self.create = self._owned_creator  # Override the create method from the class

    @property
    def inputs_map(self) -> bidict | None:
        return self._inputs_map

    @inputs_map.setter
    def inputs_map(self, new_map: dict | bidict | None):
        new_map = new_map if new_map is None else bidict(new_map)
        self._inputs_map = new_map

    @property
    def outputs_map(self) -> bidict | None:
        return self._outputs_map

    @outputs_map.setter
    def outputs_map(self, new_map: dict | bidict | None):
        new_map = new_map if new_map is None else bidict(new_map)
        self._outputs_map = new_map

    @property
    def _owned_creator(self):
        """
        A misdirection so that the `create` method behaves differently on the class
        and on instances (in the latter case, created nodes should get the instance as
        their parent).
        """
        return OwnedCreator(self, self._creator)

    @property
    def executor(self) -> None:
        return None

    @executor.setter
    def executor(self, new_executor):
        if new_executor is not None:
            raise NotImplementedError(
                "Running composite nodes with an executor is not yet supported"
            )

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
        # self.nodes = run_output
        # Running on an executor will require a more sophisticated idea than above
        return DotDict(self.outputs.to_value_dict())

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
        reconnect these according to the DAG flow of the data.

        Raises:
            ValueError: When the data connections do not form a DAG.
        """
        self.disconnect_run()
        self._set_run_connections_and_starting_nodes_according_to_linear_dag()
        # TODO: Replace this linear setup with something more powerful

    def _set_run_connections_and_starting_nodes_according_to_linear_dag(self):
        # This is the most primitive sort of topological exploitation we can do
        # It is not efficient if the nodes have executors and can run in parallel
        try:
            # Topological sorting ensures that all input dependencies have been
            # executed before the node depending on them gets run
            # The flattened part is just that we don't care about topological
            # generations that are mutually independent (inefficient but easier for now)
            execution_order = toposort_flatten(self.get_data_digraph())
        except CircularDependencyError as e:
            raise ValueError(
                f"Detected a cycle in the data flow topology, unable to automate the "
                f"execution of non-DAGs: cycles found among {e.data}"
            )

        for i, label in enumerate(execution_order[:-1]):
            next_node = execution_order[i + 1]
            self.nodes[label] > self.nodes[next_node]
        self.starting_nodes = [self.nodes[execution_order[0]]]

    def get_data_digraph(self) -> dict[str, set[str]]:
        """
        Builds a directed graph of node labels based on data connections between nodes
        directly owned by this composite -- i.e. does not worry about data connections
        which are entirely internal to an owned sub-graph.

        Returns:
            dict[str, set[str]]: A dictionary of nodes and the nodes they depend on for
                data.

        Raises:
            ValueError: When a node appears in its own input.
        """
        digraph = {}

        for node in self.nodes.values():
            node_dependencies = []
            for channel in node.inputs:
                locally_scoped_dependencies = []
                for upstream in channel.connections:
                    if upstream.node.parent is self:
                        locally_scoped_dependencies.append(upstream.node.label)
                    elif channel.node.get_first_shared_parent(upstream.node) is self:
                        locally_scoped_dependencies.append(
                            upstream.node.get_parent_proximate_to(self).label
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
                except KeyError:
                    io_panel_key = default_key
                io[io_panel_key] = self._get_linking_channel(
                    channel, io_panel_key
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
        label = self._get_unique_label(node.label if label is None else label)
        self._ensure_node_is_not_duplicated(node, label)

        self.nodes[label] = node
        node.label = label
        node.parent = self
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
        node.parent = None
        disconnected = node.disconnect()
        if node in self.starting_nodes:
            self.starting_nodes.remove(node)
        del self.nodes[node.label]
        return disconnected

    def replace(self, owned_node: Node | str, replacement: Node | type[Node]):
        """
        Replaces a node currently owned with a new node instance.
        The replacement must not belong to any other parent or have any connections.
        The IO of the new node must be a perfect superset of the replaced node, i.e.
        channel labels need to match precisely, but additional channels may be present.
        After replacement, the new node will have the old node's connections, label,
        and belong to this composite.

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

        replacement.copy_io(owned_node)
        replacement.label = owned_node.label
        is_starting_node = owned_node in self.starting_nodes
        self.remove(owned_node)
        self.add(replacement)
        if is_starting_node:
            self.starting_nodes.append(replacement)

    def __setattr__(self, key: str, node: Node):
        if isinstance(node, Node) and key != "parent":
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
