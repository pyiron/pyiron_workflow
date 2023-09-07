"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC
from functools import partial
from typing import Literal, Optional, TYPE_CHECKING

from pyiron_contrib.workflow.interfaces import Creator, Wrappers
from pyiron_contrib.workflow.io import Outputs, Inputs
from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.node_package import NodePackage
from pyiron_contrib.workflow.util import logger, DotDict, SeabornColors

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import Channel


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

    Specifies the required `on_run()` to call `run()` on a subset of owned nodes, i.e.
    to kick-start computation on the owned sub-graph.
    By default, `run()` will be called on all owned nodes have output connections but no
    input connections (i.e. the upstream-most nodes), but this can be overridden to
    specify particular nodes to use instead.
    The `run()` method (and `update()`, and calling the workflow) return a new
    dot-accessible dictionary of keys and values created from the composite output IO
    panel.

    Does not specify `input` and `output` as demanded by the parent class; this
    requirement is still passed on to children.

    Attributes:
        nodes (DotDict[pyiron_contrib.workflow.node,Node]): The owned nodes that
         form the composite subgraph.
        strict_naming (bool): When true, repeated assignment of a new node to an
         existing node label will raise an error, otherwise the label gets appended
         with an index and the assignment proceeds. (Default is true: disallow assigning
         to existing labels.)
        create (Creator): A tool for adding new nodes to this subgraph.
        upstream_nodes (list[pyiron_contrib.workflow.node,Node]): All the owned
         nodes that have output connections but no input connections, i.e. the
         upstream-most nodes.
        starting_nodes (None | list[pyiron_contrib.workflow.node,Node]): A subset
         of the owned nodes to be used on running. (Default is None, running falls back
         on using the `upstream_nodes`.)
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
        inputs_map: Optional[dict] = None,
        outputs_map: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(*args, label=label, parent=parent, **kwargs)
        self.strict_naming: bool = strict_naming
        self.inputs_map = inputs_map
        self.outputs_map = outputs_map
        self.nodes: DotDict[str:Node] = DotDict()
        self.starting_nodes: None | list[Node] = None
        self._creator = self.create
        self.create = self._owned_creator  # Override the create method from the class

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
    def upstream_nodes(self) -> list[Node]:
        """
        A list of owned nodes that receive no input from any other owned nodes.
        """
        return [
            node for node in self.nodes.values() if not self.connects_to_input_of(node)
        ]

    def has_locally_scoped_connection(self, node_connections: list[Channel]) -> bool:
        """
        Check whether connections are made to any (recursively) owned nodes.

        Args:
            node_connections [list[Channel]]: A list of connections.

        Returns:
            (bool): Whether or not any of those connections are locally scoped to the
                nodes owned by this composite node.
        """
        return len(
            set([connection.node for connection in node_connections]).intersection(
                self.nodes.values()
            )
        ) > 0 or any(
            node.has_locally_scoped_connection(node_connections)
            for node in self.nodes.values()
            if isinstance(node, Composite)
        )

    def connects_to_output_of(self, node: Node) -> bool:
        """
        Checks whether the passed node receives output from any of this composite node's
        (recursively) owned nodes.
        """
        return self.has_locally_scoped_connection(
            node.outputs.connections + node.signals.output.connections
        )

    def connects_to_input_of(self, node: Node) -> bool:
        """
        Checks whether the passed node receives input from any of this composite node's
        (recursively) owned nodes.
        """
        return self.has_locally_scoped_connection(
            node.inputs.connections + node.signals.input.connections
        )

    @property
    def on_run(self):
        return self.run_graph

    @staticmethod
    def run_graph(self):
        starting_nodes = (
            self.upstream_nodes if self.starting_nodes is None else self.starting_nodes
        )
        for node in starting_nodes:
            node.run()
        return DotDict(self.outputs.to_value_dict())

    @property
    def run_args(self) -> dict:
        return {"self": self}

    def _build_io(
        self,
        io: Inputs | Outputs,
        target: Literal["inputs", "outputs"],
        key_map: dict[str, str] | None,
    ) -> Inputs | Outputs:
        key_map = {} if key_map is None else key_map
        for node in self.nodes.values():
            panel = getattr(node, target)
            for channel_label in panel.labels:
                channel = panel[channel_label]
                default_key = f"{node.label}__{channel_label}"
                try:
                    io[key_map[default_key]] = channel
                except KeyError:
                    if not channel.connected:
                        io[default_key] = channel
        return io

    def _build_inputs(self) -> Inputs:
        return self._build_io(Inputs(), "inputs", self.inputs_map)

    def _build_outputs(self) -> Outputs:
        return self._build_io(Outputs(), "outputs", self.outputs_map)

    def add(self, node: Node, label: Optional[str] = None) -> None:
        """
        Assign a node to the parent. Optionally provide a new label for that node.

        Args:
            node (pyiron_contrib.workflow.node.Node): The node to add.
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

    def remove(self, node: Node | str):
        if isinstance(node, Node):
            node.parent = None
            node.disconnect()
            del self.nodes[node.label]
        else:
            del self.nodes[node]

    def __setattr__(self, key: str, node: Node):
        if isinstance(node, Node) and key != "parent":
            self.add(node, label=key)
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
