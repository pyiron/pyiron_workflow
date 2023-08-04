"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC
from functools import partial
from typing import Literal, Optional
from warnings import warn

from pyiron_contrib.executors import CloudpickleProcessPoolExecutor
from pyiron_contrib.workflow.io import Outputs, Inputs
from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.function import (
    Function,
    SingleValue,
    Slow,
    function_node,
    slow_node,
    single_value_node,
)
from pyiron_contrib.workflow.node_library import atomistics, standard
from pyiron_contrib.workflow.node_library.package import NodePackage
from pyiron_contrib.workflow.util import DotDict


class _NodeDecoratorAccess:
    """An intermediate container to store node-creating decorators as class methods."""

    function_node = function_node
    slow_node = slow_node
    single_value_node = single_value_node

    _macro_node = None

    @classmethod
    @property
    def macro_node(cls):
        # This jankiness is to avoid circular imports
        # Chaining classmethod and property like this got deprecated in python 3.11,
        # but it does what I want, so I'm going to use it anyhow
        if cls._macro_node is None:
            from pyiron_contrib.workflow.macro import macro_node

            cls._macro_node = macro_node
        return cls._macro_node


class Creator:
    """A shortcut interface for creating non-Node objects from the workflow class."""

    CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor


class Composite(Node, ABC):
    """
    A base class for nodes that have internal structure -- i.e. they hold a sub-graph.

    Item and attribute access is modified to give access to owned nodes.
    Adding a node with the `add` functionality or by direct attribute assignment sets
    this object as the parent of that node.

    Guarantees that each owned node is unique, and does not belong to any other parents.

    Offers a class method (`wrap_as`) to give easy access to the node-creating
    decorators.

    Specifies the required `on_run()` to call `run()` on a subset of owned nodes, i.e.
    to kick-start computation on the owned sub-graph.
    By default, `run()` will be called on all owned nodes have output connections but no
    input connections (i.e. the upstream-most nodes), but this can be overridden to
    specify particular nodes to use instead.
    The `run()` method (and `update()`, and calling the workflow, when these result in
    a run), return a new dot-accessible dictionary of keys and values created from the
    composite output IO panel.

    Does not specify `input` and `output` as demanded by the parent class; this
    requirement is still passed on to children.

    Attributes:
        nodes (DotDict[pyiron_contrib.workflow.node,Node]): The owned nodes that
         form the composite subgraph.
        strict_naming (bool): When true, repeated assignment of a new node to an
         existing node label will raise an error, otherwise the label gets appended
         with an index and the assignment proceeds. (Default is true: disallow assigning
         to existing labels.)
        add (NodeAdder): A tool for adding new nodes to this subgraph.
        upstream_nodes (list[pyiron_contrib.workflow.node,Node]): All the owned
         nodes that have output connections but no input connections, i.e. the
         upstream-most nodes.
        starting_nodes (None | list[pyiron_contrib.workflow.node,Node]): A subset
         of the owned nodes to be used on running. (Default is None, running falls back
         on using the `upstream_nodes`.)

    Methods:
        add(node: Node): Add the node instance to this subgraph.
        remove(node: Node): Break all connections the node has, remove it from this
         subgraph, and set its parent to `None`.
    """

    wrap_as = _NodeDecoratorAccess  # Class method access to decorators
    # Allows users/devs to easily create new nodes when using children of this class

    create = Creator

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Composite] = None,
        run_on_updates: bool = True,
        strict_naming: bool = True,
        inputs_map: Optional[dict] = None,
        outputs_map: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(
            *args, label=label, parent=parent, run_on_updates=run_on_updates, **kwargs
        )
        self.strict_naming: bool = strict_naming
        self.inputs_map = inputs_map
        self.outputs_map = outputs_map
        self.nodes: DotDict[str:Node] = DotDict()
        self.add: NodeAdder = NodeAdder(self)
        self.starting_nodes: None | list[Node] = None

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
        return [
            node
            for node in self.nodes.values()
            if node.outputs.connected and not node.inputs.connected
        ]

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
                default_key = f"{node.label}_{channel_label}"
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

    def add_node(self, node: Node, label: Optional[str] = None) -> None:
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
            warn(
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
            warn(
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

    def __setattr__(self, label: str, node: Node):
        if isinstance(node, Node) and label != "parent":
            self.add_node(node, label=label)
        else:
            super().__setattr__(label, node)

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


class NodeAdder:
    """
    This class provides a layer of misdirection so that `Composite` objects can set
    themselves as the parent of owned nodes.

    It also provides access to packages of nodes and the ability to register new
    packages.
    """

    def __init__(self, parent: Composite):
        self._parent: Composite = parent
        self.register_nodes("atomistics", *atomistics.nodes)
        self.register_nodes("standard", *standard.nodes)

    Function = Function
    Slow = Slow
    SingleValue = SingleValue

    def __getattribute__(self, key):
        value = super().__getattribute__(key)
        if value == Function:
            return partial(Function, parent=self._parent)
        return value

    def __call__(self, node: Node):
        return self._parent.add_node(node)

    def register_nodes(self, domain: str, *nodes: list[type[Node]]):
        """
        Add a list of node classes to be accessible for creation under the provided
        domain name.

        TODO: multiple dispatch so we can handle registering something other than a
              list, e.g. modules or even urls.
        """
        setattr(self, domain, NodePackage(self._parent, *nodes))

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return "#8c564b"
