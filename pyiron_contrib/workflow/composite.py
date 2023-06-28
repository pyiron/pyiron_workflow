"""
A base class for nodal objects that have internal structure -- i.e. they hold a
sub-graph
"""

from __future__ import annotations

from abc import ABC
from functools import partial
from typing import Optional
from warnings import warn

from pyiron_contrib.workflow.is_nodal import IsNodal
from pyiron_contrib.workflow.node import Node, node, fast_node, single_value_node
from pyiron_contrib.workflow.node_library import atomistics, standard
from pyiron_contrib.workflow.node_library.package import NodePackage
from pyiron_contrib.workflow.util import DotDict


class _NodeDecoratorAccess:
    """An intermediate container to store node-creating decorators as class methods."""

    node = node
    fast_node = fast_node
    single_value_node = single_value_node


class Composite(IsNodal, ABC):
    """
    A base class for nodes that have internal structure -- i.e. they hold a sub-graph.

    Item and attribute access is modified to give access to owned nodes.
    Adding a node with the `add` functionality or by direct attribute assignment sets
    this object as the parent of that node.

    Offers a class method (`wrap_as`) to give easy access to the node-creating
    decorators.

    Specifies the required `on_run()` to call `run()` on a subset of owned nodes, i.e.
    to kick-start computation on the owned sub-graph.
    By default, `run()` will be called on all owned nodes who's input has no
    connections, but this can be overridden to specify particular nodes to use instead.

    Does not specify `input` and `output` as demanded by the parent class; this
    requirement is still passed on to children.

    Attributes:
        TBA

    Methods:
        TBA
    """

    wrap_as = _NodeDecoratorAccess  # Class method access to decorators
    # Allows users/devs to easily create new nodes when using children of this class

    def __init__(
            self,
            label: str,
            *args,
            parent: Optional[Composite] = None,
            strict_naming: bool = True,
            **kwargs
    ):
        super().__init__(*args, label=label, parent=parent, **kwargs)
        self.strict_naming: bool = strict_naming
        self.nodes: DotDict[str: IsNodal] = DotDict()
        self.add: NodeAdder = NodeAdder(self)
        self.starting_nodes: None | list[Node] = None

    def to_dict(self):
        return {
            "label": self.label,
            "nodes": {n.label: n.to_dict() for n in self.nodes.values()},
        }

    @property
    def upstream_nodes(self) -> list[IsNodal]:
        return [
            node for node in self.nodes.values()
            if node.outputs.connected and not node.inputs.connected
        ]

    def on_run(self):
        starting_nodes = self.upstream_nodes if self.starting_nodes is None \
            else self.starting_nodes
        for node in starting_nodes:
            node.run()

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
        if isinstance(node, Node):
            self.add_node(node, label=label)
        else:
            super().__setattr__(label, node)

    def __getattr__(self, key):
        return self.nodes[key]

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

    Node = Node

    def __getattribute__(self, key):
        value = super().__getattribute__(key)
        if value == Node:
            return partial(Node, parent=self._parent)
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
