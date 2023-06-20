from __future__ import annotations

from abc import ABC
from functools import partial
from typing import Optional, TYPE_CHECKING
from warnings import warn

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.node_library import atomistics, standard
from pyiron_contrib.workflow.node_library.package import NodePackage
from pyiron_contrib.workflow.util import DotDict


class HasNodes(ABC):
    """
    A mixin class for classes which hold a graph of nodes.

    Attribute assignment is overriden such that assignment of a `Node` instance adds
    it directly to the collection of nodes.
    """

    def __init__(self, *args, strict_naming=True, **kwargs):
        self.nodes: DotDict = DotDict()
        self.add: NodeAdder = NodeAdder(self)
        self.strict_naming: bool = strict_naming

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

        label = self._ensure_label_is_unique(node.label if label is None else label)
        self._ensure_node_has_no_other_parent(node, label)

        self.nodes[label] = node
        node.label = label
        node.parent = self
        return node

    def _ensure_label_is_unique(self, label):
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
            warn(
                f"{label} is already a node; appending an index to the "
                f"node label instead: {label}{i}"
            )
            new_label = f"{label}{i}"
            i += 1
        return new_label

    def _ensure_node_has_no_other_parent(self, node: Node, label: str):
        if (
            node.parent is self  # This should guarantee the node is in self.nodes
            and label != node.label
        ):
            assert self.nodes[node.label] is node  # Should be unreachable by users
            warn(
                f"Reassigning the node {node.label} to the label {label} when "
                f"adding it to the parent {self.label}."
            )
            del self.nodes[node.label]
        elif node.parent is not None:
            raise ValueError(
                f"The node ({node.label}) already belongs to the parent "
                f"{node.parent.label}. Please remove it there before trying to "
                f"add it to this parent ({self.label})."
            )

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
    This class provides a layer of misdirection so that `HasNodes` objects can set
    themselves as the parent of owned nodes.

    It also provides access to packages of nodes and the ability to register new
    packages.
    """

    def __init__(self, parent: HasNodes):
        self._parent: HasNodes = parent
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
