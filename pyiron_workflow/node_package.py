from __future__ import annotations

from pyiron_workflow.node import Node
from pyiron_workflow.util import DotDict


class NodePackage(DotDict):
    """
    A collection of node classes.

    Node classes are accessible by their _class name_ by item or attribute access.

    Can be extended by adding node classes to new names with an item or attribute set,
    but to update an existing node the `update` method must be used.
    """

    def __init__(self, *node_classes: Node):
        super().__init__()
        for node in node_classes:
            self[node.__name__] = node

    def __setitem__(self, key, value):
        if key in self.keys():
            raise KeyError(f"The name {key} is already a stored node class.")
        elif key in self.__dir__():
            raise KeyError(
                f"The name {key} is already an attribute of this "
                f"{self.__class__.__name__} instance."
            )
        if not isinstance(value, type) or not issubclass(value, Node):
            raise TypeError(
                f"Can only set members that are (sub)classes of  {Node.__name__}, "
                f"but got {type(value)}"
            )
        super().__setitem__(key, value)

    def update(self, *node_classes):
        replacing = set(self.keys()).intersection([n.__name__ for n in node_classes])
        for name in replacing:
            del self[name]

        for node in node_classes:
            self[node.__name__] = node
