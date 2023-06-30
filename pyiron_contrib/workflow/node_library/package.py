from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.util import DotDict

if TYPE_CHECKING:
    from pyiron_contrib.workflow.composite import Composite


class NodePackage(DotDict):
    """
    A collection of node classes that, when instantiated, will have their workflow
    automatically set.

    Node classes are accessible by their _class name_ by item or attribute access.

    Can be extended by adding node classes to new names with an item or attribute set,
    but to update an existing node the `update` method must be used.
    """

    def __init__(self, parent: Composite, *node_classes: Node):
        super().__init__()
        self.__dict__["_parent"] = parent  # Avoid the __setattr__ override
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

    def __getitem__(self, item):
        value = super().__getitem__(item)
        if issubclass(value, Node):
            return partial(value, parent=self._parent)
        else:
            return value

    def update(self, *node_classes):
        replacing = set(self.keys()).intersection([n.__name__ for n in node_classes])
        for name in replacing:
            del self[name]

        for node in node_classes:
            self[node.__name__] = node
