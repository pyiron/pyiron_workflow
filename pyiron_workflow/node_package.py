from __future__ import annotations

from pyiron_workflow.node import Node
from pyiron_workflow.snippets.dotdict import DotDict


NODE_PACKAGE_ATTRIBUTES = ("_identifier",)


class NodePackage(DotDict):
    """
    A collection of node classes.

    Node classes are accessible by their _class name_ by item or attribute access.

    Can be extended by adding node classes to new names with an item or attribute set,
    but to update an existing node the :meth:`update` method must be used.
    """

    def __init__(self, identifier: str, *node_classes: Node):
        super().__init__(_identifier=identifier)
        for node in node_classes:
            self[node.__name__] = node

    def __setitem__(self, key, value):
        # Fail fast if key is forbidden
        if key in self.keys():
            raise KeyError(f"The name {key} is already a stored node class.")
        elif key in self.__dir__():
            raise KeyError(
                f"The name {key} is already an attribute of this "
                f"{self.__class__.__name__} instance."
            )

        # Continue if key/value is permissible
        if key in NODE_PACKAGE_ATTRIBUTES:
            super().__setitem__(key, value)  # Special properties that are allowed
        elif isinstance(value, type) and issubclass(value, Node):
            # Set the node class's package identifier and hold that class
            value.package_identifier = self._identifier
            super().__setitem__(key, value)
        else:
            raise TypeError(
                f"Can only set members that are (sub)classes of  {Node.__name__}, "
                f"but got {type(value)}"
            )

    def update(self, *node_classes):
        replacing = set(self.keys()).intersection([n.__name__ for n in node_classes])
        for name in replacing:
            del self[name]

        for node in node_classes:
            self[node.__name__] = node

    def __len__(self):
        # Only count the nodes themselves
        return super().__len__() - len(NODE_PACKAGE_ATTRIBUTES)
