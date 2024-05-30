from __future__ import annotations

from importlib import import_module
from inspect import isclass

from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.node import Node


NODE_PACKAGE_ATTRIBUTES = ("package_identifier",)


class NotANodePackage(Exception):
    pass


class NodePackage(DotDict):
    """
    A collection of node classes loaded from a package (right now that's just a python
    module (.py file) with a `nodes: list[Node]` attribute).

    Node classes are accessible by their _class name_ by item or attribute access.
    """

    def __init__(self, package_identifier: str):
        super().__init__(package_identifier=package_identifier)
        try:
            self._register_from_module_name(package_identifier)
        except ModuleNotFoundError as e:
            raise NotANodePackage(
                f"In the current implementation, we expect package identifiers to be "
                f"modules, but {package_identifier} couldn't be imported. If this "
                f"looks like a module, perhaps it's simply not in your path?"
            ) from e

    def _register_from_module_name(self, module_name: str):
        module = import_module(module_name)

        try:
            node_classes = module.nodes
        except AttributeError as e:
            raise NotANodePackage(
                f"Couldn't find an attribute `nodes` in {module_name}"
            )

        for node in node_classes:
            if not isclass(node) or not issubclass(node, Node):
                raise NotANodePackage(
                    f"Node packages must contain only nodes, but the package "
                    f"{module_name} got {node}"
                )
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
            value.package_identifier = self.package_identifier
            super().__setitem__(key, value)
        else:
            raise TypeError(
                f"Can only set members that are (sub)classes of  {Node.__name__}, "
                f"but got {type(value)}"
            )

    def __len__(self):
        # Only count the nodes themselves
        return super().__len__() - len(NODE_PACKAGE_ATTRIBUTES)

    def __hash__(self):
        # Dictionaries (DotDict(dict)) are mutable and thus not hashable
        # Since the identifier is expected to be unique for the package, just hash that
        return hash(self.package_identifier)
