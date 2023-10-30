"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from importlib import import_module
from sys import version_info

from pyiron_base.interfaces.singleton import Singleton

# from pympipool.mpi.executor import PyMPISingleTaskExecutor as Executor
from pyiron_workflow.executors import CloudpickleProcessPoolExecutor as Executor

from pyiron_workflow.function import (
    Function,
    SingleValue,
    function_node,
    single_value_node,
)


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Handles the registration of new node packages and, by virtue of being a singleton,
    makes them available to all composite nodes holding a creator.
    """

    def __init__(self):
        self._node_packages = {}

        self.Executor = Executor

        self.Function = Function
        self.SingleValue = SingleValue

        # Avoid circular imports by delaying import for children of Composite
        self._macro = None
        self._workflow = None
        self._meta = None

        if version_info[0] == 3 and version_info[1] >= 10:
            # These modules use syntactic sugar for type hinting that is only supported
            # in python >=3.10
            # If the CI skips testing on 3.9 gets dropped, we can think about removing
            # this if-clause and just letting users of python <3.10 hit an error.
            self.register("standard", "pyiron_workflow.node_library.standard")

    @property
    def Macro(self):
        if self._macro is None:
            from pyiron_workflow.macro import Macro

            self._macro = Macro
        return self._macro

    @property
    def Workflow(self):
        if self._workflow is None:
            from pyiron_workflow.workflow import Workflow

            self._workflow = Workflow
        return self._workflow

    @property
    def meta(self):
        if self._meta is None:
            from pyiron_workflow.meta import meta_nodes

            self._meta = meta_nodes
        return self._meta

    def __getattr__(self, item):
        try:
            module = import_module(self._node_packages[item])
            from pyiron_workflow.node_package import NodePackage

            return NodePackage(*module.nodes)
        except KeyError as e:
            raise AttributeError(
                f"{self.__class__.__name__} could not find attribute {item} -- did you "
                f"forget to register node package to this key?"
            ) from e

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state

    def register(self, domain: str, package_identifier: str) -> None:
        """
        Add a new package of nodes under the provided attribute, e.g. after adding
        nodes to the domain `"my_nodes"`, and instance of creator can call things like
        `creator.my_nodes.some_node_that_is_there()`.

        Note: If a macro is going to use a creator, the node registration should be
            _inside_ the macro definition to make sure the node actually has access to
            those nodes! It also needs to be _able_ to register those nodes, i.e. have
            import access to that location, but we don't for that check that.

        Args:
            domain (str): The attribute name at which to register the new package.
                (Note: no sanitizing is done here, so if you provide a string that
                won't work as an attribute name, that's your problem.)
            package_identifier (str): An identifier for the node package. (Right now
                that's just a string version of the path to the module, e.g.
                `pyiron_workflow.node_library.standard`.)

        Raises:
            KeyError: If the domain already exists, but the identifier doesn't match
                with the stored identifier.
            AttributeError: If you try to register at a domain that is already another
                method or attribute of the creator.
            ValueError: If the identifier can't be parsed.
        """

        if self._package_conflicts_with_existing(domain, package_identifier):
            raise KeyError(
                f"{domain} is already a registered node package, please choose a "
                f"different domain to store these nodes under"
            )
        elif domain in self.__dir__():
            raise AttributeError(f"{domain} is already an attribute of {self}")

        self._verify_identifier(package_identifier)

        self._node_packages[domain] = package_identifier

    def _package_conflicts_with_existing(
        self, domain: str, package_identifier: str
    ) -> bool:
        """
        Check if the new package conflict with an existing package at the requested
        domain; if there isn't one, or if the new and old packages are identical then
        there is no conflict!

        Args:
            domain (str): The domain at which the new package is attempting to register.
            package_identifier (str): The identifier for the new package.

        Returns:
            (bool): True iff there is a package already at that domain and it is not
                the same as the new one.
        """
        if domain in self._node_packages.keys():
            # If it's already here, it had better be the same package
            return package_identifier != self._node_packages[domain]
            # We can make "sameness" logic more complex as we allow more sophisticated
            # identifiers
        else:
            # If it's not here already, it can't conflict!
            return False

    @staticmethod
    def _verify_identifier(package_identifier: str):
        """
        Logic for verifying whether new package identifiers will actually be usable for
        creating node packages when their domain is called. Lets us fail early in
        registration.

        Right now, we just make sure it's a string from which we can import a list of
        nodes.
        """
        from pyiron_workflow.node import Node

        try:
            module = import_module(package_identifier)
            nodes = module.nodes
            if not all(issubclass(node, Node) for node in nodes):
                raise TypeError(
                    f"At least one node in {nodes} was not of the type {Node.__name__}"
                )
        except Exception as e:
            raise ValueError(
                f"The package identifier is {package_identifier} is not valid. Please "
                f"ensure it is an importable module with a list of {Node.__name__} "
                f"objects stored in the variable `nodes`."
            ) from e


class Wrappers(metaclass=Singleton):
    """
    A container class giving access to the decorators that transform functions to nodes.
    """

    def __init__(self):
        self.function_node = function_node
        self.single_value_node = single_value_node

        # Avoid circular imports by delaying import when wrapping children of Composite
        self._macro_node = None

    @property
    def macro_node(self):
        if self._macro_node is None:
            from pyiron_workflow.macro import macro_node

            self._macro_node = macro_node
        return self._macro_node
