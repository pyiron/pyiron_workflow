"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from importlib import import_module
import pkgutil
from sys import version_info

from pyiron_workflow.snippets.singleton import Singleton

# Import all the supported executors
from pympipool import Executor as PyMpiPoolExecutor, PyMPIExecutor

try:
    from pympipool import PySlurmExecutor
except ImportError:
    PySlurmExecutor = None
try:
    from pympipool import PyFluxExecutor
except ImportError:
    PyFluxExecutor = None

from pyiron_workflow.executors import CloudpickleProcessPoolExecutor

# Then choose one executor to be "standard"
Executor = PyMpiPoolExecutor

from pyiron_workflow.function import (
    Function,
    SingleValue,
    function_node,
    single_value_node,
)
from pyiron_workflow.snippets.dotdict import DotDict


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Handles the registration of new node packages and, by virtue of being a singleton,
    makes them available to all composite nodes holding a creator.

    In addition to node objects, the creator also provides workflow-compliant executors
    for parallel processing.
    This includes a very simple in-house executor that is useful for learning, but also
    choices from the `pympipool` packages.
    Some `pympipool` executors may not be available on your machine (e.g. flux- and/or
     slurm-based executors), in which case these attributes will return `None` instead.
    """

    def __init__(self):
        self._node_packages = {}

        self.Executor = Executor
        self.CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor
        self.PyMPIExecutor = PyMPIExecutor
        self.PyMpiPoolExecutor = PyMpiPoolExecutor

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
    def PyFluxExecutor(self):
        if PyFluxExecutor is None:
            raise ImportError(f"{PyFluxExecutor.__name__} is not available")
        return PyFluxExecutor

    @property
    def PySlurmExecutor(self):
        if PySlurmExecutor is None:
            raise ImportError(f"{PySlurmExecutor.__name__} is not available")
        return PySlurmExecutor

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
            from pyiron_workflow.meta import (
                for_loop,
                input_to_list,
                list_to_output,
                while_loop,
            )
            from pyiron_workflow.snippets.dotdict import DotDict

            self._meta = DotDict(
                {
                    for_loop.__name__: for_loop,
                    input_to_list.__name__: input_to_list,
                    list_to_output.__name__: list_to_output,
                    while_loop.__name__: while_loop,
                }
            )
        return self._meta

    def __getattr__(self, item):
        try:
            return self._node_packages[item][1]
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

        self._node_packages[domain] = (
            package_identifier,
            self._import_nodes(package_identifier),
        )

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
            return package_identifier != self._node_packages[domain][0]
            # We can make "sameness" logic more complex as we allow more sophisticated
            # identifiers
        else:
            # If it's not here already, it can't conflict!
            return False

    def _import_nodes(self, package_identifier: str):
        """
        Recursively walk through all submodules of the provided package identifier,
        and collect an instance of `nodes: list[Node]` from each non-package module.
        """

        module = import_module(package_identifier)
        if hasattr(module, "__path__"):
            package = DotDict()
            for _, submodule_name, _ in pkgutil.walk_packages(
                module.__path__, module.__name__ + "."
            ):
                package[submodule_name.split(".")[-1]] = self._import_nodes(
                    submodule_name
                )
        else:
            package = self._get_nodes_from_module(module)
        return package

    @staticmethod
    def _get_nodes_from_module(module):
        from pyiron_workflow.node import Node
        from pyiron_workflow.node_package import NodePackage

        try:
            nodes = module.nodes
        except AttributeError:
            raise ValueError(
                f"Could node find `nodes: list[Nodes]` in {module.__name__}"
            )
        if not all(issubclass(node, Node) for node in nodes):
            raise TypeError(
                f"At least one node in {nodes} was not of the type {Node.__name__}"
            )
        return NodePackage(*module.nodes)


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
