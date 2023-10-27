"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from importlib import import_module

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

        self.register("standard", "pyiron_workflow.node_library.standard")
        self.register("atomistics", "pyiron_workflow.node_library.atomistics")

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

    def register(self, domain: str, package_identifier: str):
        if domain in self._node_packages.keys():
            if package_identifier != self._node_packages[domain]:
                raise KeyError(
                    f"{domain} is already a registered node package, please choose a "
                    f"different domain to store these nodes under"
                )
            # Else we're just re-registering the same thing to the same place,
            # which is fine
        elif domain in self.__dir__():
            raise AttributeError(f"{domain} is already an attribute of {self}")

        self._node_packages[domain] = package_identifier


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
