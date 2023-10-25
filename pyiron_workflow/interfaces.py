"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiron_base.interfaces.singleton import Singleton

# from pympipool.mpi.executor import PyMPISingleTaskExecutor as Executor
from pyiron_workflow.executors import CloudpickleProcessPoolExecutor as Executor

from pyiron_workflow.function import (
    Function,
    SingleValue,
    function_node,
    single_value_node,
)

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Handles the registration of new node packages and, by virtue of being a singleton,
    makes them available to all composite nodes holding a creator.
    """

    def __init__(self):
        self.Executor = Executor

        self.Function = Function
        self.SingleValue = SingleValue

        # Avoid circular imports by delaying import for children of Composite
        self._macro = None
        self._workflow = None
        self._meta = None

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
    def standard(self):
        from pyiron_workflow.node_package import NodePackage
        from pyiron_workflow.node_library.standard import nodes

        return NodePackage(*nodes)

    @property
    def atomistics(self):
        from pyiron_workflow.node_package import NodePackage
        from pyiron_workflow.node_library.atomistics import nodes

        return NodePackage(*nodes)

    @property
    def atomistic_codes(self):
        from pyiron_workflow.node_package import NodePackage
        from pyiron_workflow.node_library.atomistic_codes import nodes

        return NodePackage(*nodes)

    @property
    def lammps(self):
        from pyiron_workflow.node_package import NodePackage
        from pyiron_workflow.node_library.lammps import nodes

        return NodePackage(*nodes)

    @property
    def meta(self):
        if self._meta is None:
            from pyiron_workflow.meta import meta_nodes

            self._meta = meta_nodes
        return self._meta

    def register(self, domain: str, *nodes: list[type[Node]]):
        raise NotImplementedError(
            "Registering new node packages is currently not playing well with "
            "executors. We hope to return this feature soon."
        )
        # if domain in self.__dir__():
        #     raise AttributeError(f"{domain} is already an attribute of {self}")
        # from pyiron_workflow.node_package import NodePackage
        #
        # setattr(self, domain, NodePackage(*nodes))


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
