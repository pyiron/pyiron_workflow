"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from pyiron_base.interfaces.singleton import Singleton

from pyiron_contrib.executors import CloudpickleProcessPoolExecutor
from pyiron_contrib.workflow.function import (
    Function, SingleValue, Slow, function_node, single_value_node, slow_node
)


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Handles the registration of new node packages and, by virtue of being a singleton,
    makes them available to all composite nodes holding a creator.
    """
    def __init__(self):
        self.CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor

        self.Function = Function
        self.SingleValue = SingleValue
        self.Slow = Slow

        # Avoid circular imports by delaying import for children of Composite
        self._macro = None
        self._workflow = None

    @property
    def Macro(self):
        if self._macro is None:
            from pyiron_contrib.workflow.macro import Macro
            self._macro = Macro
        return self._macro

    @property
    def Workflow(self):
        if self._workflow is None:
            from pyiron_contrib.workflow.workflow import Workflow
            self._workflow = Workflow
        return self._workflow


class Wrappers(metaclass=Singleton):
    """
    A container class giving access to the decorators that transform functions to nodes.
    """
    def __init__(self):
        self.function_node = function_node
        self.single_value_node = single_value_node
        self.slow_node = slow_node

        # Avoid circular imports by delaying import when wrapping children of Composite
        self._macro_node = None

    @property
    def macro_node(self):
        if self._macro_node is None:
            from pyiron_contrib.workflow.macro import macro_node
            self._macro_node = macro_node
        return self._macro_node
