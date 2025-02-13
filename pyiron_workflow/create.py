"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import executorlib
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.executors import CloudpickleProcessPoolExecutor
from pyiron_workflow.nodes.function import as_function_node, function_node


class Creator:
    """
    A container class for providing access to various workflow objects.
    Gives access to various workflow tools.

    In addition to node objects, the creator also provides workflow-compliant executors
    for parallel processing.
    """

    def __init__(self):
        # Standard lib
        self.ProcessPoolExecutor = ProcessPoolExecutor
        self.ThreadPoolExecutor = ThreadPoolExecutor
        # Local cloudpickler
        self.CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor

        self.function_node = function_node

    executorlib = executorlib

    @property
    def standard(self):
        from pyiron_workflow.nodes import standard

        return standard

    @property
    def for_node(self):
        from pyiron_workflow.nodes.for_loop import for_node

        return for_node

    @property
    def macro_node(self):
        from pyiron_workflow.nodes.macro import macro_node

        return macro_node

    @property
    def Workflow(self):
        from pyiron_workflow.workflow import Workflow

        return Workflow

    @property
    def meta(self):
        from pyiron_workflow.nodes.transform import inputs_to_list, list_to_outputs

        return DotDict(
            {
                inputs_to_list.__name__: inputs_to_list,
                list_to_outputs.__name__: list_to_outputs,
            }
        )

    @property
    def transformer(self):
        from pyiron_workflow.nodes.transform import (
            dataclass_node,
            inputs_to_dataframe,
            inputs_to_dict,
            inputs_to_list,
            list_to_outputs,
        )

        return DotDict(
            {
                f.__name__: f
                for f in [
                    dataclass_node,
                    inputs_to_dataframe,
                    inputs_to_dict,
                    inputs_to_list,
                    list_to_outputs,
                ]
            }
        )


class Wrappers:
    """
    A container class giving access to the decorators that transform functions to nodes.
    """

    as_function_node = staticmethod(as_function_node)

    @property
    def as_macro_node(self):
        from pyiron_workflow.nodes.macro import as_macro_node

        return as_macro_node

    @property
    def as_dataclass_node(self):
        from pyiron_workflow.nodes.transform import as_dataclass_node

        return as_dataclass_node


class HasCreator:
    """
    A mixin class for creator (including both class-like and decorator).
    """

    create = Creator()
    wrap = Wrappers()
