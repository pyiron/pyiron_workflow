"""
Container classes for giving access to various workflow objects and tools
"""

from __future__ import annotations

from abc import ABC
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import lru_cache

from pyiron_snippets.dotdict import DotDict
from pyiron_snippets.singleton import Singleton
from executorlib import Executor as ExecutorlibExecutor

from pyiron_workflow.executors import CloudpickleProcessPoolExecutor
from pyiron_workflow.nodes.function import function_node, as_function_node


class Creator(metaclass=Singleton):
    """
    A container class for providing access to various workflow objects.
    Gives access to various workflow tools and, by virtue of being a singleton, makes them
    available to all composite nodes holding a creator.

    In addition to node objects, the creator also provides workflow-compliant executors
    for parallel processing.
    This includes a very simple in-house executor that is useful for learning, but also
    choices from the :mod:`executorlib` packages.
    Some :mod:`executorlib` executors may not be available on your machine (e.g. flux-
    and/or slurm-based executors), in which case these attributes will return `None`
    instead.
    """

    def __init__(self):

        # Standard lib
        self.ProcessPoolExecutor = ProcessPoolExecutor
        self.ThreadPoolExecutor = ThreadPoolExecutor
        # Local cloudpickler
        self.CloudpickleProcessPoolExecutor = CloudpickleProcessPoolExecutor
        # executorlib
        self.ExecutorlibExecutor = ExecutorlibExecutor

        self.function_node = function_node

    @property
    @lru_cache(maxsize=1)
    def standard(self):
        from pyiron_workflow.nodes import standard

        return standard

    @property
    @lru_cache(maxsize=1)
    def for_node(self):
        from pyiron_workflow.nodes.for_loop import for_node

        return for_node

    @property
    @lru_cache(maxsize=1)
    def macro_node(self):
        from pyiron_workflow.nodes.macro import macro_node

        return macro_node

    @property
    @lru_cache(maxsize=1)
    def Workflow(self):
        from pyiron_workflow.workflow import Workflow

        return Workflow

    @property
    @lru_cache(maxsize=1)
    def meta(self):
        from pyiron_workflow.nodes.transform import inputs_to_list, list_to_outputs

        return DotDict(
            {
                inputs_to_list.__name__: inputs_to_list,
                list_to_outputs.__name__: list_to_outputs,
            }
        )

    @property
    @lru_cache(maxsize=1)
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
        return super().__dir__() + list(self._package_access.keys())


class Wrappers(metaclass=Singleton):
    """
    A container class giving access to the decorators that transform functions to nodes.
    """

    as_function_node = staticmethod(as_function_node)

    @property
    @lru_cache(maxsize=1)
    def as_macro_node(self):
        from pyiron_workflow.nodes.macro import as_macro_node

        return as_macro_node

    @property
    @lru_cache(maxsize=1)
    def as_dataclass_node(self):
        from pyiron_workflow.nodes.transform import as_dataclass_node

        return as_dataclass_node


class HasCreator(ABC):
    """
    A mixin class for creator (including both class-like and decorator).
    """

    create = Creator()
    wrap = Wrappers()
