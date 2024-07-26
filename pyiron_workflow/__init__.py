"""
:mod:`pyiron_workflow` is a python framework for constructing computational workflows
in a graph-based format.
The intent of such a framework is to improve the reliability and shareability of
computational workflows, as well as providing supporting infrastructure for the
storage and retrieval of data, and executing computations on remote resources (with a
special emphasis on HPC environments common in academic research).
It is a key goal that writing such workflows should be as easy as possible, and simple
cases should be _almost_ as simple as writing and running plain python functions.

Key features:

- Single point of import
- Easy "nodeification" of regular python code
- Macro nodes, so complex workflows can be built by composition
- (Optional) type checking for data connections
- (Optional) remote execution of individual nodes (currently only very simple
    single-core, same-machine parallel processes)
- Both acyclic (execution flow is automated) and cyclic (execution flow must be
    specified) graphs allowed
- Easy extensibility by collecting packages of nodes together for sharing/reusing

Planned:
- Storage of executed workflows, including restarting from a partially executed workflow
- Support for more complex remote execution, especially leveraging :mod:`executorlib`
- Infrastructure that supports and encourages of FAIR principles for node packages and
  finished workflows
- Ontological hinting for data channels in order to provide guided workflow design
- GUI on top for code-lite/code-free visual scripting
"""

from ._version import get_versions

__version__ = get_versions()["version"]

# API

# User entry point
from pyiron_workflow.workflow import Workflow

# Node developer entry points
from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.nodes import standard as standard_nodes
from pyiron_workflow.nodes.for_loop import for_node, for_node_factory
from pyiron_workflow.nodes.function import Function, as_function_node, function_node
from pyiron_workflow.logging import logger
from pyiron_workflow.nodes.macro import Macro, as_macro_node, macro_node
from pyiron_workflow.nodes.transform import (
    # as_dataclass_node,  # Not pickling nicely yet
    dataclass_node,
    inputs_to_dataframe,
    inputs_to_dict,
    inputs_to_list,
    list_to_outputs,
)
