"""
:mod:`pyiron_workflow._legacy` is a python framework for constructing computational workflows
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
- (Optional) remote execution of individual nodes
- Both acyclic (execution flow is automated) and cyclic (execution flow must be
    specified) graphs allowed
- Easy extensibility by collecting nodes together in a python module for sharing/reusing
"""

import importlib.metadata

try:
    # Installed package will find its version
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # Repository clones will register an unknown version
    __version__ = "0.0.0+unknown"

# User API

from pyiron_workflow._legacy.api import NodeSlurmExecutor as NodeSlurmExecutor
from pyiron_workflow._legacy.api import (
    Workflow as Workflow,
)  # pyironic user single-point of entry
from pyiron_workflow._legacy.api import as_dataclass_node as as_dataclass_node
from pyiron_workflow._legacy.api import as_function_node as as_function_node
from pyiron_workflow._legacy.api import as_macro_node as as_macro_node
from pyiron_workflow._legacy.api import dataclass_node as dataclass_node
from pyiron_workflow._legacy.api import for_node as for_node
from pyiron_workflow._legacy.api import function_node as function_node
from pyiron_workflow._legacy.api import macro_node as macro_node
from pyiron_workflow._legacy.api import std as std
from pyiron_workflow._legacy.api import to_function_node as to_function_node
from pyiron_workflow._legacy.api import while_node as while_node
