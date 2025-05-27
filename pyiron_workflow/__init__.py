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
- Easy extensibility by collecting nodes together in a python module for sharing/reusing

Planned:
- More user-friendly usage of :mod:`executorlib`
- Integration with :mod:`semantikon` for ontological hinting of data channels to provide
    guided workflow design
"""

from ._version import get_versions

__version__ = get_versions()["version"]

# User API

from pyiron_workflow.api import (
    Workflow,  # pyironic user single-point of entry
    as_dataclass_node,
    as_function_node,
    as_macro_node,
    dataclass_node,
    for_node,
    function_node,
    macro_node,
    std,
    to_function_node,
    while_node,
)
