"""
A utility for finding public `pyiron_workflow.node.Node` objects.

Supports the idea of node developers writing independent node packages.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path
from types import ModuleType
from typing import TypeVar, cast

from pyiron_workflow.node import Node

NodeType = TypeVar("NodeType", bound=Node)


def _get_subclasses(
    source: str | Path | ModuleType,
    base_class: type[NodeType],
    get_private: bool = False,
    get_abstract: bool = False,
    get_imports_too: bool = False,
) -> list[type[NodeType]]:
    if isinstance(source, str | Path):
        source = Path(source)
        if source.is_file():
            # Load the module from the file
            module_name = source.stem
            spec = importlib.util.spec_from_file_location(module_name, str(source))
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create a ModuleSpec for {source}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
        else:
            raise ValueError("File path does not point to a valid file")
    elif inspect.ismodule(source):
        module = source
    else:
        raise ValueError("Input must be a module or a valid file path")

    return [
        obj
        for name, obj in inspect.getmembers(module, inspect.isclass)
        if (
            issubclass(obj, base_class)
            and (get_private or not name.startswith("_"))
            and (get_abstract or not inspect.isabstract(obj))
            and (get_imports_too or _locally_defined(obj, module))
        )
    ]


def _locally_defined(obj, module):
    obj_module_name = obj.__module__
    obj_module = importlib.import_module(obj_module_name)
    return obj_module.__file__ == module.__file__


def find_nodes(source: str | Path | ModuleType) -> list[type[Node]]:
    """
    Get a list of all public, non-abstract nodes defined in the source.
    """
    return cast(list[type[Node]], _get_subclasses(source, Node))
