from __future__ import annotations

import inspect
import importlib.util
from pathlib import Path
import sys
from types import ModuleType

from pyiron_workflow.node import Node


def _get_subclasses(
    source: str | Path | ModuleType,
    base_class: type,
    get_private: bool = False,
    get_abstract: bool = False,
    get_local_only: bool = True,
):
    if isinstance(source, (str, Path)):
        source = Path(source)
        if source.is_file():
            # Load the module from the file
            module_name = source.stem
            spec = importlib.util.spec_from_file_location(module_name, str(source))
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
        obj for name, obj in inspect.getmembers(module, inspect.isclass)
        if (
            issubclass(obj, base_class) and
            (get_private or not name.startswith('_')) and
            (get_abstract or not inspect.isabstract(obj)) and
            (obj.__module__ == module.__name__ or not get_local_only)
        )
    ]


def find_nodes(source: str | Path | ModuleType) -> list[type[Node]]:
    """
    Get a list of all public, non-abstract nodes defined in the source.
    """
    return _get_subclasses(source, Node)
