"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
import pickle
from typing import TYPE_CHECKING

import cloudpickle

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface(ABC):

    def save(self, node: Node):
        dir = node.as_path()
        dir.mkdir(parents=True, exist_ok=True)
        try:
            self._save(node)
        except Exception as e:
            raise e
        finally:
            # If nothing got written due to the exception, clean up the directory
            # (as long as there's nothing else in it)
            if not any(dir.iterdir()):
                dir.rmdir()

    @abstractmethod
    def _save(self, node: Node):
        pass

    def load(self, node: Node) -> Node:
        # Misdirection is strictly for symmetry with _save, so child classes define the
        # private method in both cases
        return self._load(node)

    @abstractmethod
    def _load(self, node: Node):
        pass

    @abstractmethod
    def has_contents(self, node: Node) -> bool:
        pass

    def delete(self, node: Node):
        if self.has_contents(node):
            self._delete(node)
        dir = node.as_path()
        if dir.exists() and not any(dir.iterdir()):
            dir.rmdir()

    @abstractmethod
    def _delete(self, node: Node):
        """Remove an existing save-file for this backend"""


class PickleStorage(StorageInterface):

    _PICKLE = "pickle.pckl"
    _CLOUDPICKLE = "cloudpickle.cpckl"

    def _save(self, node: Node):
        if not node.import_ready:
            raise TypeNotFoundError(
                f"{node.label} cannot be saved with the storage interface "
                f"{self.__class__.__name__} because it (or one of its children) has "
                f"a type that cannot be imported. Did you dynamically define this "
                f"nodeect? \n"
                f"Import readiness report: \n"
                f"{node.report_import_readiness()}"
            )

        dir = node.as_path()
        for file, save_method in [
            (self._PICKLE, pickle.dump),
            (self._CLOUDPICKLE, cloudpickle.dump),
        ]:
            p = dir / file
            try:
                with open(p, "wb") as filehandle:
                    save_method(node, filehandle)
                return
            except Exception as e:
                p.unlink(missing_ok=True)
        raise e

    def _load(self, node: Node) -> Node:
        dir = node.as_path()
        for file, load_method in [
            (self._PICKLE, pickle.load),
            (self._CLOUDPICKLE, cloudpickle.load),
        ]:
            p = (dir / file)
            if p.exists():
                with open(p, "rb") as filehandle:
                    inst = load_method(filehandle)
                return inst

    def _delete(self, node: Node):
        (node.as_path() / self._PICKLE).unlink(missing_ok=True)
        (node.as_path() / self._CLOUDPICKLE).unlink(missing_ok=True)

    def has_contents(self, node: Node) -> bool:
        return any(
            (node.as_path() / file).exists()
            for file in [self._PICKLE, self._CLOUDPICKLE]
        )
