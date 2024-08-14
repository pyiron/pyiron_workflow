"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import os
from pathlib import Path
import pickle
from typing import TYPE_CHECKING

import cloudpickle
from pyiron_snippets.files import DirectoryObject, FileObject

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface(ABC):

    def save(self, node: Node):
        dir = DirectoryObject(self._canonical_node_directory(node))
        self._save(node)
        if dir.is_empty():
            dir.delete()

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
        if self._canonical_node_directory(node).exists():
            dir = DirectoryObject(self._canonical_node_directory(node))
            if dir.is_empty():
                dir.delete()

    @abstractmethod
    def _delete(self, node: Node):
        """Remove an existing save-file for this backend"""

    @classmethod
    def _canonical_node_directory(cls, node: Node, start: Path | None = None) -> Path:
        return (
            Path.cwd() if start is None else start
        ).joinpath(
            *node.semantic_path.split(node.semantic_delimiter)
        )


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

        try:
            with open(self._storage_file(self._PICKLE, node), "wb") as file:
                pickle.dump(node, file)
        except Exception:
            self._delete(node)
            with open(self._storage_file(self._CLOUDPICKLE, node), "wb") as file:
                cloudpickle.dump(node, file)

    def _load(self, node: Node) -> Node:
        if self._has_contents(self._PICKLE, node):
            with open(self._storage_file(self._PICKLE, node), "rb") as file:
                inst = pickle.load(file)
        elif self._has_contents(self._CLOUDPICKLE, node):
            with open(self._storage_file(self._CLOUDPICKLE, node), "rb") as file:
                inst = cloudpickle.load(file)
        return inst

    def _delete_file(self, file: str, node: Node):
        FileObject(file, DirectoryObject(self._canonical_node_directory(node))).delete()

    def _delete(self, node: Node):
        if self._has_contents(self._PICKLE, node):
            self._delete_file(self._PICKLE, node)
        elif self._has_contents(self._CLOUDPICKLE, node):
            self._delete_file(self._CLOUDPICKLE, node)

    def _storage_file(self, file: str, node: Node):
        return str((self._canonical_node_directory(node) / file).resolve())

    def has_contents(self, node: Node) -> bool:
        return any(
            self._has_contents(file, node) for file in [self._PICKLE, self._CLOUDPICKLE]
        )

    def _has_contents(self, file: str, node: Node) -> bool:
        return os.path.isfile(self._storage_file(file, node))
