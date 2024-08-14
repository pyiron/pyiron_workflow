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

    @abstractmethod
    def save(self, obj: Node):
        pass

    def load(self, obj: Node) -> Node:
        # Misdirection is strictly for symmetry with _save, so child classes define the
        # private method in both cases
        return self._load(obj)

    @abstractmethod
    def _load(self, obj: Node):
        pass

    @abstractmethod
    def has_contents(self, obj: Node) -> bool:
        pass

    def delete(self, obj: Node):
        if self.has_contents:
            self._delete(obj)
        self.tidy_storage_directory(obj)

    @abstractmethod
    def _delete(self, obj: Node):
        """Remove an existing save-file for this backend"""

    @staticmethod
    def storage_directory(obj: Node) -> DirectoryObject:
        return obj.working_directory

    @staticmethod
    def tidy_storage_directory(obj: Node):
        obj.tidy_working_directory()


class PickleStorage(StorageInterface):

    _PICKLE = "pickle.pckl"
    _CLOUDPICKLE = "cloudpickle.cpckl"

    def save(self, obj: Node):
        if not obj.import_ready:
            raise TypeNotFoundError(
                f"{obj.label} cannot be saved with the storage interface "
                f"{self.__class__.__name__} because it (or one of its children) has "
                f"a type that cannot be imported. Did you dynamically define this "
                f"object? \n"
                f"Import readiness report: \n"
                f"{obj.report_import_readiness()}"
            )

        try:
            with open(self._storage_file(self._PICKLE, obj), "wb") as file:
                pickle.dump(obj, file)
        except Exception:
            self._delete(obj)
            with open(self._storage_file(self._CLOUDPICKLE, obj), "wb") as file:
                cloudpickle.dump(obj, file)

    def _load(self, obj: Node) -> Node:
        if self._has_contents(self._PICKLE, obj):
            with open(self._storage_file(self._PICKLE, obj), "rb") as file:
                inst = pickle.load(file)
        elif self._has_contents(self._CLOUDPICKLE, obj):
            with open(self._storage_file(self._CLOUDPICKLE, obj), "rb") as file:
                inst = cloudpickle.load(file)
        return inst

    def _delete_file(self, file: str, obj: Node):
        FileObject(file, self.storage_directory(obj)).delete()

    def _delete(self, obj: Node):
        if self._has_contents(self._PICKLE, obj):
            self._delete_file(self._PICKLE, obj)
        elif self._has_contents(self._CLOUDPICKLE, obj):
            self._delete_file(self._CLOUDPICKLE, obj)

    def _storage_file(self, file: str, obj: Node):
        return str((self.storage_directory(obj).path / file).resolve())

    def has_contents(self, obj: Node) -> bool:
        return any(
            self._has_contents(file, obj) for file in [self._PICKLE, self._CLOUDPICKLE]
        )

    def _has_contents(self, file: str, obj: Node) -> bool:
        return os.path.isfile(self._storage_file(file, obj))
