"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import os
import pickle
from typing import TYPE_CHECKING

import cloudpickle
from pyiron_snippets.files import FileObject

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface(ABC):

    def save(self, obj: Node):
        root = obj.storage_root
        if not root.import_ready:
            raise TypeNotFoundError(
                f"{obj.label} cannot be saved with the "
                f"{obj.storage_backend} because it (or one of its children) has "
                f"a type that cannot be imported. Did you dynamically define this "
                f"object? \n"
                f"Import readiness report: \n"
                f"{obj.report_import_readiness()}"
            )
        if root is obj:
            self._save(obj)
        else:
            root.storage._save(root)

    @abstractmethod
    def _save(self, obj: Node):
        pass

    def load(self, obj: Node):
        # Misdirection is strictly for symmetry with _save, so child classes define the
        # private method in both cases
        return self._load(obj)

    @abstractmethod
    def _load(self, obj: Node):
        pass

    def has_contents(self, obj: Node) -> bool:
        has_contents = self._has_contents(obj)
        obj.tidy_storage_directory()
        return has_contents

    @abstractmethod
    def _has_contents(self, obj: Node) -> bool:
        """Whether a save file exists for this backend"""

    def delete(self, obj: Node):
        if self.has_contents:
            self._delete(obj)
        obj.tidy_storage_directory()

    @abstractmethod
    def _delete(self, obj: Node):
        """Remove an existing save-file for this backend"""


class PickleStorage(StorageInterface):

    _PICKLE_STORAGE_FILE_NAME = "pickle.pckl"
    _CLOUDPICKLE_STORAGE_FILE_NAME = "cloudpickle.cpckl"

    def _save(self, obj: Node):
        try:
            with open(self._pickle_storage_file_path(obj), "wb") as file:
                pickle.dump(obj, file)
        except Exception:
            self._delete(obj)
            with open(self._cloudpickle_storage_file_path(obj), "wb") as file:
                cloudpickle.dump(obj, file)

    def _load(self, obj: Node):
        if self._has_pickle_contents(obj):
            with open(self._pickle_storage_file_path(obj), "rb") as file:
                inst = pickle.load(file)
        elif self._has_cloudpickle_contents(obj):
            with open(self._cloudpickle_storage_file_path(obj), "rb") as file:
                inst = cloudpickle.load(file)

        if inst.__class__ != obj.__class__:
            raise TypeError(
                f"{obj.label} cannot load, as it has type "
                f"{obj.__class__.__name__},  but the saved node has type "
                f"{inst.__class__.__name__}"
            )
        obj.__setstate__(inst.__getstate__())

    def _delete_file(self, file: str, obj: Node):
        FileObject(file, obj.storage_directory).delete()

    def _delete(self, obj: Node):
        if self._has_pickle_contents(obj):
            self._delete_file(self._PICKLE_STORAGE_FILE_NAME, obj)
        elif self._has_cloudpickle_contents(obj):
            self._delete_file(self._CLOUDPICKLE_STORAGE_FILE_NAME, obj)

    def _storage_path(self, file: str, obj: Node):
        return str((obj.storage_directory.path / file).resolve())

    def _pickle_storage_file_path(self, obj: Node) -> str:
        return self._storage_path(self._PICKLE_STORAGE_FILE_NAME, obj)

    def _cloudpickle_storage_file_path(self, obj: Node) -> str:
        return self._storage_path(self._CLOUDPICKLE_STORAGE_FILE_NAME, obj)

    def _has_contents(self, obj: Node) -> bool:
        return self._has_pickle_contents(obj) or self._has_cloudpickle_contents(obj)

    def _has_pickle_contents(self, obj: Node) -> bool:
        return os.path.isfile(self._pickle_storage_file_path(obj))

    def _has_cloudpickle_contents(self, obj: Node) -> bool:
        return os.path.isfile(self._cloudpickle_storage_file_path(obj))
