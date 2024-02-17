"""
A bit of abstraction to declutter the node class while we support two very different
back ends.
"""

from __future__ import annotations

import os
import sys
from typing import Literal, TYPE_CHECKING

import h5io

from pyiron_workflow.snippets.files import FileObject

if TYPE_CHECKING:
    from pyiron_workflow.node import Node

ALLOWED_BACKENDS = ["h5io", "tinybase"] if sys.version_info >= (3, 11) else ["h5io"]


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface:

    _TINYBASE_STORAGE_FILE_NAME = "project.h5"
    _H5IO_STORAGE_FILE_NAME = "h5io.h5"

    def __init__(self, node: Node):
        if sys.version_info < (3, 11):
            raise NotImplementedError("Storage is only available in python 3.11+")
        self.node = node

    def save(self, backend: Literal["h5io", "tinybase"]):
        if backend not in ALLOWED_BACKENDS:
            raise ValueError(
                f"Backend {backend} not recognized, please use one of "
                f"{ALLOWED_BACKENDS}."
            )

        if self.node.parent is None:
            self._save(backend=backend)
        else:
            root = self.node.graph_root
            root.storage.save(backend=backend)

    def _save(self, backend: Literal["h5io", "tinybase"]):
        if not self.node.import_ready:
            raise TypeNotFoundError(
                f"{self.node.label} cannot be saved because it (or one "
                f"of its child nodes) has a type that cannot be imported. Did you "
                f"dynamically define this node? Try using the node wrapper as a "
                f"decorator instead. \n"
                f"Import readiness report: \n"
                f"{self.node._report_import_readiness()}"
            )
        if backend == "h5io":
            h5io.write_hdf5(
                fname=self._h5io_storage_file_path,
                data=self.node,
                title=self.node.label,
                use_state=True,
                overwrite=True,  # Don't worry about efficiency or updating yet
            )
        elif backend == "tinybase":
            os.makedirs(
                os.path.dirname(self._tinybase_storage_file_path), exist_ok=True
            )  # Make sure the path to the storage location exists
            self.node.to_storage(self._tinybase_storage)
        else:
            raise ValueError(
                f"Backend {backend} not recognized, please use 'h5io' or 'tinybase'."
            )

    def load(self, backend: Literal["h5io", "tinybase"]):
        if backend not in ALLOWED_BACKENDS:
            raise ValueError(
                f"Backend {backend} not recognized, please use one of "
                f"{ALLOWED_BACKENDS}."
            )
        elif backend == "h5io":
            inst = h5io.read_hdf5(
                fname=self._h5io_storage_file_path, title=self.node.label
            )
            self.node.__setstate__(inst.__getstate__())
        elif backend == "tinybase":
            tinybase_storage = self._tinybase_storage
            if tinybase_storage["class_name"] != self.node.class_name:
                raise TypeError(
                    f"{self.node.label} cannot load, as it has type "
                    f"{self.node.class_name},  but the saved node has type "
                    f"{tinybase_storage['class_name']}"
                )
            self.node.from_storage(tinybase_storage)

    @property
    def has_contents(self) -> bool:
        has_contents = self._tinybase_storage_is_there or self._h5io_storage_is_there
        self.node.tidy_working_directory()
        return has_contents

    def delete(self):
        if self._tinybase_storage_is_there:
            up = self._tinybase_storage.close()
            del up[self.node.label]
            if self.node.parent is None:
                FileObject(
                    self._TINYBASE_STORAGE_FILE_NAME, self.node.working_directory
                ).delete()
        if self._h5io_storage_is_there:
            FileObject(
                self._H5IO_STORAGE_FILE_NAME, self.node.working_directory
            ).delete()
        self.node.tidy_working_directory()

    @property
    def _h5io_storage_file_path(self) -> str:
        return str(
            (self.node.working_directory.path / self._H5IO_STORAGE_FILE_NAME).resolve()
        )

    @property
    def _h5io_storage_is_there(self) -> bool:
        return os.path.isfile(self._h5io_storage_file_path)

    @property
    def _tinybase_storage_file_path(self) -> str:
        return str(
            (
                self.node.graph_root.working_directory.path
                / self._TINYBASE_STORAGE_FILE_NAME
            ).resolve()
        )

    @property
    def _tinybase_storage(self):
        from pyiron_contrib.tinybase.storage import H5ioStorage
        from h5io_browser import Pointer

        return H5ioStorage(
            Pointer(self._tinybase_storage_file_path, h5_path=self.node.graph_path),
            None,
        )

    @property
    def _tinybase_storage_is_there(self) -> bool:
        if os.path.isfile(self._tinybase_storage_file_path):
            storage = self._tinybase_storage
            return (len(storage.list_groups()) + len(storage.list_nodes())) > 0
        else:
            return False
