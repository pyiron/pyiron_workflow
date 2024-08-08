"""
A bit of abstraction to declutter the node class while we support two very different
back ends.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from importlib import import_module
import os
import pickle
import sys
from typing import Optional

import cloudpickle
from pyiron_snippets.files import FileObject, DirectoryObject

from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.has_interface_mixins import HasLabel, HasParent


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface:
    def __init__(self, owner: HasStorage):
        if sys.version_info < (3, 11):
            raise NotImplementedError("Storage is only available in python 3.11+")
        self._owner = owner

    @property
    def owner(self) -> HasStorage:
        # Property access is just to allow children to override the type hint
        return self._owner

    def save(self):
        root = self.owner.storage_root
        if not root.import_ready:
            raise TypeNotFoundError(
                f"{self.owner.label} cannot be saved with the "
                f"{self.owner.storage_backend} because it (or one of its children) has "
                f"a type that cannot be imported. Did you dynamically define this "
                f"object? \n"
                f"Import readiness report: \n"
                f"{self.owner.report_import_readiness()}"
            )
        root_storage = self if root is self.owner else root.storage
        root_storage._save()

    @abstractmethod
    def _save(self):
        pass

    def load(self):
        # Misdirection is strictly for symmetry with _save, so child classes define the
        # private method in both cases
        return self._load()

    @abstractmethod
    def _load(self):
        pass

    @property
    def has_contents(self) -> bool:
        has_contents = self._has_contents
        self.owner.tidy_storage_directory()
        return has_contents

    @property
    @abstractmethod
    def _has_contents(self) -> bool:
        """Whether a save file exists for this backend"""

    def delete(self):
        if self.has_contents:
            self._delete()
        self.owner.tidy_storage_directory()

    @abstractmethod
    def _delete(self):
        """Remove an existing save-file for this backend"""


class PickleStorage(StorageInterface):

    _PICKLE_STORAGE_FILE_NAME = "pickle.pckl"
    _CLOUDPICKLE_STORAGE_FILE_NAME = "cloudpickle.cpckl"

    def __init__(self, owner: HasPickleStorage):
        super().__init__(owner=owner)

    @property
    def owner(self) -> HasPickleStorage:
        return self._owner

    def _save(self):
        try:
            with open(self._pickle_storage_file_path, "wb") as file:
                pickle.dump(self.owner, file)
        except Exception:
            self._delete()
            with open(self._cloudpickle_storage_file_path, "wb") as file:
                cloudpickle.dump(self.owner, file)

    def _load(self):
        if self._has_pickle_contents:
            with open(self._pickle_storage_file_path, "rb") as file:
                inst = pickle.load(file)
        elif self._has_cloudpickle_contents:
            with open(self._cloudpickle_storage_file_path, "rb") as file:
                inst = cloudpickle.load(file)

        if inst.__class__ != self.owner.__class__:
            raise TypeError(
                f"{self.owner.label} cannot load, as it has type "
                f"{self.owner.__class__.__name__},  but the saved node has type "
                f"{inst.__class__.__name__}"
            )
        self.owner.__setstate__(inst.__getstate__())

    def _delete_file(self, file: str):
        FileObject(file, self.owner.storage_directory).delete()

    def _delete(self):
        if self._has_pickle_contents:
            self._delete_file(self._PICKLE_STORAGE_FILE_NAME)
        elif self._has_cloudpickle_contents:
            self._delete_file(self._CLOUDPICKLE_STORAGE_FILE_NAME)

    def _storage_path(self, file: str):
        return str((self.owner.storage_directory.path / file).resolve())

    @property
    def _pickle_storage_file_path(self) -> str:
        return self._storage_path(self._PICKLE_STORAGE_FILE_NAME)

    @property
    def _cloudpickle_storage_file_path(self) -> str:
        return self._storage_path(self._CLOUDPICKLE_STORAGE_FILE_NAME)

    @property
    def _has_contents(self) -> bool:
        return self._has_pickle_contents or self._has_cloudpickle_contents

    @property
    def _has_pickle_contents(self) -> bool:
        return os.path.isfile(self._pickle_storage_file_path)

    @property
    def _has_cloudpickle_contents(self) -> bool:
        return os.path.isfile(self._cloudpickle_storage_file_path)


class HasStorage(HasLabel, HasParent, ABC):
    @classmethod
    def allowed_backends(cls):
        return tuple(cls._storage_interfaces().keys())

    @classmethod
    def _storage_interfaces(cls):
        return {}

    @classmethod
    def default_backend(cls):
        raise NotImplementedError("Exactly one child must define a preferred backend")

    def __init__(self, *args, storage_backend: Optional[str] = None, **kwargs):
        self._storage_backend = None
        super().__init__(*args, **kwargs)
        self.storage_backend = storage_backend

    @property
    @abstractmethod
    def storage_directory(self) -> DirectoryObject:
        # Effectively the working directory, but I want to avoid inheriting from that
        # Clearly there is some bad architecture here, but we'll deal with it later
        pass

    @property
    @abstractmethod
    def storage_path(self) -> str:
        # Effectively the graph path, but I want to avoid inheriting from that
        # Clearly there is some bad architecture here, but we'll deal with it later
        pass

    @abstractmethod
    def tidy_storage_directory(self):
        # Effectively tidy working directory, but I want to avoid inheriting from that
        # Clearly there is some bad architecture here, but we'll deal with it later
        pass

    _save_load_warnings = """
            HERE BE DRAGONS!!!

            Warning:
                This almost certainly only fails for subclasses of :class:`Node` that don't
                override `node_function` or `macro_creator` directly, as these are expected 
                to be part of the class itself (and thus already present on our instantiated 
                object) and are never stored. Nodes created using the provided decorators 
                should all work.

            Warning:
                If you modify a `Macro` class in any way (changing its IO maps, rewiring 
                internal connections, or replacing internal nodes), don't expect 
                saving/loading to work.

            Warning:
                If the underlying source code has changed since saving (i.e. the node doing 
                the loading does not use the same code as the node doing the saving, or the 
                nodes in some node package have been modified), then all bets are off.

            Note:
                Saving and loading `Workflows` only works when all child nodes were created 
                via the creator (and thus have a `package_identifier`). Right now, this is 
                not a big barrier to custom nodes as all you need to do is move them into a 
                .py file, make sure it's in your python path, and :func:`register` it as 
                usual.
        """

    def save(self):
        """
        Writes the node to file (using HDF5) such that a new node instance of the same
        type can :meth:`load()` the data to return to the same state as the save point,
        i.e. the same data IO channel values, the same flags, etc.
        """
        self.storage.save()

    save.__doc__ += _save_load_warnings

    def load(self):
        """
        Loads the node file (from HDF5) such that this node restores its state at time
        of loading.

        Raises:
            TypeError: when the saved node has a different class name.
        """
        if self.storage.has_contents:
            self.storage.load()
        else:
            # Check for saved content using any other backend
            for backend in self.allowed_backends():
                interface = self._storage_interfaces()[backend](self)
                if interface.has_contents:
                    interface.load()
                    break

    save.__doc__ += _save_load_warnings

    def delete_storage(self):
        """Remove save files for _all_ available backends."""
        for backend in self.allowed_backends():
            interface = self._storage_interfaces()[backend](self)
            try:
                interface.delete()
            except FileNotFoundError:
                pass

    @property
    def storage_root(self):
        """The parent-most object that has storage."""
        parent = self.parent
        if isinstance(parent, HasStorage):
            return parent.storage_root
        else:
            return self

    @property
    def storage_backend(self):
        storage_root = self.storage_root
        if storage_root is self:
            backend = self._storage_backend
        else:
            backend = storage_root.storage_backend
        return self.default_backend() if backend is None else backend

    @storage_backend.setter
    def storage_backend(self, new_backend):
        storage_root = self.storage_root
        if new_backend is not None:
            if new_backend not in self.allowed_backends():
                raise ValueError(
                    f"{self.label} got the storage backend {new_backend}, but only "
                    f"{self.allowed_backends()} are permitted."
                )
            elif (
                storage_root is not self and new_backend != storage_root.storage_backend
            ):
                raise ValueError(
                    f"Storage backends should only be set on the storage root "
                    f"({self.storage_root.label}), not on child ({self.label})"
                )
        self._storage_backend = new_backend

    @property
    def storage(self) -> StorageInterface:
        if self.storage_backend is None:
            raise ValueError(f"{self.label} does not have a storage backend set")
        return self._storage_interfaces()[self.storage_backend](self)

    @property
    def any_storage_has_contents(self):
        return any(
            self._storage_interfaces()[backend](self).has_contents
            for backend in self.allowed_backends()
        )

    @property
    def import_ready(self) -> bool:
        """
        Checks whether `importlib` can find this node's class, and if so whether the
        imported object matches the node's type.

        Returns:
            (bool): Whether the imported module and name of this node's class match
                its type.
        """
        try:
            module = self.__class__.__module__
            class_ = getattr(import_module(module), self.__class__.__name__)
            if module == "__main__":
                logger.warning(f"{self.label} is only defined in __main__")
            return type(self) is class_
        except (ModuleNotFoundError, AttributeError):
            return False

    @property
    def import_readiness_report(self):
        print(self.report_import_readiness())

    def report_import_readiness(self, tabs=0, report_so_far=""):
        newline = "\n" if len(report_so_far) > 0 else ""
        tabspace = tabs * "\t"
        return (
            report_so_far + f"{newline}{tabspace}{self.label}: "
            f"{'ok' if self.import_ready else 'NOT IMPORTABLE'}"
        )


class HasPickleStorage(HasStorage, ABC):
    @classmethod
    def _storage_interfaces(cls):
        interfaces = super(HasPickleStorage, cls)._storage_interfaces()
        interfaces["pickle"] = PickleStorage
        return interfaces

    @classmethod
    def default_backend(cls):
        return "pickle"
