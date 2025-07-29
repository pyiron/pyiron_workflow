"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

import pickle
from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeAlias

import bagofholding
import cloudpickle

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class FileTypeError(TypeError):
    """
    Raised when you're looking for a save file, and you find something, but the file
    appears to be the wrong type.
    """


def delete_files_and_directories_recursively(path: Path):
    """
    Recursively delete all files and directories in the given path.

    Args:
        path (Path): The path to the directory to delete.
    """
    if not path.exists():
        return
    for item in path.rglob("*"):
        if item.is_file():
            item.unlink()
        else:
            delete_files_and_directories_recursively(item)
    path.rmdir()


class StorageInterface(ABC):
    """
    Abstract base class defining the interface for saving, loading, and managing node
    storage.

    Public methods accept `**kwargs` so that specific implementations can have extra
    behaviour. In general, whatever can be set for the kwargs should also be able to
    be specified at initialization as a default, so that users can configure a storage
    back-end the way they want and then run it from there -- kwargs are just exposed to
    make special instances of use easier.
    """

    @abstractmethod
    def _save(self, node: Node, filename: Path, /, *args, **kwargs):
        """
        Save a node to file.

        Args:
            node (Node): The node to save
            filename (Path | None): The path to the save location (WITHOUT file
                extension.)
            **kwargs: Additional keyword arguments.
        """

    @abstractmethod
    def _load(self, filename: Path, /, *args, **kwargs) -> Node:
        """
        Instantiate a node from file.

        Args:
            filename (Path): The path to the file to load (WITHOUT file extension).
            **kwargs: Additional keyword arguments.

        Returns:
            Node: The node stored there.
        """

    @abstractmethod
    def _has_saved_content(self, filename: Path, /, *args, **kwargs) -> bool:
        """
        Check for a save file matching this storage interface.

        Args:
            filename (Path): The path to the file to look for (WITHOUT file extension).
            **kwargs: Additional keyword arguments.

        Returns:
            bool: Whether a commensurate file was found.
        """

    @abstractmethod
    def _delete(self, filename: Path, /, *args, **kwargs):
        """
        Remove an existing save-file for this backend.

        Args:
            filename (Path): The path to the file to delete (WITHOUT file extension).
            **kwargs: Additional keyword arguments.
        """

    def save(self, node: Node, filename: str | Path | None = None, **kwargs):
        """
        Save a node to file.

        Args:
            node (Node): The node to save
            filename (Path | None): The path to the save location (WITHOUT file
                extension.)
            **kwargs: Additional keyword arguments.
        """
        filename = self._parse_filename(
            node=node if filename is None else None,
            filename=filename,
        )
        filename.parent.mkdir(parents=True, exist_ok=True)

        try:
            self._save(node, filename, **kwargs)
        except Exception as e:
            raise e
        finally:
            # If nothing got written due to the exception, clean up the directory
            # (as long as there's nothing else in it)
            if not any(filename.parent.iterdir()):
                filename.parent.rmdir()

    def load(
        self, node: Node | None = None, filename: str | Path | None = None, **kwargs
    ) -> Node:
        """
        Load a node from a file.

        Args:
            node (Node | None): The node to load. Optional if filename is provided.
            filename (str | Path | None): The path to the file to load (without file
                extension). Uses the canonical filename based on the node's lexical
                path instead if this is None.
            **kwargs: Additional keyword arguments.

        Returns:
            Node: The loaded node.
        """
        return self._load(self._parse_filename(node=node, filename=filename), **kwargs)

    def has_saved_content(
        self,
        node: Node | None = None,
        filename: str | Path | None = None,
        **kwargs,
    ) -> bool:
        """
        Check if a file has contents related to a node.

        Args:
            node (Node | None): The node to check. Optional if filename is provided.
            filename (str | Path | None): The path to the file to check (without file
                extension). Optional if the node is provided.
            **kwargs: Additional keyword arguments.

        Returns:
            bool: True if contents exist, False otherwise.
        """
        return self._has_saved_content(
            self._parse_filename(node=node, filename=filename), **kwargs
        )

    def delete(
        self,
        node: Node | None = None,
        filename: str | Path | None = None,
        delete_even_if_not_empty: bool = False,
        **kwargs,
    ):
        """
        Delete a file associated with a node.

        Args:
            node (Node | None): The node whose associated file is to be deleted.
                Optional if filename is provided.
            filename (str | Path | None): The path to the file to delete (without file
                extension). Optional if the node is provided.
            **kwargs: Additional keyword arguments.
        """
        filename = self._parse_filename(node=node, filename=filename)
        if self._has_saved_content(filename, **kwargs):
            self._delete(filename, **kwargs)
        if delete_even_if_not_empty:
            delete_files_and_directories_recursively(filename.parent)
        elif filename.parent.exists() and not any(filename.parent.iterdir()):
            filename.parent.rmdir()

    def _parse_filename(
        self, node: Node | None, filename: str | Path | None = None
    ) -> Path:
        """
        Make sure the node xor filename was provided, and if it's the node, convert it
        into a canonical filename by exploiting the node's lexical path.
        """
        if node is None and filename is None:
            raise ValueError(
                "At least one of node or filename must be specified, or we can't know "
                "where to load from."
            )
        elif node is None and filename is not None:
            if isinstance(filename, Path):
                return filename
            elif isinstance(filename, str):
                return Path(filename)
            else:
                raise TypeError(
                    f"Expected filename to be str, pathlib.Path, or None, but got "
                    f"{filename}"
                )
        elif node is not None and filename is None:
            return node.as_path() / self.__class__.__name__.lower()
        elif node is not None and filename is not None:
            raise ValueError(
                f"Both the node ({node.full_label}) and filename ({filename}) were "
                f"specified for loading -- please only specify one or the other."
            )
        else:
            raise AssertionError(
                "This is an unreachable state -- we have covered all four cases of the "
                "boolean `is (not) None` square."
            )


class PickleStorage(StorageInterface):
    _PICKLE = ".pckl"
    _CLOUDPICKLE = ".cpckl"

    def __init__(self, cloudpickle_fallback: bool = True):
        self.cloudpickle_fallback = cloudpickle_fallback

    def _fallback(self, cpf: bool | None) -> bool:
        return self.cloudpickle_fallback if cpf is None else cpf

    def _save(
        self, node: Node, filename: Path, /, cloudpickle_fallback: bool | None = None
    ):
        if not self._fallback(cloudpickle_fallback) and not node.import_ready:
            raise TypeNotFoundError(
                f"{node.label} cannot be saved with the storage interface "
                f"{self.__class__.__name__} because it (or one of its children) has "
                f"a type that cannot be imported. Is this node defined inside <locals>? "
                f"\n"
                f"Import readiness report: \n"
                f"{node.report_import_readiness()}"
            )

        attacks = [(self._PICKLE, pickle.dump)]
        if self._fallback(cloudpickle_fallback):
            attacks += [(self._CLOUDPICKLE, cloudpickle.dump)]

        e: Exception | None = None
        for suffix, save_method in attacks:
            e = None
            p = filename.with_suffix(suffix)
            try:
                with open(p, "wb") as filehandle:
                    save_method(node, filehandle)
                return
            except Exception as ee:
                e = ee
                p.unlink(missing_ok=True)
        if e is not None:
            raise e

    def _load(
        self, filename: Path, /, cloudpickle_fallback: bool | None = None
    ) -> Node:
        attacks = [(self._PICKLE, pickle.load)]
        if self._fallback(cloudpickle_fallback):
            attacks += [(self._CLOUDPICKLE, cloudpickle.load)]

        for suffix, load_method in attacks:
            p = filename.with_suffix(suffix)
            if p.is_file():
                with open(p, "rb") as filehandle:
                    inst = load_method(filehandle)
                return inst
        raise FileNotFoundError(f"Could not load {filename}, no such file found.")

    def _delete(self, filename: Path, /, cloudpickle_fallback: bool | None = None):
        suffixes = (
            [self._PICKLE, self._CLOUDPICKLE]
            if self._fallback(cloudpickle_fallback)
            else [self._PICKLE]
        )
        for suffix in suffixes:
            filename.with_suffix(suffix).unlink(missing_ok=True)

    def _has_saved_content(
        self, filename: Path, /, cloudpickle_fallback: bool | None = None
    ) -> bool:
        suffixes = (
            [self._PICKLE, self._CLOUDPICKLE]
            if self._fallback(cloudpickle_fallback)
            else [self._PICKLE]
        )
        return any(filename.with_suffix(suffix).exists() for suffix in suffixes)


class H5BagStorage(StorageInterface):
    """
    Storage using :class:`bagofholding.H5Bag`.
    """

    @staticmethod
    def _ensure_file_extension(filename: Path | str) -> str:
        suffix = ".h5"
        fname = str(filename)
        return fname if fname.endswith(suffix) else fname + suffix

    def _save(self, node: Node, filename: Path, /, *args, **kwargs):
        if not node.import_ready:
            raise TypeNotFoundError(
                f"{node.label} cannot be saved with the storage interface "
                f"{self.__class__.__name__} because it (or one of its children) has "
                f"a type that cannot be imported. Is this node defined inside <locals>? "
                f"\n"
                f"Import readiness report: \n"
                f"{node.report_import_readiness()}"
            )
        bagofholding.H5Bag.save(
            node, self._ensure_file_extension(filename), *args, **kwargs
        )

    def _load(self, filename: Path, /, *args, **kwargs) -> Node:
        bag = bagofholding.H5Bag(self._ensure_file_extension(filename))
        return bag.load(*args, **kwargs)

    def _has_saved_content(self, filename: Path) -> bool:
        alleged_location = Path(self._ensure_file_extension(filename))
        if not alleged_location.exists():
            return False
        try:
            bagofholding.H5Bag(alleged_location)
        except Exception as e:
            raise FileTypeError(
                f"Found a file at {alleged_location}, but it could not be resolved as "
                f"a {bagofholding.H5Bag.__name__} bag."
            ) from e
        return True

    def _delete(self, filename: Path):
        Path(self._ensure_file_extension(filename)).unlink(missing_ok=True)


BackendIdentifier: TypeAlias = Literal["h5bag", "pickle"]

_standard_backends: dict[BackendIdentifier, type[StorageInterface]] = {
    "h5bag": H5BagStorage,
    "pickle": PickleStorage,
}


def available_backends(
    backend: BackendIdentifier | StorageInterface | None = None,
    only_requested: bool = False,
) -> Generator[StorageInterface, None, None]:
    """
    A generator for accessing available :class:`StorageInterface` instances, starting
    with the one requested.

    Args:
        backend (BackendIdentifier | StorageInterface | None): The interface to yield
            first.
        only_requested (bool): Stop after yielding whatever was specified by
            :param:`backend`.

    Yields:
        StorageInterface: An interface for serializing :class:`Node`.
    """
    backend_instance = (
        _standard_backends.get(backend, PickleStorage)()
        if isinstance(backend, str)
        else backend
    )

    if backend_instance is not None:
        yield backend_instance
        if only_requested:
            return

    yield from (v() for k, v in _standard_backends.items() if k != backend)
