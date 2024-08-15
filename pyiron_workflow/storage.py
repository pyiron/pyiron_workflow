"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
import pickle
from typing import Generator, Literal, TYPE_CHECKING

import cloudpickle

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class TypeNotFoundError(ImportError):
    """
    Raised when you try to save a node, but importing its module and class give
    something other than its type.
    """


class StorageInterface(ABC):

    @abstractmethod
    def _save(self, node: Node, filename: Path, /, **kwargs):
        """
        Save a node to file.

        Args:
            node (Node): The node to save
            filename (Path | None): The path to the save location (WITHOUT file
                extension.)
        """
        pass

    @abstractmethod
    def _load(self, filename: Path, /, **kwargs) -> Node:
        """
        Instantiate a node from file.

        Args:
            filename (Path): The path to the file to load (WITHOUT file extension).

        Returns:
            Node: The node stored there.
        """
        pass

    @abstractmethod
    def _has_contents(self, filename: Path, /, **kwargs) -> bool:
        pass

    @abstractmethod
    def _delete(self, filename: Path, /, **kwargs):
        """Remove an existing save-file for this backend"""

    def save(
        self,
        node: Node,
        filename: str | Path | None = None,
        **kwargs
    ):
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
        self,
        node: Node | None = None,
        filename: str | Path | None = None,
        **kwargs
    ) -> Node:
        return self._load(
            self._parse_filename(node=node, filename=filename),
            **kwargs
        )

    def has_contents(
        self,
        node: Node | None = None,
        filename: str | Path | None = None,
        **kwargs,
    ):
        return self._has_contents(
            self._parse_filename(node=node, filename=filename),
            **kwargs
        )

    def delete(
        self,
        node: Node | None = None,
        filename: str | Path | None = None,
        **kwargs
    ):
        filename = self._parse_filename(node=node, filename=filename)
        if self._has_contents(filename, **kwargs):
            self._delete(filename, **kwargs)
        if filename.parent.exists() and not any(filename.parent.iterdir()):
            filename.parent.rmdir()

    def _parse_filename(self, node: Node | None, filename: str | Path | None = None):
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


class PickleStorage(StorageInterface):

    _PICKLE = ".pckl"
    _CLOUDPICKLE = ".cpckl"

    def __init__(self, cloudpickle_fallback: bool = True):
        self.cloudpickle_fallback = cloudpickle_fallback

    def _fallback(self, cpf: bool | None):
        return self.cloudpickle_fallback if cpf is None else cpf

    def _save(
        self,
        node: Node,
        filename: Path,
        cloudpickle_fallback: bool | None = None
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

        for suffix, save_method in attacks:
            p = filename.with_suffix(suffix)
            try:
                with open(p, "wb") as filehandle:
                    save_method(node, filehandle)
                return
            except Exception as e:
                p.unlink(missing_ok=True)
        raise e

    def _load(self, filename: Path, cloudpickle_fallback: bool | None = None) -> Node:
        attacks = [(self._PICKLE, pickle.load)]
        if self._fallback(cloudpickle_fallback):
            attacks += [(self._CLOUDPICKLE, cloudpickle.load)]

        for suffix, load_method in attacks:
            p = filename.with_suffix(suffix)
            if p.exists():
                with open(p, "rb") as filehandle:
                    inst = load_method(filehandle)
                return inst

    def _delete(self, filename: Path, cloudpickle_fallback: bool | None = None):
        suffixes = (
            [self._PICKLE, self._CLOUDPICKLE] if self._fallback(cloudpickle_fallback)
            else [self._PICKLE]
        )
        for suffix in suffixes:
            filename.with_suffix(suffix).unlink(missing_ok=True)

    def _has_contents(
        self,
        filename: Path,
        cloudpickle_fallback: bool | None = None
    ) -> bool:
        suffixes = (
            [self._PICKLE, self._CLOUDPICKLE] if self._fallback(cloudpickle_fallback)
            else [self._PICKLE]
        )
        return any(
            filename.with_suffix(suffix).exists()
            for suffix in suffixes
        )


def available_backends(
    backend: Literal["pickle"] | StorageInterface | None = None,
    only_requested: bool = False,
) -> Generator[StorageInterface, None, None]:
    """
    A generator for accessing available :class:`StorageInterface` instances, starting
    with the one requested.

    Args:
        backend (Literal["pickle"] | StorageInterface | None): The interface to yield
            first.
        only_requested (bool): Stop after yielding whatever was specified by
            :param:`backend`.

    Yields:
        StorageInterface: An interface for serializing :class:`Node`.
    """

    standard_backends = {"pickle": PickleStorage}

    def yield_requested():
        if isinstance(backend, str):
            yield standard_backends[backend]()
        elif isinstance(backend, StorageInterface):
            yield backend

    if backend is not None:
        yield from yield_requested()
        if only_requested:
            return

    for key, value in standard_backends.items():
        if (
            backend is None
            or (isinstance(backend, str) and key != backend)
            or (isinstance(backend, StorageInterface) and value != backend)
        ):
            yield value()
