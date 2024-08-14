"""
A bit of abstraction connecting generic storage routines to nodes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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

    def save(self, node: Node):
        directory = node.as_path()
        directory.mkdir(parents=True, exist_ok=True)
        try:
            self._save(node)
        except Exception as e:
            raise e
        finally:
            # If nothing got written due to the exception, clean up the directory
            # (as long as there's nothing else in it)
            if not any(directory.iterdir()):
                directory.rmdir()

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
        directory = node.as_path()
        if directory.exists() and not any(directory.iterdir()):
            directory.rmdir()

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

        directory = node.as_path()
        for file, save_method in [
            (self._PICKLE, pickle.dump),
            (self._CLOUDPICKLE, cloudpickle.dump),
        ]:
            p = directory / file
            try:
                with open(p, "wb") as filehandle:
                    save_method(node, filehandle)
                return
            except Exception as e:
                p.unlink(missing_ok=True)
        raise e

    def _load(self, node: Node) -> Node:
        directory = node.as_path()
        for file, load_method in [
            (self._PICKLE, pickle.load),
            (self._CLOUDPICKLE, cloudpickle.load),
        ]:
            p = directory / file
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
