from __future__ import annotations

import abc
from collections.abc import MutableMapping
from typing import Any


class StorageGroup(abc.ABC, MutableMapping[str, Any]):
    """API for organizing/loading/storing stuff"""

    @abc.abstractmethod
    def create_group(self, key: str) -> StorageGroup:
        pass

    @abc.abstractmethod
    def get(self, key: str, default: Any = None):
        pass

    @abc.abstractmethod
    def require_group(self, key: str) -> StorageGroup:
        pass


class GenericStorage(abc.ABC):
    """Context manager for the storage blob, i.e. file open/close, database connection, etc"""

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def __enter__(self) -> StorageGroup:
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass
