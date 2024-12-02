from __future__ import annotations

import json
from typing import Any

from pyiron_workflow.generic_storage import GenericStorage, StorageGroup


class JSONStorage(GenericStorage):
    def __init__(self, filename: str, mode="r"):
        super().__init__()
        self.file = open(filename, mode)
        self.data: dict = {}

    def close(self):
        self.file.close()

    def __enter__(self) -> JSONGroup:
        if self.file.readable():
            self.data = json.loads(self.file.read())

        return JSONGroup(self.data)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file.writable():
            self.file.write(json.dumps(self.data))
        self.close()


class JSONGroup(StorageGroup):
    def __init__(self, data: dict):
        self.data = data

    def __contains__(self, item: object):
        return item in self.data

    def __delitem__(self, key: str):
        del self.data[key]

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any):
        self.data[key] = value

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def create_group(self, key: str) -> JSONGroup:
        if key in self.data:
            raise KeyError(f"{key} already exists")
        self.data[key] = {}
        return JSONGroup(self.data[key])

    def get(self, key: str, default: Any = None):
        return self.data.get(key, default)

    def require_group(self, key: str) -> JSONGroup:
        return JSONGroup(self.data[key])
