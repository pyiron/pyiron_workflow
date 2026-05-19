from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Generic, Protocol, TypeAlias, TypeVar

from flowrep.api import schemas as frs

LexicalPathStr: TypeAlias = str
# TODO: Make a formal lexical path string type that's labels with delimiters,
#       then take frs.Label | LexicalPath here. This is just a placeholder


def lexical_path(*labels: LexicalPathStr) -> LexicalPathStr:
    return ".".join(labels)


class HasLexicalPath(Protocol):
    @property
    def lexical_path(self) -> LexicalPathStr: ...


OwnerType_co = TypeVar("OwnerType_co", bound=HasLexicalPath, covariant=True)  # for maps


class Lexical(Protocol[OwnerType_co]):
    @property
    def label(self) -> frs.Label: ...

    @property
    def owner(self) -> OwnerType_co | None: ...

    @property
    def lexical_path(self) -> LexicalPathStr: ...


LexicalType = TypeVar("LexicalType", bound=Lexical[Any])


class LexicalMap(Mapping[frs.Label, LexicalType], Generic[LexicalType, OwnerType_co]):

    __slots__ = ("_pwf_lexical_map__data", "_pwf_lexical_map__owner")

    _pwf_lexical_map__data: dict[frs.Label, LexicalType]
    _pwf_lexical_map__owner: OwnerType_co

    def __init__(self, owner: OwnerType_co, *items: LexicalType):
        object.__setattr__(self, "_pwf_lexical_map__owner", owner)
        if not_co_owned := {
            i.lexical_path: None if i.owner is None else i.owner.lexical_path
            for i in items
            if i.owner is not owner
        }:
            raise ValueError(
                f"Map owned by {owner.lexical_path!r} cannot be initialized with "
                f"items that have a different owner. item: owner = {not_co_owned}"
            )
        object.__setattr__(self, "_pwf_lexical_map__data", {i.label: i for i in items})

    def __getitem__(self, k):
        if k in self.__slots__:
            raise KeyError(f"Cannot use reserved name {k!r} as a label")
        return self._pwf_lexical_map__data[k]

    def __iter__(self):
        return iter(self._pwf_lexical_map__data)

    def __len__(self):
        return len(self._pwf_lexical_map__data)

    def __getattr__(self, item):
        if item.startswith("_pwf_lexical_map"):
            raise AttributeError(item)
        try:
            return self._pwf_lexical_map__data[item]
        except KeyError:
            raise AttributeError(
                f"{type(self).__name__!r} has no attribute {item!r}"
            ) from None
