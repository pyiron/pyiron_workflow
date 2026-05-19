from __future__ import annotations

import keyword
from collections.abc import Mapping
from typing import Any, Generic, Protocol, TypeVar

from flowrep.api import schemas as frs

LEXICAL_PATH_DELIMITER = "."


def _is_valid_segment(seg: str) -> bool:
    # Equivalent to frs.Label rules with frs.RESERVED_NAMES (inputs/outputs)
    # exempted, since port paths legitimately contain the IO indicator.
    # NOTE: does not auto-track future flowrep label-rule changes beyond
    # identifier+keyword.
    return seg.isidentifier() and not keyword.iskeyword(seg)


class LexicalPath(str):
    """A delimiter-joined chain of identifier segments.

    Drop-in ``str`` (comparison, ``.replace``, f-strings, dict keys all work);
    construction validates every segment. Empty path == "" is permitted and
    represents "no path/root".
    """

    __slots__ = ()

    def __new__(cls, *parts: str) -> LexicalPath:
        segments: list[str] = []
        for part in parts:
            if part == "":
                continue  # tolerate the empty-root sentinel when concatenating
            for seg in part.split(LEXICAL_PATH_DELIMITER):
                if not _is_valid_segment(seg):
                    raise ValueError(
                        f"Invalid lexical path segment {seg!r} in {part!r}: "
                        f"each segment must be a valid Python identifier and "
                        f"not a keyword (reserved IO names "
                        f"{sorted(frs.RESERVED_NAMES)} are permitted as path "
                        f"segments)."
                    )
                segments.append(seg)
        return super().__new__(cls, LEXICAL_PATH_DELIMITER.join(segments))

    @property
    def segments(self) -> tuple[str, ...]:
        return tuple(self.split(LEXICAL_PATH_DELIMITER)) if self else ()

    @property
    def label(self) -> str:
        return self.rsplit(LEXICAL_PATH_DELIMITER, 1)[-1]

    @property
    def parent(self) -> LexicalPath:
        return LexicalPath(*self.segments[:-1])


def lexical_path(*labels: LexicalPath | frs.Label) -> LexicalPath:
    return LexicalPath(*labels)


def get_label(path: LexicalPath | frs.Label) -> str:
    return path.rsplit(LEXICAL_PATH_DELIMITER, 1)[-1]


class HasLexicalPath(Protocol):
    @property
    def lexical_path(self) -> LexicalPath: ...


OwnerType_co = TypeVar("OwnerType_co", bound=HasLexicalPath, covariant=True)  # for maps


class Lexical(Protocol[OwnerType_co]):
    @property
    def label(self) -> frs.Label: ...

    @property
    def owner(self) -> OwnerType_co | None: ...

    @property
    def lexical_path(self) -> LexicalPath: ...


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
