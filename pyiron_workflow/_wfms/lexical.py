from __future__ import annotations

import keyword
from collections.abc import Iterable, Mapping
from typing import Any, Generic, Protocol, TypeVar

import flowrep as fr

LEXICAL_PATH_DELIMITER = "."


def _is_valid_segment(seg: str) -> bool:
    # Equivalent to fr.schemas.Label rules with fr.schemas.RESERVED_NAMES (inputs/outputs)
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
                        f"{sorted(fr.schemas.RESERVED_NAMES)} are permitted as path "
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


def lexical_path(*labels: LexicalPath | fr.schemas.Label) -> LexicalPath:
    return LexicalPath(*labels)


def get_label(path: LexicalPath | fr.schemas.Label) -> str:
    return path.rsplit(LEXICAL_PATH_DELIMITER, 1)[-1]


class HasLexicalPath(Protocol):
    @property
    def lexical_path(self) -> LexicalPath: ...


OwnerType = TypeVar("OwnerType", bound=HasLexicalPath)
OwnerType_co = TypeVar("OwnerType_co", bound=HasLexicalPath, covariant=True)  # for maps


class Lexical(Protocol[OwnerType_co]):
    @property
    def label(self) -> fr.schemas.Label: ...

    @property
    def owner(self) -> OwnerType_co | None: ...

    @property
    def lexical_path(self) -> LexicalPath: ...


LexicalType = TypeVar("LexicalType", bound=Lexical[Any])


class LexicalMap(
    Mapping[fr.schemas.Label, LexicalType], Generic[LexicalType, OwnerType]
):

    __slots__ = ("_pwf_lexical_map__data", "_pwf_lexical_map__owner")
    _pwf_lexical_map__data: dict[fr.schemas.Label, LexicalType]
    _pwf_lexical_map__owner: OwnerType

    def __init__(
        self,
        owner: OwnerType,
        data: Mapping[fr.schemas.Label, LexicalType] | None = None,
        /,
    ):
        object.__setattr__(self, "_pwf_lexical_map__owner", owner)
        data = data if data is not None else {}
        check_co_ownership(owner, data.values())
        object.__setattr__(self, "_pwf_lexical_map__data", dict(data))

    def _not_there_message(self, key: str):
        return (
            f"{self.__class__.__name__!r} on "
            f"{self._pwf_lexical_map__owner.lexical_path!r} has no element {key!r}. "
            f"Available elements: {self._pwf_lexical_map__data.keys()}"
        )

    def __getitem__(self, k):
        if k in self.__slots__:
            raise KeyError(f"Cannot use reserved name {k!r} as a label")
        try:
            return self._pwf_lexical_map__data[k]
        except KeyError:
            raise KeyError(self._not_there_message(k)) from None

    def __iter__(self):
        return iter(self._pwf_lexical_map__data)

    def __len__(self):
        return len(self._pwf_lexical_map__data)

    def __getattr__(self, item: str):
        if item.startswith("_pwf_lexical_map"):
            raise AttributeError(item)
        try:
            return self._pwf_lexical_map__data[item]
        except KeyError:
            raise AttributeError(self._not_there_message(item)) from None

    def __str__(self):
        data = "  ".join(
            f"{k!r}: {str(v)}\n" for k, v in self._pwf_lexical_map__data.items()
        )
        return f"{self.__class__.__name__}({self._pwf_lexical_map__owner.lexical_path}): {data}"


def check_co_ownership(owner: HasLexicalPath, items: Iterable[Lexical[Any]]) -> None:
    if not_co_owned := {
        i.lexical_path: i.owner.lexical_path
        for i in items
        if i.owner is not None and i.owner is not owner
    }:
        raise ValueError(
            f"Map owned by {owner.lexical_path!r} cannot be initialized with "
            f"items that have a different owner. item: owner = {not_co_owned}"
        )


_MappedType = TypeVar("_MappedType", bound=Lexical[Any])


def get_item_from_map(
    item: _MappedType | fr.schemas.Label,
    map_: LexicalMap[_MappedType, Any],
    kind: str,
) -> _MappedType:
    if isinstance(item, str):
        return map_[item]
    owned = map_.get(item.label, None)
    if owned is None or item is not owned:
        raise KeyError(
            f"Cannot get {item!r} named {item.label!r} -- no such {kind} is owned."
        )
    return item
