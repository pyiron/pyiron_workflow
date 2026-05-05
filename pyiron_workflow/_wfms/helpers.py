from typing import Annotated, Any, get_args, get_origin

from semantikon import converter
from semantikon import datastructure as sds


def annotation_to_type_hint(annotation) -> type | None:
    return _unwrap_annotated(annotation) if annotation is not None else None


def _unwrap_annotated(annotation: Any) -> Any:
    """
    Strip `Annotated` wrappers, returning just the base type hint.
    """
    if get_origin(annotation) is Annotated:
        return get_args(annotation)[0]
    return annotation


def annotation_to_type_metadata(annotation) -> sds.TypeMetadata | None:
    return converter.parse_metadata(annotation) if annotation is not None else None
