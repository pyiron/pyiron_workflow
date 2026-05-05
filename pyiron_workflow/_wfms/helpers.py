from semantikon import converter, flowrep_dict
from semantikon import datastructure as sds


def annotation_to_type_hint(annotation) -> type | None:
    return (
        flowrep_dict._unwrap_annotated(annotation) if annotation is not None else None
    )


def annotation_to_type_metadata(annotation) -> sds.TypeMetadata | None:
    return converter.parse_metadata(annotation) if annotation is not None else None
