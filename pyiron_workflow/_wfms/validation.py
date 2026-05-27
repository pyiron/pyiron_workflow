from __future__ import annotations

from pyiron_workflow._wfms import workflow
from pyiron_workflow._wfms.datatypes import EdgeTuple, StaticGraph
from pyiron_workflow.type_hinting import type_hint_is_as_or_more_specific_than


def validate_edge(edge: EdgeTuple, owner: StaticGraph | workflow.Workflow) -> EdgeTuple:
    source_node = owner.get_node(edge.source.node) if edge.source.node else owner
    source_port = (
        source_node.get_output(edge.source.port)
        if edge.source.node
        else source_node.get_input(edge.source.port)
    )
    target_node = owner.get_node(edge.target.node) if edge.target.node else owner
    target_port = (
        target_node.get_input(edge.target.port)
        if edge.target.node
        else target_node.get_output(edge.target.port)
    )
    source_hint = source_port.type_hint
    target_hint = target_port.type_hint

    if (
        source_hint is not None
        and target_hint is not None
        and not type_hint_is_as_or_more_specific_than(source_hint, target_hint)
    ):
        raise TypeError(
            "Processing edge "
            f"'{edge.source.serialize()}->{edge.target.serialize()}' on "
            f"{owner.lexical_path!r}, the type hint of the source ({source_hint}) "
            f"is not as-or-more-specific-than the target ({target_hint})."
        )
    return edge
