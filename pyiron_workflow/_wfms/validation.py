from __future__ import annotations

from typing import Any, NamedTuple

import rdflib
import semantikon
from flowrep.api import schemas as frs
from flowrep.api import tools as frt
from semantikon import flowrep_dict as semantikon2flowrep

from pyiron_workflow._wfms import constructors, execution, workflow
from pyiron_workflow._wfms.datatypes import EdgeTuple, Node, StaticGraph
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


class SemantikonValidationReport(NamedTuple):
    valid: bool
    graph: rdflib.ConjunctiveGraph | rdflib.Graph
    report: str


def _validate_data_ontology(
    data: frs.NodeData[Any],
    with_io: bool,
    with_function: bool,
    label: str | None = None,
    extra_knowledge: rdflib.Graph | None = None,
) -> SemantikonValidationReport:
    as_dict = semantikon2flowrep.node_data_to_dict(
        data,
        with_io=with_io,
        with_function=with_function,
        label=label,
    )
    g = semantikon.get_knowledge_graph(wf_dict=as_dict)
    if extra_knowledge is not None:
        g += extra_knowledge
    return SemantikonValidationReport(*semantikon.validate_values(g))


def validate_ontology(
    target: (
        Node[Any, Any]
        | execution.Run[Any]
        | constructors.RecipeOptions
        | frs.NodeData[Any]
    ),
    with_io: bool = True,
    with_function: bool = True,
    extra_knowledge: rdflib.Graph | None = None,
) -> SemantikonValidationReport:
    if isinstance(target, Node):
        return _validate_data_ontology(
            target.generate_flowrep_live_node(),
            with_io=with_io,
            with_function=with_function,
            label=target.label,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, execution.Run):
        return _validate_data_ontology(
            target.result,
            with_io=with_io,
            with_function=with_function,
            label=target.label,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, constructors.RecipeOptions):
        return _validate_data_ontology(
            frt.recipe2data(recipe=target),
            with_io=with_io,
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, frs.NodeData):
        return _validate_data_ontology(
            target,
            with_io=with_io,
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )
    else:
        raise TypeError(
            f"Unknown target type: {target}. Please provide a {Node.__name__}, "
            f"{execution.Run.__name__}, or {frs.NodeData.__name__}."
        )
