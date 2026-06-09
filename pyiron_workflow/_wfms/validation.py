from __future__ import annotations

import dataclasses
from typing import Any, ClassVar

import rdflib
import semantikon
from flowrep.api import schemas as frs
from flowrep.api import tools as frt

from pyiron_workflow._wfms import atomic, constructors, dag, execution, workflow
from pyiron_workflow._wfms.datatypes import EdgeList, EdgeTuple, Node, StaticGraph
from pyiron_workflow.type_hinting import type_hint_is_as_or_more_specific_than


def _resolve_edge_hints(
    edge: EdgeTuple, owner: StaticGraph | workflow.Workflow
) -> tuple[type | None, type | None]:
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
    return source_port.type_hint, target_port.type_hint


def validate_edge(
    edge: EdgeTuple,
    owner: StaticGraph | workflow.Workflow,
    strict: bool = False,
) -> EdgeTuple:
    source_hint, target_hint = _resolve_edge_hints(edge, owner)

    if source_hint is not None and target_hint is not None:
        if not type_hint_is_as_or_more_specific_than(source_hint, target_hint):
            raise TypeError(
                "Processing edge "
                f"'{edge.source.serialize()}->{edge.target.serialize()}' on "
                f"{owner.lexical_path!r}, the type hint of the source ({source_hint}) "
                f"is not as-or-more-specific-than the target ({target_hint})."
            )
    elif strict and target_hint is not None and source_hint is None:
        raise TypeError(
            "Processing edge "
            f"'{edge.source.serialize()}->{edge.target.serialize()}' on "
            f"{owner.lexical_path!r} in strict mode, the target requests a type hint "
            f"({target_hint}) but the source provides none."
        )
    return edge


class NotParseable:
    valid: ClassVar[bool] = True  # no *detectable* type error
    complete: ClassVar[bool] = False  # but it could not be checked

    def __repr__(self) -> str:
        return "<NOT PARSEABLE>"


@dataclasses.dataclass(frozen=True)
class TypeValidationReport:
    name: str
    invalid_edges: EdgeList
    unfulfilled_edges: EdgeList
    subreports: dict[str, TypeValidationReport | NotParseable]
    depth: int = 0

    @property
    def valid(self) -> bool:
        return not self.invalid_edges and all(s.valid for s in self.subreports.values())

    @property
    def complete(self) -> bool:
        return not self.unfulfilled_edges and all(
            s.complete for s in self.subreports.values()
        )

    @property
    def text(self) -> str:
        indent = "\t" * self.depth
        header = (
            f"{indent}Type validation for {self.name!r} "
            f"(valid={self.valid}, complete={self.complete})"
        )
        if (
            not self.invalid_edges
            and not self.unfulfilled_edges
            and not self.subreports
        ):
            return f"{header}: OK"

        lines = [f"{header}:"]
        for title, edges in (
            ("invalid edges", self.invalid_edges),
            ("unfulfilled edges", self.unfulfilled_edges),
        ):
            if edges:
                lines.append(f"{indent}\t{title}:")
                lines.extend(
                    f"{indent}\t\t{edge.source.serialize()}->{edge.target.serialize()}"
                    for edge in edges
                )
        if self.subreports:
            lines.append(f"{indent}\tsubreports:")
            for label, sub in self.subreports.items():
                if isinstance(sub, NotParseable):
                    lines.append(f"{indent}\t{label}: {sub!r}")
                else:
                    # `sub.text` already self-indents to `depth + 1`.
                    lines.append(sub.text)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self.text


def validate_types(
    target: (
        atomic.Atomic
        | dag.Macro
        | workflow.Workflow  # Prospective nodes
        | frs.WorkflowRecipe  # Prospective flowrep recipes
    ),
) -> TypeValidationReport:
    if isinstance(target, atomic.Atomic):
        # An Atomic has no internal structure, so it is trivially valid.
        return TypeValidationReport(target.lexical_path, [], [], {})
    if isinstance(target, dag.Macro | workflow.Workflow):
        owner: StaticGraph | workflow.Workflow = target
    elif isinstance(target, frs.WorkflowRecipe):
        owner = dag.Macro("from_recipe", target)
    else:
        raise TypeError(
            f"Cannot validate types for {target!r}; expected a "
            f"{atomic.Atomic.__name__}, {dag.Macro.__name__}, "
            f"{workflow.Workflow.__name__}, or {frs.WorkflowRecipe.__name__}."
        )
    return _validate_graph(owner, depth=0)


def _validate_graph(
    owner: StaticGraph | workflow.Workflow, depth: int
) -> TypeValidationReport:
    invalid_edges: EdgeList = []
    unfulfilled_edges: EdgeList = []
    subreports: dict[str, TypeValidationReport | NotParseable] = {}

    for edge in owner.edges:
        source_hint, target_hint = _resolve_edge_hints(edge, owner)
        if source_hint is not None and target_hint is not None:
            if not type_hint_is_as_or_more_specific_than(source_hint, target_hint):
                invalid_edges.append(edge)
        elif target_hint is not None and source_hint is None:
            unfulfilled_edges.append(edge)

    for label, node in owner.nodes.items():
        if isinstance(node, atomic.Atomic):
            continue
        elif isinstance(node, dag.Macro | workflow.Workflow):
            subreports[label] = _validate_graph(node, depth=depth + 1)
        else:
            subreports[label] = NotParseable()

    return TypeValidationReport(
        owner.lexical_path, invalid_edges, unfulfilled_edges, subreports, depth=depth
    )


@dataclasses.dataclass(frozen=True)
class SemantikonValidationReport:
    valid: bool
    graph: rdflib.ConjunctiveGraph | rdflib.Graph
    text: str

    def __repr__(self):
        return self.text


def _validate_data_ontology(
    data: frs.NodeData[Any],
    with_function: bool,
    label: str | None = None,
    extra_knowledge: rdflib.Graph | None = None,
) -> SemantikonValidationReport:
    as_dict = semantikon.nodedata2dict(
        data,
        with_function=with_function,
        label=label,
    )
    g = semantikon.get_knowledge_graph(wf_dict=as_dict)
    if extra_knowledge is not None:
        g += extra_knowledge
    semantikon_report = semantikon.validate_values(g)
    return SemantikonValidationReport(
        valid=semantikon_report[0],
        graph=semantikon_report[1],
        text=semantikon_report[2],
    )


def validate_ontology(
    target: (
        Node[Any, Any]
        | execution.Run[Any]
        | constructors.RecipeOptions
        | frs.NodeData[Any]
    ),
    with_function: bool = True,
    extra_knowledge: rdflib.Graph | None = None,
) -> SemantikonValidationReport:
    if isinstance(target, Node):
        return _validate_data_ontology(
            target.generate_flowrep_live_node(),
            with_function=with_function,
            label=target.label,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, execution.Run):
        return _validate_data_ontology(
            target.result,
            with_function=with_function,
            label=target.label,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, constructors.RecipeOptions):
        return _validate_data_ontology(
            frt.recipe2data(recipe=target),
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )
    elif isinstance(target, frs.NodeData):
        return _validate_data_ontology(
            target,
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )
    else:
        raise TypeError(
            f"Unknown target type: {target}. Please provide a {Node.__name__}, "
            f"{execution.Run.__name__}, or {frs.NodeData.__name__}."
        )


@dataclasses.dataclass
class CombinedValidationReport:
    types: TypeValidationReport | None
    metadata: SemantikonValidationReport | None

    @property
    def valid(self) -> bool:
        return all(r.valid for r in (self.types, self.metadata) if r is not None)

    def __repr__(self):
        return f"{self.types}\n{self.metadata}"


def validate_plan(
    target: atomic.Atomic | dag.Macro | workflow.Workflow,
    do_types: bool = True,
    do_ontology: bool = True,
    with_function: bool = True,
    extra_knowledge: rdflib.Graph | None = None,
) -> CombinedValidationReport:
    types_report = validate_types(target) if do_types else None
    onto_report = (
        validate_ontology(
            target,
            with_function=with_function,
            extra_knowledge=extra_knowledge,
        )
        if do_ontology
        else None
    )
    return CombinedValidationReport(types_report, onto_report)
