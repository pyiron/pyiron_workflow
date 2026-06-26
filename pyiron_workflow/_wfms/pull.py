from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import flowrep as fr
import typing_extensions

from pyiron_workflow._wfms import execution, workflow
from pyiron_workflow._wfms.datatypes import (
    EdgeList,
    EdgeTuple,
    Graph,
    ImmutableDag,
    MutableDag,
    Node,
)

if TYPE_CHECKING:
    import semantikon

    from pyiron_workflow._wfms.datatypes import InputPort


@dataclasses.dataclass
class _Cone:
    """Accumulates the flattened dependency cone of a pulled node."""

    pulled_label: str
    members: dict[str, Node] = dataclasses.field(default_factory=dict)
    internal_edges: EdgeList = dataclasses.field(default_factory=list)
    input_specs: dict[str, tuple[type | None, semantikon.TypeMetadata | None]] = (
        dataclasses.field(default_factory=dict)
    )
    input_edges: EdgeList = dataclasses.field(default_factory=list)


def _ceiling(node: Node, break_out_of_context: bool) -> Graph | None:
    if break_out_of_context:
        root: Graph | None = node.owner
        while isinstance(root, Node) and root.owner is not None:
            root = root.owner
        return root
    else:
        return node.owner


def _relative(node: Node, ceiling: Graph | None) -> str:
    """Flat label relative to `ceiling`; '' iff `node is ceiling`."""
    if ceiling is None:
        return ""
    prefix = ceiling.lexical_path
    return node.lexical_path[len(prefix) + 1 :].replace(".", "__")


def _member_label(node: Node, ceiling: Graph | None) -> str:
    return _relative(node, ceiling) or node.label


def _is_traceable(graph: object) -> typing_extensions.TypeIs[ImmutableDag | MutableDag]:
    """Whether a graph exposes concrete (non-prospective) edges we may walk."""
    return isinstance(graph, ImmutableDag | MutableDag)


def _incoming_edge(graph: Graph, node_label: str, port: str) -> EdgeTuple | None:
    target = fr.schemas.TargetHandle(node=node_label, port=port)
    for edge in graph.edges:
        if edge.target == target:
            return edge
    return None


def _flow_control_error(controller: Graph, pulled: Node) -> ValueError:
    return ValueError(
        f"Cannot pull {pulled.lexical_path!r} out of the flow controller "
        f"{controller.lexical_path!r} (a {type(controller).__name__}): a pull cannot "
        f"break out of a flow controller's context. Use break_out_of_context=False "
        f"to pull in isolation, supplying its inputs directly."
    )


def _add_input(
    cone: _Cone,
    key: str,
    target_node_label: str,
    target_port_label: str,
    hint: type | None,
    metadata: semantikon.TypeMetadata | None,
) -> None:
    cone.input_specs.setdefault(key, (hint, metadata))
    cone.input_edges.append(
        EdgeTuple(
            fr.schemas.InputSource(port=key),
            fr.schemas.TargetHandle(node=target_node_label, port=target_port_label),
        )
    )


def _require(
    member: Node, port_label: str, port: InputPort, ceiling: Graph | None, cone: _Cone
) -> None:
    """Surface a genuinely-unfed input port as a required workflow input."""
    rel = _relative(member, ceiling)
    node_label = rel or member.label
    key = f"{rel}__{port_label}" if rel else port_label
    _add_input(cone, key, node_label, port_label, port.type_hint, port.type_metadata)


def _add_dependency(
    dep: Node,
    dep_port: str,
    consumer_label: str,
    consumer_port: str,
    ceiling: Graph | None,
    cone: _Cone,
    worklist: list[Node],
    seen: set[str],
) -> None:
    dep_label = _member_label(dep, ceiling)
    cone.internal_edges.append(
        EdgeTuple(
            fr.schemas.SourceHandle(node=dep_label, port=dep_port),
            fr.schemas.TargetHandle(node=consumer_label, port=consumer_port),
        )
    )
    if dep_label not in seen:
        seen.add(dep_label)
        worklist.append(dep)


def _resolve_boundary(
    graph: ImmutableDag | MutableDag,
    boundary_port: str,
    consumer_label: str,
    consumer_port: str,
    consumer_port_obj: InputPort,
    ceiling: Graph | None,
    cone: _Cone,
    worklist: list[Node],
    seen: set[str],
    pulled: Node,
) -> None:
    parent = graph.owner  # the subgraph `graph` is itself a child of `parent`
    if (
        graph is ceiling
        or parent is None
        # These are _equivalent conditions_ -- if the owner is None, this is the ceiling
    ):
        ceiling_port = graph.inputs[boundary_port]
        _add_input(
            cone,
            boundary_port,
            consumer_label,
            consumer_port,
            ceiling_port.type_hint,
            ceiling_port.type_metadata,
        )
        return

    if not _is_traceable(parent):
        raise _flow_control_error(parent, pulled)

    edge = _incoming_edge(parent, graph.label, boundary_port)
    if edge is None:
        _add_input(
            cone,
            f"{consumer_label}__{consumer_port}",
            consumer_label,
            consumer_port,
            consumer_port_obj.type_hint,
            consumer_port_obj.type_metadata,
        )
        return
    source = edge.source
    if isinstance(source, fr.schemas.SourceHandle):
        dep = parent.nodes[source.node]
        _add_dependency(
            dep,
            source.port,
            consumer_label,
            consumer_port,
            ceiling,
            cone,
            worklist,
            seen,
        )
    else:  # fr.schemas.InputSource — keep climbing
        _resolve_boundary(
            parent,
            source.port,
            consumer_label,
            consumer_port,
            consumer_port_obj,
            ceiling,
            cone,
            worklist,
            seen,
            pulled,
        )


def _build_cone(
    node: Node, break_out_of_context: bool, expose_defaults: bool
) -> tuple[_Cone, Graph | None]:
    ceiling = _ceiling(node, break_out_of_context)
    cone = _Cone(pulled_label=_member_label(node, ceiling))
    seen = {cone.pulled_label}
    worklist = [node]
    while worklist:
        member = worklist.pop()
        cone.members[_member_label(member, ceiling)] = member
        for port_label, port in member.inputs.items():
            _resolve_input(
                member,
                port_label,
                port,
                ceiling,
                break_out_of_context,
                expose_defaults,
                cone,
                worklist,
                seen,
                node,
            )
    return cone, ceiling


def _resolve_input(
    member: Node,
    port_label: str,
    port: InputPort,
    ceiling: Graph | None,
    break_out: bool,
    expose_defaults: bool,
    cone: _Cone,
    worklist: list[Node],
    seen: set[str],
    pulled: Node,
) -> None:
    if port.has_default and not expose_defaults:
        return

    graph = member.owner
    if graph is None:
        _require(member, port_label, port, ceiling, cone)
        return

    if not _is_traceable(graph):
        # `graph` is a flow controller: its edges are prospective and may not be
        # walked. Punching out is impossible; stopping isolates the node.
        if break_out:
            raise _flow_control_error(graph, pulled)
        _require(member, port_label, port, ceiling, cone)
        return

    # else _is_traceable(graph) and graph: ImmutableDag | MutableDag
    edge = _incoming_edge(graph, member.label, port_label)
    if edge is None:
        _require(member, port_label, port, ceiling, cone)
        return
    source = edge.source
    member_label = _member_label(member, ceiling)
    if isinstance(source, fr.schemas.SourceHandle):
        dep = graph.nodes[source.node]
        _add_dependency(
            dep, source.port, member_label, port_label, ceiling, cone, worklist, seen
        )
    else:  # fr.schemas.InputSource
        _resolve_boundary(
            graph,
            source.port,
            member_label,
            port_label,
            port,
            ceiling,
            cone,
            worklist,
            seen,
            pulled,
        )


def pulled_workflow(
    node: Node, break_out_of_context: bool = False, expose_defaults: bool = False, /
) -> workflow.Workflow:
    cone, _ = _build_cone(node, break_out_of_context, expose_defaults)
    wf = workflow.Workflow(f"pulled_{node.label}")
    for label, member in cone.members.items():
        wf.add_node(member.copy(label))
    if cone.internal_edges:
        wf.add_edge(*cone.internal_edges, type_validate=False)
    for key, (hint, metadata) in cone.input_specs.items():
        wf.create_input(key, type_hint=hint, type_metadata=metadata)
    if cone.input_edges:
        wf.add_edge(*cone.input_edges, type_validate=False)
    for port_label, out_port in node.outputs.items():
        wf.create_output(
            port_label,
            type_hint=out_port.type_hint,
            type_metadata=out_port.type_metadata,
        )
        wf.add_edge(
            EdgeTuple(
                fr.schemas.SourceHandle(node=cone.pulled_label, port=port_label),
                fr.schemas.OutputTarget(port=port_label),
            ),
            type_validate=False,
        )
    return wf


def pulled_inputs(
    node: Node, break_out_of_context: bool = False, expose_defaults: bool = False, /
):
    return pulled_workflow(node, break_out_of_context, expose_defaults).inputs


def pull(
    node: Node,
    config: execution.RunConfig | None = None,
    break_out_of_context: bool = False,
    expose_defaults: bool = False,
    /,
    **input_kwargs: object,
) -> execution.Run:
    wf = pulled_workflow(node, break_out_of_context, expose_defaults)
    needed = set(wf.inputs)
    provided = set(input_kwargs)
    if unknown := provided - needed:
        raise ValueError(
            f"Unexpected pull input(s) {sorted(unknown)} for {node.lexical_path!r}. "
            f"Valid keys are {sorted(needed)}; inspect them with `pulled_inputs`."
        )
    if missing := needed - provided:
        raise ValueError(
            f"Missing required pull input(s) {sorted(missing)} for "
            f"{node.lexical_path!r}. Required keys are {sorted(needed)}; inspect them "
            f"with `pulled_inputs`."
        )
    return wf.run(config, **input_kwargs)
