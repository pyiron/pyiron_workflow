from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

import rdflib
from semantikon import ontology as onto

from pyiron_workflow.data import NOT_DATA, SemantikonRecipeChange
from pyiron_workflow.nodes import for_loop, function, transform, while_loop
from pyiron_workflow.nodes.composite import Composite

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel
    from pyiron_workflow.node import Node
    from pyiron_workflow.workflow import Workflow


def _extract_data(item: Channel, with_values=True, with_default=True) -> dict:
    data = {}
    data_dict = {"default": NOT_DATA, "value": NOT_DATA, "type_hint": None}
    if not with_values:
        data_dict.pop("value")
    if not with_default:
        data_dict.pop("default")
    for key, value in data_dict.items():
        if getattr(item, key) is not value:
            data[key] = getattr(item, key)
    return data


def _is_internal_connection(channel: Channel, workflow: Composite, io_: str) -> bool:
    """
    Check if a channel is connected to another channel in the same workflow.

    Args:
        channel (Channel): The channel to check.
        workflow (Composite): The workflow to check whether the channel is connected to.
        io_ (str): The IO direction to check.

    Returns:
        bool: Whether the channel is connected to another channel in the same workflow.
    """
    if not channel.connected:
        return False
    return any(channel.connections[0] in getattr(n, io_) for n in workflow)


def _get_scoped_label(channel: Channel, io_: str) -> str:
    return channel.scoped_label.replace("__", f".{io_}.")


def _io_to_dict(
    node: Node, with_values: bool = True, with_default: bool = True
) -> dict:
    data: dict[str, dict] = {"inputs": {}, "outputs": {}}
    for io_ in ["inputs", "outputs"]:
        for inp in getattr(node, io_):
            if isinstance(node, Composite):
                data[io_][inp.scoped_label] = _extract_data(
                    inp, with_values=with_values, with_default=with_default
                )
            else:
                data[io_][inp.label] = _extract_data(
                    inp, with_values=with_values, with_default=with_default
                )
    return data


KnownAtomicNodes: TypeAlias = (
    for_loop.For | function.Function | transform.Transformer | while_loop.While
)


def _export_node_to_dict(
    node: KnownAtomicNodes,
    with_values: bool = True,
    with_default: bool = True,
) -> dict:
    """
    Export a node to a dictionary.

    Args:
        node (Node): The node to export.
        with_values (bool): Whether to include the values of the channels in the
            dictionary. (Default is True.)

    Returns:
        dict: The exported node as a dictionary.
    """
    data: dict[str, Any] = {"inputs": {}, "outputs": {}}
    if isinstance(node, function.Function):
        data["function"] = node.node_function
    data.update(_io_to_dict(node, with_values=with_values, with_default=with_default))
    return data


def _export_composite_to_dict(
    workflow: Composite, with_values: bool = True, with_default: bool = True
) -> dict:
    """
    Export a composite to a dictionary.

    Args:
        workflow (Composite): The composite to export.
        with_values (bool): Whether to include the values of the channels in the
            dictionary. (Default is True.)

    Returns:
        dict: The exported composite as a dictionary.
    """
    data: dict[str, Any] = {
        "inputs": {},
        "outputs": {},
        "nodes": {},
        "edges": [],
        "label": workflow.label,
    }
    for inp in workflow.inputs:
        if inp.value_receiver is not None and inp.value_receiver.owner in workflow:
            data["edges"].append(
                (
                    f"inputs.{inp.scoped_label}",
                    _get_scoped_label(inp.value_receiver, "inputs"),
                )
            )
    for node in workflow:
        label = node.label
        if isinstance(node, Composite):
            data["nodes"][label] = _export_composite_to_dict(
                node, with_values=with_values
            )
        else:
            data["nodes"][label] = _export_node_to_dict(node, with_values=with_values)
        for inp in node.inputs:
            if _is_internal_connection(inp, workflow, "outputs"):
                data["edges"].append(
                    (
                        _get_scoped_label(inp.connections[0], "outputs"),
                        _get_scoped_label(inp, "inputs"),
                    )
                )
        for out in node.outputs:
            if out.value_receiver is not None:
                data["edges"].append(
                    (
                        _get_scoped_label(out, "outputs"),
                        f"outputs.{out.value_receiver.scoped_label}",
                    )
                )
    data.update(
        _io_to_dict(workflow, with_values=with_values, with_default=with_default)
    )
    return data


def export_to_dict(
    node: Node, with_values: bool = True, with_default: bool = True
) -> dict:
    if isinstance(node, Composite):
        return _export_composite_to_dict(node, with_values=with_values)
    elif isinstance(node, KnownAtomicNodes):
        return _export_node_to_dict(
            node,
            with_values=with_values,
            with_default=with_default,
        )
    else:
        raise TypeError(f"Unsupported node type: {type(node)}")


def parse_workflow(
    workflow: Workflow,
    with_values: bool = True,
    with_default: bool = True,
    graph: rdflib.Graph | None = None,
    inherit_properties: bool = True,
    ontology=onto.SNS,
    append_missing_items: bool = True,
) -> rdflib.Graph:
    """
    Generate RDF graph from a pyiron workflow object

    Args:
        workflow (pyiron_workflow.workflow.Workflow): workflow object
        with_values (bool): include channel values in the graph
        with_default (bool): include default values in the graph
        graph (rdflib.Graph): graph to add workflow information to
        inherit_properties (bool): inherit properties from the ontology
        ontology (str): ontology to use
        append_missing_items (bool): append missing items for restrictions to
            the ontology

    Returns:
        (rdflib.Graph): graph containing workflow information
    """
    wf_dict = export_to_dict(
        workflow, with_values=with_values, with_default=with_default
    )
    return onto.get_knowledge_graph(
        wf_dict=wf_dict,
        graph=graph,
        inherit_properties=inherit_properties,
        ontology=ontology,
        append_missing_items=append_missing_items,
    )


def validate_workflow(root, new_edge_change: SemantikonRecipeChange | None = None):
    """
    A shortcut for running `semantikon.ontology.validate_values` on a graph generated
    by a `pyiron_workflow` node (the graph root node).

    Takes care of converting the workflow to a compatible representation, and allows
    new edges to be added prior to validation.

    Args:
        root: The workflow or macro to validate.
        new_edge_change: A (semantikon-representation) node path to where the new edge
        should be added.

    Returns:
        dict: The validation report.
    """
    recipe = export_to_dict(
        root,
        with_values=False,
        with_default=False,
    )

    if new_edge_change is not None:
        location = recipe
        path = list(new_edge_change.location[1:])
        while path:
            location = location["nodes"][path.pop(0)]
        location["edges"].append(new_edge_change.new_edge)
        if new_edge_change.parent_input:
            location["inputs"].pop(new_edge_change.parent_input, None)
        if new_edge_change.parent_output:
            location["outputs"].pop(new_edge_change.parent_output, None)

    g = onto.get_knowledge_graph(
        wf_dict=recipe,
        graph=(
            root.knowledge
            if hasattr(root, "knowledge") and isinstance(root.knowledge, rdflib.Graph)
            else None
        ),
    )
    return onto.validate_values(g)


def is_valid(validation: dict[str, Any]) -> bool:
    return (
        len(validation["missing_triples"]) == 0
        and len(validation["incompatible_connections"]) == 0
        and len(validation["distinct_units"]) == 0
    )


def is_involved(validation, new_edge_change: SemantikonRecipeChange) -> bool:
    # We only care if the receiving end of the new edge appears in the validation
    # report.
    # This is still not sufficient, because of limitations in how semantikon treats
    # restrictions: https://github.com/pyiron/semantikon/issues/262
    downstream_term = rdflib.term.URIRef(
        f"{'.'.join(new_edge_change.location)}.{new_edge_change.new_edge[1]}"
    )
    # Also for units the validity report makes reference to the upstream term...
    upstream_term = rdflib.term.URIRef(
        f"{'.'.join(new_edge_change.location)}.{new_edge_change.new_edge[0]}"
    )
    return term_appears_in_validation(
        downstream_term, validation
    ) or _target_in_distinct_units(upstream_term, validation)


def term_appears_in_validation(
    term: rdflib.term.Node, validation: dict[str, Any]
) -> bool:
    return (
        _target_in_missing_triples(term, validation)
        or _target_in_incompatible_connections(term, validation)
        or _target_in_distinct_units(term, validation)
    )


def _target_in_missing_triples(target, validation):
    for triple in validation["missing_triples"]:
        if any(target == t for t in triple):
            return True
    return False


def _target_in_incompatible_connections(target, validation):
    for connection_report in validation["incompatible_connections"]:
        if any(target == t for t in connection_report[:2]):
            return True
    return False


def _target_in_distinct_units(target, validation):
    unit_target = rdflib.term.URIRef(f"{target}.value")
    return any(unit_target == t for t in validation["distinct_units"])
