from semantikon.converter import parse_input_args, parse_output_args
from semantikon.ontology import get_knowledge_graph, SNS
from rdflib import Graph
from pyiron_workflow import Workflow
from pyiron_workflow.api import NOT_DATA, Macro
from pyiron_workflow.node import Node
from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.channels import Channel


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
    data = {"inputs": {}, "outputs": {}}
    is_composite = isinstance(node, Composite)
    for io_ in ["inputs", "outputs"]:
        for inp in getattr(node, io_):
            if is_composite:
                data[io_][inp.scoped_label] = _extract_data(
                    inp, with_values=with_values, with_default=with_default
                )
            else:
                data[io_][inp.label] = _extract_data(
                    inp, with_values=with_values, with_default=with_default
                )
    return data


def _export_node_to_dict(
    node: Node, with_values: bool = True, with_default: bool = True
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
    data = {"inputs": {}, "outputs": {}, "function": node.node_function}
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
    data = {
        "inputs": {},
        "outputs": {},
        "nodes": {},
        "data_edges": [],
        "label": workflow.label,
    }
    for inp in workflow.inputs:
        if inp.value_receiver is not None and inp.value_receiver.owner in workflow:
            data["data_edges"].append(
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
                data["data_edges"].append(
                    (
                        _get_scoped_label(inp.connections[0], "outputs"),
                        _get_scoped_label(inp, "inputs"),
                    )
                )
        for out in node.outputs:
            if out.value_receiver is not None:
                data["data_edges"].append(
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
    workflow: Node, with_values: bool = True, with_default: bool = True
) -> dict:
    if isinstance(workflow, Composite):
        return _export_composite_to_dict(workflow, with_values=with_values)
    return _export_node_to_dict(
        workflow, with_values=with_values, with_default=with_default
    )


def parse_workflow(
    workflow: Workflow,
    with_values: bool = True,
    with_default: bool = True,
    graph: Graph | None = None,
    inherit_properties: bool = True,
    ontology=SNS,
    append_missing_items: bool = True,
) -> Graph:
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
    return get_knowledge_graph(
        wf_dict=wf_dict,
        graph=graph,
        inherit_properties=inherit_properties,
        ontology=ontology,
        append_missing_items=append_missing_items,
    )
