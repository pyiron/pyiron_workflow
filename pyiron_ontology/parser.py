from semantikon.converter import parse_input_args, parse_output_args
from semantikon.ontology import get_knowledge_graph, PNS
from rdflib import Graph
from pyiron_workflow import NOT_DATA, Workflow, Macro
from pyiron_workflow.node import Node


def get_source_output(var: Node) -> str | None:
    if not var.connected:
        return None
    connection = var.connections[0]
    connection_name = f"{connection.owner.label}.outputs.{connection.label}"
    return connection_name


def _get_function_dict(function):
    result = {
        "label": function.__name__,
    }
    function_has_metadata = hasattr(function, "_semantikon_metadata")
    if function_has_metadata:
        result.update(function._semantikon_metadata)
    return result


def _parse_output_args(node: Node) -> dict:
    output_tuple_or_dict_or_none = parse_output_args(node.node_function)
    if isinstance(output_tuple_or_dict_or_none, dict):
        output_tuple = (output_tuple_or_dict_or_none,)
    elif output_tuple_or_dict_or_none is None:
        output_tuple = ({} for _ in node.outputs.labels)
    else:
        output_tuple = output_tuple_or_dict_or_none
    outputs = {key: out for key, out in zip(node.outputs.labels, output_tuple)}
    return outputs


def get_inputs_and_outputs(node: Node) -> dict:
    """
    Read input and output arguments with their type hints and return a
    dictionary containing all input output information

    Args:
        node (pyiron_workflow.nodes.Node): node to be parsed

    Returns:
        (dict): dictionary containing input output args, type hints, values
            and variable names
    """
    if isinstance(node, Macro):
        raise NotImplementedError("Macros are not supported yet")
    inputs = parse_input_args(node.node_function)
    outputs = _parse_output_args(node)
    for key, value in node.inputs.items():
        if inputs[key] is None:
            inputs[key] = {}
        if value.value is not NOT_DATA:
            inputs[key]["value"] = value.value
        inputs[key]["connection"] = get_source_output(value)
    for key, value in node.outputs.to_value_dict().items():
        if value is not NOT_DATA:
            outputs[key]["value"] = value
    return {
        "inputs": inputs,
        "outputs": outputs,
        "function": _get_function_dict(node.node_function),
        "label": node.label,
    }


def workflow_to_dict(workflow: Workflow) -> dict:
    """
    Convert a workflow to a dictionary

    Args:
        workflow (pyiron_workflow.workflow.Workflow): workflow object

    Returns:
        (dict): dictionary containing workflow information
    """
    result = {}
    for node in workflow:
        data = get_inputs_and_outputs(node)
        result[node.label] = data
    result["workflow_label"] = workflow.label
    return result


def parse_workflow(
    workflow: Workflow,
    graph: Graph | None = None,
    inherit_properties: bool = True,
    ontology=PNS,
    append_missing_items: bool = True,
) -> Graph:
    """
    Generate RDF graph from a pyiron workflow object

    Args:
        workflow (pyiron_workflow.workflow.Workflow): workflow object
        graph (rdflib.Graph): graph to be updated
        inherit_properties (bool): if True, properties are inherited

    Returns:
        (rdflib.Graph): graph containing workflow information
    """
    wf_dict = workflow_to_dict(workflow)
    return get_knowledge_graph(
        wf_dict=wf_dict,
        graph=graph,
        inherit_properties=inherit_properties,
        ontology=ontology,
        append_missing_items=append_missing_items,
    )
