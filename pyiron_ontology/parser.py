from semantikon.converter import parse_input_args, parse_output_args
from rdflib import Graph, Literal, RDF, RDFS, URIRef, OWL
from pyiron_workflow import NOT_DATA


def get_source_output(var):
    if not var.connected:
        return None
    connection = var.connections[0]
    return f"{connection.owner.label}.outputs.{connection.label}"


def get_inputs_and_outputs(node):
    """
    Read input and output arguments with their type hints and return a
    dictionary containing all input output information

    Args:
        node (pyiron_workflow.nodes.Node): node to be parsed

    Returns:
        (dict): dictionary containing input output args, type hints, values
            and variable names
    """
    inputs = parse_input_args(node.node_function)
    outputs = parse_output_args(node.node_function)
    if isinstance(outputs, dict):
        outputs = (outputs,)
    elif outputs is None:
        outputs = len(node.outputs.labels) * ({},)
    outputs = {key: out for key, out in zip(node.outputs.labels, outputs)}
    for key, value in node.inputs.items():
        if inputs[key] is None:
            inputs[key] = {}
        inputs[key]["value"] = value.value
        inputs[key]["var_name"] = key
        inputs[key]["connection"] = get_source_output(value)
    for key, value in node.outputs.to_value_dict().items():
        outputs[key]["value"] = value
        outputs[key]["var_name"] = key
    return {
        "inputs": inputs,
        "outputs": outputs,
        "function": node.node_function.__name__,
        "label": node.label,
    }


def get_triples(
    data,
    EX,
    hasSourceFunction=None,
    hasUnits=None,
    inheritsPropertiesFrom=None,
    update_query=True
):
    if hasSourceFunction is None:
        hasSourceFunction = EX.hasSourceFunction
    if hasUnits is None:
        hasUnits = EX.hasUnits
    if inheritsPropertiesFrom is None:
        inheritsPropertiesFrom = EX.inheritsPropertiesFrom
    graph = Graph()
    label_def_triple = (
        EX[data["label"]], hasSourceFunction, EX[data["function"]]
    )
    if len(list(graph.triples(label_def_triple))) > 0:
        return graph
    graph.add(label_def_triple)
    for io_ in ["inputs", "outputs"]:
        for key, d in data[io_].items():
            full_key = data["label"] + f".{io_}." + key
            label = EX[full_key]
            graph.add((label, RDFS.label, Literal(full_key)))
            if d.get("uri", None) is not None:
                graph.add((label, RDF.type, d["uri"]))
            if d.get("value", NOT_DATA) is not NOT_DATA:
                graph.add((label, RDF.value, Literal(d["value"])))
            graph.add((label, EX[io_[:-1] + "Of"], EX[data["label"]]))
            if d.get("units", None) is not None:
                graph.add((label, hasUnits, EX[d["units"]]))
            if d.get("connection", None) is not None:
                graph.add((label, inheritsPropertiesFrom, EX[d["connection"]]))
            if d.get("triple", None) is not None:
                if isinstance(d["triple"][0], tuple | list):
                    triple = list(d["triple"])
                else:
                    triple = [d["triple"]]
                for t in triple:
                    graph.add(_parse_triple(t, EX, label=label, data=data))
    if update_query:
        _inherit_properties(graph, EX)
    return graph


def _parse_triple(triple, EX, label=None, data=None):
    if len(triple) == 2:
        subj, pred, obj = label, triple[0], triple[1]
    elif len(triple) == 3:
        subj, pred, obj = triple
    else:
        raise ValueError("Triple must have 2 or 3 elements")
    if obj.startswith("inputs.") or obj.startswith("outputs."):
        obj = data["label"] + "." + obj
    if not isinstance(obj, URIRef):
        obj = EX[obj]
    return subj, pred, obj


def _inherit_properties(graph, NS):
    update_query = (
        f"PREFIX ns: <{NS}>",
        f"PREFIX rdfs: <{RDFS}>",
        f"PREFIX rdf: <{RDF}>",
        "",
        "INSERT {",
        "    ?subject ?p ?o .",
        "}",
        "WHERE {",
        "    ?subject ns:inheritsPropertiesFrom ?target .",
        "    ?target ?p ?o .",
        "    FILTER(?p != ns:inheritsPropertiesFrom)",
        "    FILTER(?p != rdfs:label)",
        "    FILTER(?p != rdf:value)",
        "}"
    )
    graph.update("\n".join(update_query))
