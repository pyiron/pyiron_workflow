from semantikon.converter import parse_input_args, parse_output_args
from rdflib import Graph, Literal, RDF, RDFS, URIRef
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


def get_triples(data, EX):
    graph = Graph()
    label_def_triple = (EX[data["label"]], EX.hasSourceFunction, EX[data["function"]])
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
                graph.add((label, EX.hasUnits, EX[d["units"]]))
            if d.get("connection", None) is not None:
                graph.add((label, EX.comesFrom, EX[d["connection"]]))
            if d.get("triple", None) is not None:
                if isinstance(d["triple"][0], tuple | list):
                    triple = list(d["triple"])
                else:
                    triple = [d["triple"]]
                for t in triple:
                    if len(t) == 2:
                        subj = label
                        pred = t[0]
                        obj = t[1]
                    elif len(t) == 3:
                        subj = t[0]
                        pred = t[1]
                        obj = t[2]
                    else:
                        raise ValueError("Triple must have 2 or 3 elements")
                    if obj.startswith("inputs.") or obj.startswith("outputs."):
                        obj = data["label"] + "." + obj
                    if not isinstance(obj, URIRef):
                        obj = EX[obj]
                    graph.add((subj, pred, obj))
    return graph
