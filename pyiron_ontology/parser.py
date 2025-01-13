from semantikon.converter import parse_input_args, parse_output_args
from rdflib import Graph, Literal, RDF, RDFS


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
    outputs = {key: out for key, out in zip(node.outputs.labels, outputs)}
    for key, value in node.inputs.items():
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
            if d["uri"]:
                graph.add((label, RDF.type, d["uri"]))
            if d["value"]:
                graph.add((label, RDF.value, Literal(d["value"])))
            graph.add((label, EX[io_[:-1] + "Of"], EX[data["label"]]))
            if d["units"] is not None:
                graph.add((label, EX.hasUnits, EX[d["units"]]))
            if d.get("connection", None) is not None:
                graph.add((label, EX.comesFrom, EX[d["connection"]]))
            if d["triple"] is not None:
                if isinstance(d["triple"][0], tuple):
                    triple = d["triple"]
                else:
                    triple = (d["triple"],)
                for t in triple:
                    if t[1].startswith("inputs.") or t[1].startswith("outputs."):
                        t[1] = data["label"] + "." + t[1]
                    graph.add((label, t[0], EX[obj]))
    return graph
