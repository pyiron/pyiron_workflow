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
        outputs = ({} for _ in node.outputs.labels)
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
    NS,
    workflow_namespace=None,
    hasSourceFunction=None,
    hasUnits=None,
    inheritsPropertiesFrom=None,
):
    if workflow_namespace is None:
        workflow_namespace = ""
    else:
        workflow_namespace += "."
    if hasSourceFunction is None:
        hasSourceFunction = NS.hasSourceFunction
    if hasUnits is None:
        hasUnits = NS.hasUnits
    if inheritsPropertiesFrom is None:
        inheritsPropertiesFrom = NS.inheritsPropertiesFrom
    graph = Graph()
    full_label = workflow_namespace + data["label"]
    # Triple already exists
    label_def_triple = (NS[full_label], hasSourceFunction, NS[data["function"]])
    if len(list(graph.triples(label_def_triple))) > 0:
        return graph
    graph.add(label_def_triple)
    for io_ in ["inputs", "outputs"]:
        for key, d in data[io_].items():
            full_key = full_label + f".{io_}." + key
            label = NS[full_key]
            graph.add((label, RDFS.label, Literal(full_key)))
            if d.get("uri", None) is not None:
                graph.add((label, RDF.type, d["uri"]))
            if d.get("value", NOT_DATA) is not NOT_DATA:
                graph.add((label, RDF.value, Literal(d["value"])))
            graph.add((label, NS[io_[:-1] + "Of"], NS[full_label]))
            if d.get("units", None) is not None:
                graph.add((label, hasUnits, NS[d["units"]]))
            if d.get("connection", None) is not None:
                graph.add(
                    (
                        label,
                        inheritsPropertiesFrom,
                        NS[workflow_namespace + d["connection"]],
                    )
                )
            for t in _get_triples_from_restrictions(d, NS):
                graph.add(_parse_triple(t, NS, ns=full_label, label=label))
    return graph


def _get_triples_from_restrictions(data, NS):
    triples = []
    if data.get("restrictions", None) is not None:
        triples = restriction_to_triple(data["restrictions"])
    if data.get("triples", None) is not None:
        if isinstance(data["triples"][0], tuple | list):
            triples.extend(list(data["triples"]))
        else:
            triples.extend([data["triples"]])
    return triples


def restriction_to_triple(restrictions):
    triples = []
    assert isinstance(restrictions, tuple) and isinstance(restrictions[0], tuple)
    if not isinstance(restrictions[0][0], tuple):
        restrictions = (restrictions,)
    for r in restrictions:
        assert len(r[0]) == 2
        label = r[0][1] + "Restriction"
        triples.append((label, RDF.type, OWL.Restriction))
        for rr in r:
            triples.append((label, rr[0], rr[1]))
        triples.append((RDF.type, label))
    return triples


def _parse_triple(triples, NS, ns, label=None):
    if len(triples) == 2:
        subj, pred, obj = label, triples[0], triples[1]
    elif len(triples) == 3:
        subj, pred, obj = triples
    else:
        raise ValueError("Triple must have 2 or 3 elements")
    if obj.startswith("inputs.") or obj.startswith("outputs."):
        obj = ns + "." + obj
    if not isinstance(obj, URIRef):
        obj = NS[obj]
    return subj, pred, obj


def _inherit_properties(graph, NS, n=None):
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
        "    FILTER(?p != rdf:type)",
        "}",
    )
    if n is None:
        n = len(list(graph.triples((None, NS.inheritsPropertiesFrom, None))))
    for _ in range(n):
        graph.update("\n".join(update_query))


def validate_values(graph):
    missing_triples = []
    for restrictions in graph.subjects(RDF.type, OWL.Restriction):
        on_property = graph.value(restrictions, OWL.onProperty)
        some_values_from = graph.value(restrictions, OWL.someValuesFrom)
        if on_property and some_values_from:
            for cls in graph.subjects(OWL.equivalentClass, restrictions):
                for instance in graph.subjects(RDF.type, cls):
                    if not (instance, on_property, some_values_from) in graph:
                        missing_triples.append(
                            (instance, on_property, some_values_from)
                        )
    return missing_triples


def parse_workflow(
    workflow,
    NS,
    graph=None,
    inherit_properties=True,
    hasNode=None,
    hasSourceFunction=None,
    hasUnits=None,
    inheritsPropertiesFrom=None,
):
    if hasNode is None:
        hasNode = NS.hasNode
    if hasSourceFunction is None:
        hasSourceFunction = NS.hasSourceFunction
    if hasUnits is None:
        hasUnits = NS.hasUnits
    if inheritsPropertiesFrom is None:
        inheritsPropertiesFrom = NS.inheritsPropertiesFrom
    if graph is None:
        graph = Graph()
    workflow_label = NS[workflow.label]
    graph.add((workflow_label, RDFS.label, Literal(workflow.label)))
    for value in workflow.children.values():
        data = get_inputs_and_outputs(value)
        graph.add((workflow_label, hasNode, NS[workflow.label + "." + data["label"]]))
        graph += get_triples(
            data=data,
            NS=NS,
            workflow_namespace=workflow.label,
            hasSourceFunction=hasSourceFunction,
            hasUnits=hasUnits,
            inheritsPropertiesFrom=inheritsPropertiesFrom,
        )
    if inherit_properties:
        _inherit_properties(graph, NS)
    return graph
