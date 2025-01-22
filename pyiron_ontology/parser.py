from semantikon.converter import parse_input_args, parse_output_args
from rdflib import Graph, Literal, RDF, RDFS, URIRef, OWL, PROV, Namespace
from pyiron_workflow import NOT_DATA, Workflow, Macro
from pyiron_workflow.node import Node


class PNS:
    BASE = Namespace("http://pyiron.org/ontology/")
    hasNode = BASE["hasNode"]
    hasSourceFunction = BASE["hasSourceFunction"]
    hasUnits = BASE["hasUnits"]
    inheritsPropertiesFrom = BASE["inheritsPropertiesFrom"]
    inputOf = BASE["inputOf"]
    outputOf = BASE["outputOf"]

    def __getattr__(self, name):
        raise AttributeError(f"Namespace {self.BASE} has no attribute {name}")


def get_source_output(var: Node) -> str | None:
    if not var.connected:
        return None
    connection = var.connections[0]
    return f"{connection.owner.label}.outputs.{connection.label}"


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
    data: dict,
    workflow_namespace: str | None = None,
) -> Graph:
    """
    Generate triples from a dictionary containing input output information. The
    dictionary should be obtained from the get_inputs_and_outputs function, and
    should contain the keys "inputs", "outputs", "function" and "label". Within
    "inputs" and "outputs", the keys should be the variable names, and the values
    should be dictionaries containing the keys "type", "value", "var_name" and
    "connection". The "connection" key should contain the label of the output
    variable that the input is connected to. The "type" key should contain the
    URI of the type of the variable. The "value" key should contain the value of
    the variable. The "var_name" key should contain the variable name. The "function"
    key should contain the name of the function that the node is connected to. The
    "label" key should contain the label of the node. In terms of python code,
    it should look like this:

    >>> data = {
    >>>     "inputs": {
    >>>         "input1": {
    >>>             "type": URIRef("http://example.org/Type"),
    >>>             "value": 1,
    >>>             "triples": some_triples,
    >>>             "restrictions": some_restrictions,
    >>>             "var_name": "input1",
    >>>             "connection": "output1"
    >>>         }
    >>>     },
    >>>     "outputs": {
    >>>         "output1": {
    >>>             "type": URIRef("http://example.org/Type"),
    >>>             "value": 1,
    >>>             "triples": other_triples,
    >>>             "var_name": "output1"
    >>>         }
    >>>     },
    >>>     "function": "function_name",
    >>>     "label": "label"
    >>> }

    triples should consist of a list of tuples, where each tuple contains 2 or 3
    elements. If the tuple contains 2 elements, the first element should be the
    predicate and the second element should be the object, in order for the subject
    to be  generated from the variable name.

    Args:
        data (dict): dictionary containing input output information
        workflow_namespace (str): namespace of the workflow

    Returns:
        (rdflib.Graph): graph containing triples
    """
    if workflow_namespace is None:
        workflow_namespace = ""
    else:
        workflow_namespace += "."
    graph = Graph()
    full_label = workflow_namespace + data["label"]
    # Triple already exists
    label_def_triple = (URIRef(full_label), RDF.type, PROV.Activity)
    if len(list(graph.triples(label_def_triple))) > 0:
        return graph
    graph.add(label_def_triple)
    graph.add((URIRef(full_label), PNS.hasSourceFunction, URIRef(data["function"])))
    for io_ in ["inputs", "outputs"]:
        for key, d in data[io_].items():
            full_key = full_label + f".{io_}." + key
            label = URIRef(full_key)
            graph.add((label, RDFS.label, Literal(full_key)))
            graph.add((label, RDF.type, PROV.Entity))
            if d.get("uri", None) is not None:
                graph.add((label, RDF.type, URIRef(d["uri"])))
            if d.get("value", NOT_DATA) is not NOT_DATA:
                graph.add((label, RDF.value, Literal(d["value"])))
            if io_ == "inputs":
                graph.add((label, PNS.inputOf, URIRef(full_label)))
            elif io_ == "outputs":
                graph.add((label, PNS.outputOf, URIRef(full_label)))
            if d.get("units", None) is not None:
                graph.add((label, PNS.hasUnits, URIRef(d["units"])))
            if d.get("connection", None) is not None:
                graph.add(
                    (
                        label,
                        PNS.inheritsPropertiesFrom,
                        URIRef(workflow_namespace + d["connection"]),
                    )
                )
            for t in _get_triples_from_restrictions(d):
                graph.add(_parse_triple(t, ns=full_label, label=label))
    return graph


def _get_triples_from_restrictions(data: dict) -> list:
    triples = []
    if data.get("restrictions", None) is not None:
        triples = restriction_to_triple(data["restrictions"])
    if data.get("triples", None) is not None:
        if isinstance(data["triples"][0], tuple | list):
            triples.extend(list(data["triples"]))
        else:
            triples.extend([data["triples"]])
    return triples


def restriction_to_triple(restrictions: tuple) -> list:
    """
    Convert restrictions to triples

    Args:
        restrictions (tuple): tuple of restrictions

    Returns:
        (list): list of triples

    In the semantikon notation, restrictions are given in the format:

    >>> restrictions = (
    >>>     (OWL.onProperty, EX.HasSomething),
    >>>     (OWL.someValuesFrom, EX.Something)
    >>> )

    This tuple is internally converted to the triples:

    >>> (
    >>>     (EX.HasSomethingRestriction, RDF.type, OWL.Restriction),
    >>>     (EX.HasSomethingRestriction, OWL.onProperty, EX.HasSomething),
    >>>     (EX.HasSomethingRestriction, OWL.someValuesFrom, EX.Something),
    >>>     (my_object, RDFS.subClassOf, EX.HasSomethingRestriction)
    >>> )
    """
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


def _parse_triple(
    triples: tuple,
    ns: str,
    label: URIRef | None = None,
) -> tuple:
    if len(triples) == 2:
        subj, pred, obj = label, triples[0], triples[1]
    elif len(triples) == 3:
        subj, pred, obj = triples
    else:
        raise ValueError("Triple must have 2 or 3 elements")
    if obj.startswith("inputs.") or obj.startswith("outputs."):
        obj = ns + "." + obj
    if not isinstance(obj, URIRef):
        obj = URIRef(obj)
    return subj, pred, obj


def _inherit_properties(graph: Graph, n: int | None = None):
    update_query = (
        f"PREFIX ns: <{PNS.BASE}>",
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
        n = len(list(graph.triples((None, PNS.inheritsPropertiesFrom, None))))
    for _ in range(n):
        graph.update("\n".join(update_query))


def validate_values(graph: Graph) -> list:
    """
    Validate if all values required by restrictions are present in the graph

    Args:
        graph (rdflib.Graph): graph to be validated

    Returns:
        (list): list of missing triples
    """
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
    workflow: Workflow,
    graph: Graph | None = None,
    inherit_properties: bool = True,
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
    if graph is None:
        graph = Graph()
    workflow_label = URIRef(workflow.label)
    graph.add((workflow_label, RDFS.label, Literal(workflow.label)))
    for node in workflow:
        data = get_inputs_and_outputs(node)
        graph.add(
            (workflow_label, PNS.hasNode, URIRef(workflow.label + "." + data["label"]))
        )
        graph += get_triples(
            data=data,
            workflow_namespace=workflow.label,
        )
    if inherit_properties:
        _inherit_properties(graph)
    return graph
