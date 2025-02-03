from typing import TypeAlias, Any
import warnings

from semantikon.converter import parse_input_args, parse_output_args, meta_to_dict
from rdflib import Graph, Literal, RDF, RDFS, URIRef, OWL, PROV, Namespace
from pyiron_workflow import NOT_DATA, Workflow, Macro
from pyiron_workflow.node import Node
from dataclasses import is_dataclass


class PNS:
    BASE = Namespace("http://pyiron.org/ontology/")
    hasNode = BASE["hasNode"]
    hasSourceFunction = BASE["hasSourceFunction"]
    hasUnits = BASE["hasUnits"]
    inheritsPropertiesFrom = BASE["inheritsPropertiesFrom"]
    inputOf = BASE["inputOf"]
    outputOf = BASE["outputOf"]
    hasValue = BASE["hasValue"]


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
    output_tuple_or_dict_or_none = parse_output_args(node)
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
    outputs = _parse_output_args(node.node_function)
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


def _translate_has_value(
    graph: Graph,
    label: URIRef,
    tag: str,
    value: Any = None,
    dtype: type | None = None,
    units: URIRef | None = None,
    parent: URIRef | None = None,
) -> Graph:
    tag_uri = URIRef(tag + ".value")
    graph.add((label, PNS.hasValue, tag_uri))
    if is_dataclass(dtype):
        warnings.warn(
            "semantikon_class is experimental - triples may change in the future",
            FutureWarning,
        )
        for k, v in dtype.__dict__.items():
            if isinstance(v, type) and is_dataclass(v):
                _translate_has_value(
                    graph=graph,
                    label=label,
                    tag=tag + "." + k,
                    value=getattr(value, k, None),
                    dtype=v,
                    parent=tag_uri,
                )
        for k, v in dtype.__annotations__.items():
            metadata = meta_to_dict(v)
            _translate_has_value(
                graph=graph,
                label=label,
                tag=tag + "." + k,
                value=getattr(value, k, None),
                dtype=metadata["dtype"],
                units=metadata.get("units", None),
                parent=tag_uri,
            )
    else:
        if parent is not None:
            graph.add((tag_uri, RDFS.subClassOf, parent))
        if value is not None:
            graph.add((tag_uri, RDF.value, Literal(value)))
        if units is not None:
            graph.add((tag_uri, PNS.hasUnits, URIRef(units)))
    return graph


def get_triples(
    data: dict,
    workflow_namespace: str | None = None,
) -> Graph:
    """
    Generate triples from a dictionary containing input output information.
    The dictionary should be obtained from the get_inputs_and_outputs function,
    and should contain the keys "inputs", "outputs", "function" and "label".
    Within "inputs" and "outputs", the keys should be the variable names, and
    the values should be dictionaries containing the keys "type", "value" and
    "connection". The "connection" key should contain the label of the output
    variable that the input is connected to. The "type" key should contain the
    URI of the type of the variable. The "value" key should contain the value
    of the variable. The "function" key should contain the name of the function
    that the node is connected to. The "label" key should contain the label of
    the node. In terms of python code, it should look like this:

    >>> data = {
    >>>     "inputs": {
    >>>         "input1": {
    >>>             "type": URIRef("http://example.org/Type"),
    >>>             "value": 1,
    >>>             "triples": some_triples,
    >>>             "restrictions": some_restrictions,
    >>>             "connection": "output1"
    >>>         }
    >>>     },
    >>>     "outputs": {
    >>>         "output1": {
    >>>             "type": URIRef("http://example.org/Type"),
    >>>             "value": 1,
    >>>             "triples": other_triples,
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
    node_label = workflow_namespace + data["label"]
    graph.add((URIRef(node_label), RDF.type, PROV.Activity))
    graph.add(
        (URIRef(node_label), PNS.hasSourceFunction, URIRef(data["function"]["label"]))
    )
    if data["function"].get("uri", None) is not None:
        graph.add((URIRef(node_label), RDF.type, URIRef(data["function"]["uri"])))
    for t in _get_triples_from_restrictions(data["function"]):
        graph.add(_parse_triple(t, ns=node_label, label=URIRef(node_label)))
    for io_ in ["inputs", "outputs"]:
        for key, d in data[io_].items():
            channel_label = URIRef(node_label + f".{io_}." + key)
            graph.add((channel_label, RDFS.label, Literal(str(channel_label))))
            graph.add((channel_label, RDF.type, PROV.Entity))
            if d.get("uri", None) is not None:
                graph.add((channel_label, RDF.type, URIRef(d["uri"])))
            if io_ == "inputs":
                graph.add((channel_label, PNS.inputOf, URIRef(node_label)))
            elif io_ == "outputs":
                graph.add((channel_label, PNS.outputOf, URIRef(node_label)))
            tag = channel_label
            if io_ == "inputs" and d.get("connection", None) is not None:
                tag = workflow_namespace + d["connection"]
                graph.add((channel_label, PNS.inheritsPropertiesFrom, URIRef(tag)))
            graph = _translate_has_value(
                graph=graph,
                label=channel_label,
                tag=tag,
                value=d.get("value", None),
                dtype=d.get("dtype", None),
                units=d.get("units", None),
            )
            for t in _get_triples_from_restrictions(d):
                graph.add(_parse_triple(t, ns=node_label, label=channel_label))
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


_rest_type: TypeAlias = tuple[tuple[URIRef, URIRef], ...]


def _validate_restriction_format(
    restrictions: _rest_type | tuple[_rest_type] | list[_rest_type],
) -> tuple[_rest_type]:
    if not all(isinstance(r, tuple) for r in restrictions):
        raise ValueError("Restrictions must be tuples of URIRefs")
    elif all(isinstance(rr, URIRef) for r in restrictions for rr in r):
        return (restrictions,)
    elif all(isinstance(rrr, URIRef) for r in restrictions for rr in r for rrr in rr):
        return restrictions
    else:
        raise ValueError("Restrictions must be tuples of URIRefs")


def restriction_to_triple(
    restrictions: _rest_type | tuple[_rest_type] | list[_rest_type],
) -> list[tuple[URIRef | None, URIRef, URIRef]]:
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
    restrictions_collection = _validate_restriction_format(restrictions)
    triples: list[tuple[URIRef | None, URIRef, URIRef]] = []
    for r in restrictions_collection:
        label = r[0][1] + "Restriction"
        triples.append((label, RDF.type, OWL.Restriction))
        for rr in r:
            triples.append((label, rr[0], rr[1]))
        triples.append((None, RDF.type, label))
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
    if subj is None:
        subj = label
    if obj is None:
        obj = label
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
        "    FILTER(?p != ns:hasValue)",
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
