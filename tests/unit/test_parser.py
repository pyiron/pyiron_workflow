import unittest
from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph, OWL, RDF
from pyiron_ontology.parser import (
    get_inputs_and_outputs,
    get_triples,
    _inherit_properties,
    validate_values,
)
from pyiron_workflow import Workflow
from semantikon.typing import u
from rdflib import Namespace


NS = Namespace("http://example.org/")


@Workflow.wrap.as_function_node("speed")
def calculate_speed(
    distance: u(float, units="meter") = 10.0,
    time: u(float, units="second") = 2.0,
) -> u(
    float,
    units="meter/second",
    triples=((NS.isOutputOf, "inputs.time"), (NS.subject, NS.predicate, NS.object)),
):
    return distance / time


@Workflow.wrap.as_function_node("result")
def add(a: float, b: float) -> u(float, triples=(NS.HasOperation, NS.Addition)):
    return a + b


@Workflow.wrap.as_function_node("result")
def multiply(a: float, b: float) -> u(
    float,
    triples=(
        (NS.HasOperation, NS.Multiplication),
        (NS.inheritsPropertiesFrom, "inputs.a"),
    ),
):
    return a * b


@Workflow.wrap.as_function_node("result")
def correct_analysis(
    a: u(
        float,
        restrictions=(
            (OWL.onProperty, NS.HasOperation),
            (OWL.someValuesFrom, NS.Addition),
        ),
    )
) -> float:
    return a


@Workflow.wrap.as_function_node("result")
def wrong_analysis(
    a: u(
        float,
        restrictions=(
            (OWL.onProperty, NS.HasOperation),
            (OWL.someValuesFrom, NS.Division),
        ),
    )
) -> float:
    return a


@Workflow.wrap.as_function_node
def multiple_outputs(a: int = 1, b: int = 2) -> tuple[int, int]:
    return a, b


class TestParser(unittest.TestCase):
    def test_parser(self):
        c = calculate_speed()
        output_dict = get_inputs_and_outputs(c)
        for label in ["inputs", "outputs", "function", "label"]:
            self.assertIn(label, output_dict)

    def test_triples(self):
        speed = calculate_speed()
        data = get_inputs_and_outputs(speed)
        graph = get_triples(data=data, NS=NS)
        self.assertGreater(
            len(list(graph.triples((None, NS.hasUnits, NS["meter/second"])))), 0
        )
        ex_triple = (None, NS.isOutputOf, NS["calculate_speed.inputs.time"])
        self.assertEqual(
            len(list(graph.triples(ex_triple))),
            1,
            msg=f"Triple {ex_triple} not found {graph.serialize(format='turtle')}",
        )
        self.assertEqual(
            len(list(graph.triples((NS.subject, NS.predicate, NS.object)))), 1
        )

    def test_correct_analysis(self):
        def get_graph(wf):
            graph = Graph()
            graph.add((NS.HasOperation, RDF.type, RDF.Property))
            graph.add((NS.Addition, RDF.type, OWL.Class))
            graph.add((NS.Multiplication, RDF.type, OWL.Class))
            for value in wf.children.values():
                data = get_inputs_and_outputs(value)
                graph += get_triples(data=data, NS=NS)
            _inherit_properties(graph, NS)
            DeductiveClosure(OWLRL_Semantics).expand(graph)
            return graph

        wf = Workflow("correct_analysis")
        wf.addition = add(a=1.0, b=2.0)
        wf.multiply = multiply(a=wf.addition, b=3.0)
        wf.analysis = correct_analysis(a=wf.multiply)
        graph = get_graph(wf)
        self.assertEqual(len(validate_values(graph)), 0)
        wf = Workflow("wrong_analysis")
        wf.addition = add(a=1.0, b=2.0)
        wf.multiply = multiply(a=wf.addition, b=3.0)
        wf.analysis = wrong_analysis(a=wf.multiply)
        graph = get_graph(wf)
        self.assertEqual(len(validate_values(graph)), 1)

    def test_multiple_outputs(self):
        node = multiple_outputs()
        node.run()
        data = get_inputs_and_outputs(node)
        self.assertEqual(data["outputs"]["a"]["value"], 1)
        self.assertEqual(data["outputs"]["b"]["value"], 2)


if __name__ == "__main__":
    unittest.main()
