import unittest
from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph, OWL, RDF
from pyiron_ontology.parser import get_inputs_and_outputs, get_triples, inherit_properties, validate_values
from pyiron_workflow import Workflow
from semantikon.typing import u
from rdflib import Namespace


EX = Namespace("http://example.org/")


@Workflow.wrap.as_function_node("speed")
def calculate_speed(
    distance: u(float, units="meter") = 10.0,
    time: u(float, units="second") = 2.0,
) -> u(
    float,
    units="meter/second",
    triple=(
        (EX.isOutputOf, "inputs.time"),
        (EX.subject, EX.predicate, EX.object)
    )
):
    return distance / time


@Workflow.wrap.as_function_node("result")
def add(a: float, b: float) -> u(float, triple=(EX.HasOperation, EX.Addition)):
    return a + b


@Workflow.wrap.as_function_node("result")
def multiply(a: float, b: float) -> u(
    float,
    triple=(
        (EX.HasOperation, EX.Multiplication),
        (EX.inheritsPropertiesFrom, "inputs.a")
    )
):
    return a * b


@Workflow.wrap.as_function_node("result")
def correct_analysis(
    a: u(
        float,
        restriction=(
            (OWL.onProperty, EX.HasOperation),
            (OWL.someValuesFrom, EX.Addition)
        )
    )
) -> float:
    return a


@Workflow.wrap.as_function_node("result")
def wrong_analysis(
    a: u(
        float,
        restriction=(
            (OWL.onProperty, EX.HasOperation),
            (OWL.someValuesFrom, EX.Division)
        )
    )
) -> float:
    return a


class TestParser(unittest.TestCase):
    def test_parser(self):
        c = calculate_speed()
        output_dict = get_inputs_and_outputs(c)
        for label in ["inputs", "outputs", "function", "label"]:
            self.assertIn(label, output_dict)

    def test_triples(self):
        speed = calculate_speed()
        data = get_inputs_and_outputs(speed)
        graph = get_triples(data, EX)
        self.assertGreater(
            len(list(graph.triples((None, EX.hasUnits, EX["meter/second"])))), 0
        )
        self.assertEqual(
            len(
                list(
                    graph.triples(
                        (None, EX.isOutputOf, EX["calculate_speed.inputs.time"])
                    )
                )
            ),
            1
        )
        self.assertEqual(
            len(list(graph.triples((EX.subject, EX.predicate, EX.object)))),
            1
        )

    def test_correct_analysis(self):
        def get_graph(wf):
            graph = Graph()
            graph.add((EX.HasOperation, RDF.type, RDF.Property))
            graph.add((EX.Addition, RDF.type, OWL.Class))
            graph.add((EX.Multiplication, RDF.type, OWL.Class))
            for value in wf.children.values():
                data = get_inputs_and_outputs(value)
                graph += get_triples(data, EX)
            inherit_properties(graph, EX)
            DeductiveClosure(OWLRL_Semantics).expand(graph)
            return graph
        wf = Workflow("correct_analysis")
        wf.addition = add(a=1., b=2.)
        wf.multiply = multiply(a=wf.addition, b=3.)
        wf.analysis = correct_analysis(a=wf.multiply)
        graph = get_graph(wf)
        self.assertEqual(len(validate_values(graph)), 0)
        wf = Workflow("wrong_analysis")
        wf.addition = add(a=1., b=2.)
        wf.multiply = multiply(a=wf.addition, b=3.)
        wf.analysis = wrong_analysis(a=wf.multiply)
        graph = get_graph(wf)
        self.assertEqual(len(validate_values(graph)), 1)


if __name__ == "__main__":
    unittest.main()
