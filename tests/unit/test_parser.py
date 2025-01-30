import unittest
from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph, OWL, RDF, RDFS, Literal, URIRef
from pyiron_ontology.parser import (
    get_inputs_and_outputs,
    get_triples,
    _inherit_properties,
    validate_values,
    parse_workflow,
    PNS,
)
from pyiron_workflow import Workflow
from semantikon.typing import u
from semantikon.converter import semantikon_class
from dataclasses import dataclass
from rdflib import Namespace


EX = Namespace("http://example.org/")


@Workflow.wrap.as_function_node("speed")
def calculate_speed(
    distance: u(float, units="meter") = 10.0,
    time: u(float, units="second") = 2.0,
) -> u(
    float,
    units="meter/second",
    triples=(
        (EX.somehowRelatedTo, "inputs.time"),
        (EX.subject, EX.predicate, EX.object),
        (EX.subject, EX.predicate, None),
        (None, EX.predicate, EX.object),
    ),
):
    return distance / time


@Workflow.wrap.as_function_node("result")
def add(a: float, b: float) -> u(float, triples=(EX.HasOperation, EX.Addition)):
    return a + b


@Workflow.wrap.as_function_node("result")
def multiply(a: float, b: float) -> u(
    float,
    triples=(
        (EX.HasOperation, EX.Multiplication),
        (PNS.inheritsPropertiesFrom, "inputs.a"),
    ),
):
    return a * b


@Workflow.wrap.as_function_node("result")
def correct_analysis(
    a: u(
        float,
        restrictions=(
            (OWL.onProperty, EX.HasOperation),
            (OWL.someValuesFrom, EX.Addition),
        ),
    )
) -> float:
    return a


@Workflow.wrap.as_function_node("result")
def wrong_analysis(
    a: u(
        float,
        restrictions=(
            (OWL.onProperty, EX.HasOperation),
            (OWL.someValuesFrom, EX.Division),
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

    def test_units_with_sparql(self):
        wf = Workflow("speed")
        wf.speed = calculate_speed()
        wf.run()
        graph = parse_workflow(wf)
        query_txt = [
            "PREFIX ex: <http://example.org/>",
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            f"PREFIX pns: <{PNS.BASE}>",
            "SELECT DISTINCT ?speed ?units",
            "WHERE {",
            "    ?output pns:hasValue ?output_tag .",
            "    ?output_tag rdf:value ?speed .",
            "    ?output_tag pns:hasUnits ?units .",
            "}",
        ]
        query = "\n".join(query_txt)
        results = graph.query(query)
        self.assertEqual(len(results), 3)
        result_list = [row[0].value for row in graph.query(query)]
        self.assertEqual(sorted(result_list), [2.0, 5.0, 10.0])

    def test_triples(self):
        speed = calculate_speed()
        data = get_inputs_and_outputs(speed)
        graph = get_triples(data=data)
        subj = URIRef("http://example.org/subject")
        obj = URIRef("http://example.org/object")
        label = URIRef("calculate_speed.outputs.speed")
        self.assertGreater(
            len(list(graph.triples((None, PNS.hasUnits, URIRef("meter/second"))))), 0
        )
        ex_triple = (None, EX.somehowRelatedTo, URIRef("calculate_speed.inputs.time"))
        self.assertEqual(
            len(list(graph.triples(ex_triple))),
            1,
            msg=f"Triple {ex_triple} not found {graph.serialize(format='turtle')}",
        )
        self.assertEqual(
            len(list(graph.triples((EX.subject, EX.predicate, EX.object)))), 1
        )
        self.assertEqual(len(list(graph.triples((subj, EX.predicate, label)))), 1)
        self.assertEqual(len(list(graph.triples((label, EX.predicate, obj)))), 1)

    def test_correct_analysis(self):
        def get_graph(wf):
            graph = Graph()
            graph.add((EX.HasOperation, RDF.type, RDF.Property))
            graph.add((EX.Addition, RDF.type, OWL.Class))
            graph.add((EX.Multiplication, RDF.type, OWL.Class))
            for value in wf.children.values():
                data = get_inputs_and_outputs(value)
                graph += get_triples(data=data)
            _inherit_properties(graph)
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

    def test_parse_workflow(self):
        wf = Workflow("correct_analysis")
        wf.addition = add(a=1.0, b=2.0)
        graph = parse_workflow(wf)
        self.assertEqual(
            len(
                list(
                    graph.triples(
                        (
                            URIRef("correct_analysis.addition.inputs.a"),
                            RDFS.label,
                            Literal("correct_analysis.addition.inputs.a"),
                        )
                    )
                )
            ),
            1,
        )

    def test_macro(self):
        @Workflow.wrap.as_macro_node
        def operation(macro=None):
            macro.add = add(a=1.0, b=2.0)
            macro.multiply = multiply(a=macro.add, b=3.0)
            return macro.multiply

        wf = Workflow("macro")
        wf.macro = operation()
        self.assertRaises(NotImplementedError, get_inputs_and_outputs, wf.macro)

    def test_namespace(self):
        self.assertEqual(PNS.hasUnits, URIRef("http://pyiron.org/ontology/hasUnits"))
        with self.assertRaises(AttributeError):
            _ = PNS.ahoy

    def test_parsing_without_running(self):
        wf = Workflow("correct_analysis")
        wf.addition = add(a=1.0, b=2.0)
        data = get_inputs_and_outputs(wf.addition)
        self.assertFalse("value" in data["outputs"])
        graph = get_triples(data)
        self.assertEqual(
            len(list(graph.triples((None, RDF.value, None)))),
            2,
            msg="There should be only values for a and b, but not for the output",
        )
        wf.run()
        data = get_inputs_and_outputs(wf.addition)
        graph = get_triples(data)
        self.assertEqual(
            len(list(graph.triples((None, RDF.value, None)))),
            3,
            msg="There should be values for a, b and the output",
        )


@semantikon_class
@dataclass
class Input:
    T: u(float, units="kelvin")
    n: int
    # This line should be removed with the next version of semantikon
    _is_semantikon_class = True
    class parameters:
        _is_semantikon_class = True
        a: int = 2


@semantikon_class
@dataclass
class Output:
    E: u(float, units="electron_volt")
    L: u(float, units="angstrom")
    # This line should be removed with the next version of semantikon
    _is_semantikon_class = True


@Workflow.wrap.as_function_node
def run_md(inp: Input) -> Output:
    out = Output(E=1.0, L=2.0)
    return out


class TestDataclass(unittest.TestCase):
    def test_dataclass(self):
        wf = Workflow("my_wf")
        inp = Input(T=300.0, n=100)
        inp.parameters.a = 1
        wf.node = run_md(inp)
        wf.run()
        graph = parse_workflow(wf)
        i_txt = "my_wf.node.inputs.inp"
        o_txt = "my_wf.node.outputs.out"
        triples = (
            (URIRef(f"{i_txt}.n.value"), RDFS.subClassOf, URIRef(f"{i_txt}.value")),
            (URIRef(f"{i_txt}.n.value"), RDF.value, Literal(100)),
            (URIRef(f"{i_txt}.parameters.a.value"), RDF.value, Literal(1)),
            (URIRef(o_txt), PNS.hasValue, URIRef(f"{o_txt}.E.value")),
        )
        s = graph.serialize(format="turtle")
        for triple in triples:
            self.assertEqual(
                len(list(graph.triples(triple))), 1, msg=f"{triple} not found in {s}"
            )


if __name__ == "__main__":
    unittest.main()
