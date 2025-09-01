import dataclasses
import unittest

import rdflib
from semantikon import ontology as onto
from semantikon.metadata import u

import pyiron_workflow as pwf
from pyiron_workflow.channels import ChannelConnectionError
from pyiron_workflow.knowledge import export_to_dict, parse_workflow
from pyiron_workflow.nodes.composite import FailedChildError

EX = rdflib.Namespace("http://example.org/")
QUDT = rdflib.Namespace("http://qudt.org/vocab/unit/")


@pwf.as_function_node("speed")
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


@pwf.as_function_node("result")
@u(uri=EX.Addition)
def add(a: float, b: float) -> u(float, triples=(EX.HasOperation, EX.Addition)):
    return a + b


@pwf.as_function_node("result")
def multiply(a: float, b: float) -> u(
    float,
    triples=(
        (EX.HasOperation, EX.Multiplication),
        (onto.PROV.wasDerivedFrom, "inputs.a"),
    ),
):
    return a * b


@pwf.as_macro_node("result")
def operation(macro=None, a: float = 1.0, b: float = 1.0) -> float:
    macro.addition = add(a=a, b=b)
    macro.multiply = multiply(a=macro.addition, b=b)
    return macro.multiply


@pwf.as_function_node("result")
def correct_analysis(
    a: u(
        float,
        restrictions=(
            (rdflib.OWL.onProperty, EX.HasOperation),
            (rdflib.OWL.someValuesFrom, EX.Addition),
        ),
    ),
) -> float:
    return a


@pwf.as_function_node("result")
def wrong_analysis(
    a: u(
        float,
        restrictions=(
            (rdflib.OWL.onProperty, EX.HasOperation),
            (rdflib.OWL.someValuesFrom, EX.Division),
        ),
    ),
) -> float:
    return a


@pwf.as_function_node
def multiple_outputs(a: int = 1, b: int = 2) -> tuple[int, int]:
    return a, b


@pwf.as_function_node("z")
def AddOnetology(x: u(int, uri=EX.Data)) -> u(int, uri=EX.Data):
    y = x + 1
    return y


@pwf.as_macro_node("zout")
def AddTwoMacrontology(self, inp: u(int, uri=EX.Data)) -> u(int, uri=EX.Data):
    self.a1 = AddOnetology(inp)
    self.a2 = AddOnetology(self.a1)
    return self.a2


class TestParser(unittest.TestCase):
    def test_parser(self):
        wf = pwf.Workflow("speed")
        wf.c = calculate_speed()
        output_dict = export_to_dict(wf)
        for label in ["inputs", "outputs", "nodes", "edges", "label"]:
            self.assertIn(label, output_dict)

    def test_export_to_dict_failures(self):
        with self.assertRaises(TypeError):
            export_to_dict(
                "not a node of known type",
                msg="Fail cleanly on unsupported node types",
            )

    def test_units_with_sparql(self):
        wf = pwf.Workflow("speed")
        wf.speed = calculate_speed()
        wf.run()
        graph = parse_workflow(wf)
        query_txt = [
            "PREFIX ex: <http://example.org/>",
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
            f"PREFIX pns: <{onto.SNS.BASE}>",
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
        wf = pwf.Workflow("speed")
        wf.speed = calculate_speed()
        graph = parse_workflow(wf)
        subj = rdflib.URIRef("http://example.org/subject")
        obj = rdflib.URIRef("http://example.org/object")
        label = rdflib.URIRef("speed.speed.outputs.speed")
        self.assertIn(
            (None, onto.SNS.hasUnits, QUDT["M-PER-SEC"]),
            graph,
            msg=graph.serialize(format="turtle"),
        )
        ex_triple = (
            None,
            EX.somehowRelatedTo,
            rdflib.URIRef("speed.speed.inputs.time"),
        )
        self.assertIn(
            ex_triple,
            graph,
            msg=f"Triple {ex_triple} not found {graph.serialize(format='turtle')}",
        )
        self.assertIn((subj, EX.predicate, obj), graph)
        self.assertIn((subj, EX.predicate, label), graph)
        self.assertIn((label, EX.predicate, obj), graph)

    def test_correct_analysis(self):
        wf = pwf.Workflow("correct_analysis")
        wf.addition = add(a=1.0, b=2.0)
        wf.multiply = multiply(a=wf.addition, b=3.0)
        wf.analysis = correct_analysis(a=wf.multiply)
        graph = onto.get_knowledge_graph(export_to_dict(wf))
        self.assertEqual(onto.validate_values(graph)["missing_triples"], [])
        wf = pwf.Workflow("wrong_analysis")
        wf.addition = add(a=1.0, b=2.0)
        wf.multiply = multiply(a=wf.addition, b=3.0)
        with self.assertRaises(ChannelConnectionError):
            wf.analysis = wrong_analysis(a=wf.multiply)

    def test_multiple_outputs(self):
        wf = pwf.Workflow("multiple_outputs")
        wf.node = multiple_outputs()
        wf.node.run()
        data = export_to_dict(wf)
        self.assertEqual(data["outputs"]["node__a"]["value"], 1)
        self.assertEqual(data["outputs"]["node__b"]["value"], 2)

    def test_parse_workflow(self):
        wf = pwf.Workflow("correct_analysis")
        wf.addition = add(a=1.0, b=2.0)
        data = export_to_dict(wf)
        graph = onto.get_knowledge_graph(data)
        self.assertTrue(
            EX.Addition
            in list(
                graph.objects(
                    rdflib.URIRef("correct_analysis.addition"), rdflib.RDF.type
                )
            )
        )

    def test_namespace(self):
        self.assertEqual(
            onto.SNS.hasUnits, rdflib.URIRef("http://pyiron.org/ontology/hasUnits")
        )
        with self.assertRaises(AttributeError):
            _ = onto.SNS.ahoy

    def test_parsing_without_running(self):
        wf = pwf.Workflow("test")
        wf.addition = add(a=1.0, b=2.0)
        data = export_to_dict(wf)
        self.assertFalse("value" in data["outputs"]["addition__result"])
        graph = onto.get_knowledge_graph(data)
        self.assertEqual(
            len(list(graph.triples((None, rdflib.RDF.value, None)))),
            4,
            msg="There should be only values for a and b, but not for the output",
        )
        wf.run()
        data = export_to_dict(wf)
        graph = onto.get_knowledge_graph(data)
        self.assertEqual(
            len(list(graph.triples((None, rdflib.RDF.value, None)))),
            6,
            msg="There should be values for a, b and the output",
        )

    def test_macro(self):
        wf = pwf.Workflow("operation")
        wf.node = operation(a=1.0, b=2.0)
        wf.run()
        data = export_to_dict(wf)
        self.assertEqual(
            set(data.keys()), {"edges", "inputs", "label", "nodes", "outputs"}
        )
        self.assertEqual(
            data["inputs"]["node__b"],
            {"default": 1.0, "value": 2.0, "type_hint": float},
        )

    def test_custom_labels(self):
        x0 = 5
        wf = pwf.Workflow("rename_channels")
        wf.a1 = AddOnetology(x0)
        wf.a3 = AddTwoMacrontology(wf.a1.outputs.z)
        out = wf()
        self.assertDictEqual(
            {"a3__zout": x0 + 3},
            out,
            msg="Giving custom labels to node or macro outputs should not cause any "
            "harm when combined with ontological validation.",
        )


@dataclasses.dataclass
class Input:
    T: u(float, units="kelvin")
    n: int

    @dataclasses.dataclass
    class parameters:
        a: int = 2

    class not_dataclass:
        b: int = 3


@dataclasses.dataclass
class Output:
    E: u(float, units="electron_volt")
    L: u(float, units="angstrom")


@pwf.as_function_node
def run_md(inp: Input) -> Output:
    out = Output(E=1.0, L=2.0)
    return out


class TestDataclass(unittest.TestCase):
    def test_dataclass(self):
        wf = pwf.Workflow("my_wf")
        inp = Input(T=300.0, n=100)
        inp.parameters.a = 1
        wf.node = run_md(inp)
        wf.run()
        data = export_to_dict(wf)
        graph = onto.get_knowledge_graph(data)
        i_txt = "my_wf.node.inputs.inp"
        o_txt = "my_wf.node.outputs.out"
        triples = (
            (
                rdflib.URIRef(f"{i_txt}.n.value"),
                rdflib.RDFS.subClassOf,
                rdflib.URIRef(f"{i_txt}.value"),
            ),
            (rdflib.URIRef(f"{i_txt}.n.value"), rdflib.RDF.value, rdflib.Literal(100)),
            (
                rdflib.URIRef(f"{i_txt}.parameters.a.value"),
                rdflib.RDF.value,
                rdflib.Literal(1),
            ),
            (
                rdflib.URIRef(o_txt),
                onto.SNS.hasValue,
                rdflib.URIRef(f"{o_txt}.E.value"),
            ),
        )
        s = graph.serialize(format="turtle")
        for ii, triple in enumerate(triples):
            with self.subTest(i=ii):
                self.assertEqual(
                    len(list(graph.triples(triple))),
                    1,
                    msg=f"{triple} not found in {s}",
                )
        self.assertIsNone(graph.value(rdflib.URIRef(f"{i_txt}.not_dataclass.b.value")))


class Meal: ...


class Garbage: ...


@pwf.as_function_node("pizza")
def PreparePizza() -> u(Meal, uri=EX.Pizza):
    return Meal()


@pwf.as_function_node("unidentified_meal")
def PrepareNonOntologicalMeal() -> Meal:
    return Meal()


@pwf.as_function_node("rice")
def PrepareRice() -> u(Meal, uri=EX.Rice):
    return Meal()


@pwf.as_function_node("garbage")
def PrepareGarbage() -> u(Garbage, uri=EX.Garbage):
    return Garbage()


@pwf.as_function_node("garbage")
def PrepareUnhintedGarbage():
    return Garbage()


@pwf.as_function_node("verdict")
def Eat(meal: u(Meal, uri=EX.Meal)) -> str:
    return f"Yummy {meal.__class__.__name__} meal"


@pwf.as_function_node("verdict")
def EatPizza(meal: u(Meal, uri=EX.Pizza)) -> str:
    return f"Yummy {meal.__class__.__name__} pizza"


class TestValidation(unittest.TestCase):
    def test_connection_validity(self):
        with self.subTest("Fully hinted"):
            wf = pwf.Workflow("ontoflow")
            wf.make = PreparePizza()
            wf.eat = EatPizza(wf.make)
            out = wf()
            self.assertTrue(
                out,
                msg="With everything hinted correctly, output should be produced as "
                "normal",
            )

        with self.subTest("Upstream type hint is missing"):
            wf = pwf.Workflow("no_type")
            wf.make = PrepareUnhintedGarbage()
            wf.eat = EatPizza(wf.make)
            wf.recovery = None  # Disable recovery so there's nothing to clean up
            with self.assertRaises(
                FailedChildError,
                msg="We should be allowed to form the connection (since the source has "
                "no hint), but at runtime, we expect to fail when we try to actually "
                "assign the value. Ontological typing should not interfere with this.",
            ):
                wf()

        with self.subTest("Upstream type hint is wrong"):
            wf = pwf.Workflow("no_type")
            wf.make = PrepareGarbage()
            wf.eat = EatPizza()
            with self.assertRaises(
                ChannelConnectionError,
                msg="Expected to be stopped by an type-hint invalid connection. "
                "Ontological hinting should not impact this.",
            ):
                wf.eat.inputs.meal = wf.make

        with self.subTest("Upstream ontological hint is missing"):
            wf = pwf.Workflow("no_type")
            wf.make = PrepareNonOntologicalMeal()
            wf.eat = EatPizza(wf.make)
            out = wf()
            self.assertTrue(
                out,
                msg="As with regular type hints, if one ontological hint is missing, "
                "we don't expect to perform ontological validation -- so this should "
                "produce output just fine.",
            )

        with self.subTest("Upstream ontological hint is wrong"):
            wf = pwf.Workflow("no_type")
            wf.make = PrepareRice()
            wf.eat = EatPizza()
            with self.assertRaises(
                ChannelConnectionError,
                msg="Expected to be stopped by an ontologically invalid connection.",
            ):
                wf.eat.inputs.meal = wf.make

        with self.subTest("Downstream ontological hint is less specific"):
            with self.subTest("Naively"):
                wf = pwf.Workflow("missing_information")
                wf.make = PrepareRice()
                wf.eat = Eat()
                with self.assertRaises(
                    ChannelConnectionError,
                    msg="The validator has no way of knowing that rice is a meal, so "
                    "unfortunately this should indeed fail.",
                ):
                    wf.eat.inputs.meal = wf.make

            with self.subTest("With a primed graph"):
                wf = pwf.Workflow("prepopulated_knowledge_graph")
                wf.knowledge = rdflib.Graph()
                wf.knowledge.add((EX.Rice, rdflib.RDFS.subClassOf, EX.Meal))
                wf.make = PrepareRice()
                wf.eat = Eat(wf.make)
                out = wf()
                self.assertTrue(
                    out,
                    msg="We can supply graph knowledge prior to validation. Here we "
                    "test the interface: `knowledge` attribute of the topmost object "
                    "for this purpose.",
                )


if __name__ == "__main__":
    unittest.main()
