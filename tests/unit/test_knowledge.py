import dataclasses
import unittest

import rdflib
from semantikon import ontology as onto
from semantikon.metadata import u

import pyiron_workflow as pwf
from pyiron_workflow.channels import ChannelConnectionError
from pyiron_workflow.knowledge import (
    SemantikonRecipeChange,
    export_to_dict,
    is_involved,
    is_valid,
    parse_workflow,
    validate_workflow,
)
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


@pwf.as_function_node
def Up(x: u(str, uri=EX.TriggerOnto)) -> u(str, uri=EX.TriggerOnto):
    return x


@pwf.as_function_node
def Middle(
    y: u(str, uri=EX.TriggerOnto),
) -> u(str, uri=EX.TriggerOnto, triples=(EX.hasThing, EX.thing)):
    return y


@pwf.as_function_node
def Down(
    z: u(
        str,
        uri=EX.TriggerOnto,
        restrictions=(
            (rdflib.OWL.onProperty, EX.hasThing),
            (rdflib.OWL.someValuesFrom, EX.thing),
        ),
    ),
):
    return z


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


class Clothes:
    pass


@pwf.as_function_node
def Wash(
    clothes: u(Clothes, uri=EX.Clothes),
) -> u(Clothes, triples=(EX.hasProperty, EX.cleaned), derived_from="inputs.clothes"):
    ...
    return clothes


@pwf.as_function_node
def Dye(clothes: u(Clothes, uri=EX.Clothes), color="blue") -> u(
    Clothes,
    triples=(EX.hasProperty, EX.color),
    derived_from="inputs.clothes",
):
    ...
    return clothes


@pwf.as_function_node
def Sell(
    clothes: u(
        Clothes,
        uri=EX.Clothes,
        restrictions=(
            (
                (rdflib.OWL.onProperty, EX.hasProperty),
                (rdflib.OWL.someValuesFrom, EX.cleaned),
            ),
            (
                (rdflib.OWL.onProperty, EX.hasProperty),
                (rdflib.OWL.someValuesFrom, EX.color),
            ),
        ),
    ),
) -> int:
    price = 10
    return price


@pwf.as_function_node
def DyeWithCancel(clothes: Clothes, color="blue") -> u(
    Clothes,
    triples=(EX.hasProperty, EX.color),
    derived_from="inputs.clothes",
    cancel=(EX.hasProperty, EX.cleaned),
):
    return clothes


@pwf.as_macro_node
def CorrectMacro(self, clothes: Clothes):
    self.dyed_clothes = Dye(clothes)
    self.washed_clothes = Wash(self.dyed_clothes)
    self.money = Sell(self.washed_clothes)
    return self.money


@pwf.as_macro_node
def IncorrectMacro(self, clothes: Clothes):
    self.washed_clothes = Wash(clothes)
    self.money = Sell(self.washed_clothes)
    return self.money


@pwf.as_function_node
def IOTransformer(x: u(int, uri=EX.Input)) -> u(int, uri=EX.Output):
    y = x
    return y


@pwf.as_macro_node
def MatchingWrapper(self, x_outer: u(int, uri=EX.Input)) -> u(int, uri=EX.Output):
    self.add = IOTransformer(x_outer)
    return self.add


@pwf.as_macro_node
def MismatchingInput(self, x_outer: u(int, uri=EX.NotInput)) -> u(int, uri=EX.Output):
    self.add = IOTransformer(x_outer)
    return self.add


@pwf.as_macro_node
def MismatchingOutput(
    self, x_outer: u(int, uri=EX.NotInput)
) -> u(int, uri=EX.NotOutput):
    self.add = IOTransformer(x_outer)
    return self.add


@pwf.as_function_node
def Distance(x: u(float, units="meter")) -> u(float, derived_from="inputs.x"):
    return x


@pwf.as_function_node
def Speed(
    dx: u(float, units="meter"), dt: u(float, units="second")
) -> u(float, units="meter/second"):
    s = dx / dt
    return s


@pwf.as_function_node
def NanoTime(t: u(float, units="nanosecond")) -> u(float, units="nanosecond"):
    return t


@pwf.as_function_node
def Time(t: u(float, units="second")) -> u(float, units="second"):
    return t


@pwf.as_function_node
def Canada(
    british_distance: u(float, units="mile"),
) -> u(float, derived_from="inputs.driving"):
    canadian_distance = british_distance
    return canadian_distance


@pwf.as_function_node
def HasNeed(
    x: u(
        str,
        uri=EX.Foo,
        restrictions=(
            (rdflib.OWL.onProperty, EX.hasNeed),
            (rdflib.OWL.someValuesFrom, EX.need),
        ),
    ) = "foo",
) -> u(int, uri=EX.Data):
    data = 42
    return data


@pwf.as_macro_node
def UnconnectedMacro(self):
    self.needy = HasNeed()
    self.adds = AddOnetology(1)
    self.transforms = IOTransformer(2)
    return self.adds, self.transforms


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

    def test_restrictions(self):
        with self.subTest("Restrictions fulfilled"):
            wf = pwf.Workflow("my_correct_workflow")
            wf.dyed_clothes = Dye(Clothes())
            wf.washed_clothes = Wash(wf.dyed_clothes)
            wf.money = Sell(wf.washed_clothes)
            out = wf()
            self.assertTrue(
                out,
                msg="Expected type and restrictions should be fulfilled by upstream "
                "derivations and added triple.",
            )

        with self.subTest("Restrictions denied by cancellation"):
            wf = pwf.Workflow("my_wf_with_cancellation")
            wf.washed_clothes = Wash(Clothes())
            wf.unclean_dyed_clothes = DyeWithCancel(wf.washed_clothes)
            wf.money = Sell()
            with self.assertRaises(
                ChannelConnectionError,
                msg="Expect the cancel declaration to have cleared a required triple,"
                "leading to failed ontological validation",
            ):
                wf.money.inputs.clothes = wf.unclean_dyed_clothes

    def test_unrelated_problems(self):
        wf = pwf.Workflow("connect_later")
        wf.up = Up("we only want to probe the relevant connection")
        wf.middle = Middle()
        wf.down = Down()
        wf.extra = Up()
        self.assertFalse(
            is_valid(validate_workflow(wf)),
            msg="Sanity check: the overall workflow should be invalid due to the Down "
            "node restrictions being unfulfilled",
        )
        with self.subTest(
            "Even though the whole graph will not validate, for the purpose of "
            "ontologically validating connections, we only care whether "
            "_that connection_ causes problems. Thus, connecting up and middle should "
            "be totally fine."
        ):
            wf.middle.inputs.y = wf.up.outputs.x

        wf.down.inputs.z.disconnect_all()
        with self.subTest(
            "Even while the _node_ that invalidates the graph is wrong, we should "
            "still allow _other channels_ on that node to participate in validated "
            "connections."
        ):
            wf.extra.inputs.x = wf.down.outputs.z
            self.assertFalse(
                is_valid(validate_workflow(wf)),
                msg="Sanity check: the overall workflow should be invalid due to the "
                "Down node restrictions being unfulfilled",
            )

    def test_macros(self):
        with self.subTest("Correct macro"):
            wf = pwf.Workflow("my_correct_workflow")
            wf.dyed_clothes = Dye(Clothes())
            wf.washed_clothes = Wash(wf.dyed_clothes)
            wf.money = Sell(wf.washed_clothes)
            out = wf()
            macro_out = CorrectMacro(Clothes()).run()
            self.assertListEqual(
                list(out.values()),
                list(macro_out.values()),
                msg="We can wrap ontological workflows in macros.",
            )

        with (
            self.subTest("Incorrect macro"),
            self.assertRaises(
                ChannelConnectionError,
                msg="It would be lovely to get this validation error when the macro "
                "definition is parsed -- so if this test fails and you can do that "
                "now, that is excellent! In the meantime, we try to construct macro "
                "subgraphs at macro instantiation time, and at that point expect to "
                "find out this macro subgraph won't validate.",
            ),
        ):
            IncorrectMacro()

        with self.subTest("Macro-subgraph communication"):
            with self.subTest("Fully matching"):
                self.assertTrue(MatchingWrapper(1).run())

            with (
                self.subTest("Bad parent input->child input flow"),
                self.assertRaises(ChannelConnectionError),
            ):
                MismatchingInput()

            with (
                self.subTest("Bad child output->parent output flow"),
                self.assertRaises(ChannelConnectionError),
            ):
                MismatchingOutput()

    def test_unparented(self):
        """
        If this behaviour changes, it may be because we are now able to ontologically
        validate node networks that exist outside a formal graph parent, or it may be
        because we've outlawed such networks. Either way, the user messaging in the
        ontology notebook will need to be updated if this test starts failing.
        """
        uses_data = AddOnetology(5)
        uses_io = IOTransformer()
        with self.assertRaises(
            ChannelConnectionError,
            msg="Ontological validation requires that both parties live in the same "
            "graph with the same graph root -- when they are not parented in a "
            "macro or workflow, this should fail hard.",
        ):
            uses_io.inputs.x = uses_data.outputs.z

        uses_data._validate_ontologies = False
        with self.assertRaises(
            ChannelConnectionError,
            msg="Disabling ontological validation for one party should not be enough",
        ):
            uses_io.inputs.x = uses_data.outputs.z

        uses_io._validate_ontologies = False
        uses_io.inputs.x = uses_data.outputs.z
        self.assertEqual(
            1,
            len(uses_io.inputs.x.connections),
            msg="In the error message, we inform the users of this escape hatch."
            "Change that messaging if this test starts to work differently.",
        )

    def test_units(self):
        wf = pwf.Workflow("speedometer")
        wf.dx = Distance(100)
        wf.speed = Speed(dx=wf.dx)

        with self.subTest("Wrong units"):
            wf.dt = NanoTime(10)
            with self.assertRaises(
                ChannelConnectionError,
                msg="Incorrect units should cause failed ontological validation",
            ):
                wf.speed.inputs.dt = wf.dt
            wf.remove_child(wf.dt)

        with self.subTest("Right units"):
            wf.dt = Time(10)
            wf.speed.inputs.dt = wf.dt
            out = wf()
            self.assertTrue(out, msg="Correct units connect fine.")

    def test_units_inheritance(self):
        wf = pwf.Workflow("we_drive_in_miles")
        wf.lets_use_metric = Canada()
        graph = parse_workflow(wf)
        reference_stem = f"{wf.label}.{wf.lets_use_metric.label}"
        british_units = graph.objects(
            rdflib.term.URIRef(f"{reference_stem}.inputs.british_distance.value"),
            onto.SNS.hasUnits,
        )
        self.assertEqual(
            1,
            len(list(british_units)),
            msg="Sanity check that we are parsing correctly for the units",
        )
        canadian_units = graph.objects(
            rdflib.term.URIRef(f"{reference_stem}.outputs.canadian_distance.value"),
            onto.SNS.hasUnits,
        )
        self.assertListEqual(
            [],
            list(canadian_units),
            msg="Of course actually Canada uses kilometers where the UK uses miles. "
            "But the point here is that unlike other properties, units are _not_ "
            "inherited. This is an intentional choice in semantikon "
            "(https://github.com/pyiron/semantikon/issues/256), but if that changes we "
            "need to update the documentation notebook here.",
        )

    def test_is_involved(self):
        wf = pwf.Workflow("validate_involvement")
        # We want to check the scope of the paths, so stick it all in a macro
        wf.macro = UnconnectedMacro()
        self.assertFalse(
            is_valid(validate_workflow(wf)),
            msg="Sanity check -- the needy node has unfilled requirements. Unless "
            "semantikon is changed to allow these to pass in the absence of an edge, "
            "this should cause validation to fail.",
        )
        connect_matching = SemantikonRecipeChange(
            [wf.label, wf.macro.label],
            (
                f"{wf.macro.needy.label}.outputs.{wf.macro.needy.outputs.data.label}",
                f"{wf.macro.adds.label}.inputs.{wf.macro.adds.inputs.x.label}",
            ),
        )
        matching_validation = validate_workflow(wf, connect_matching)
        self.assertFalse(
            is_valid(matching_validation),
            msg="Sanity check: we didn't fulfill the needs of the needy node, so this "
            "should still be invalid.",
        )
        self.assertFalse(
            is_involved(matching_validation, connect_matching),
            msg="Even though the workflow is invalid, this new connection is fine: "
            "the two counterparties have matching URI and that's all we were asking.",
        )

        connect_mismatching = SemantikonRecipeChange(
            [wf.label, wf.macro.label],
            (
                f"{wf.macro.needy.label}.outputs.{wf.macro.needy.outputs.data.label}",
                f"{wf.macro.transforms.label}.inputs.{wf.macro.transforms.inputs.x.label}",
            ),
        )
        mismatching_validation = validate_workflow(wf, connect_mismatching)
        self.assertFalse(
            is_valid(mismatching_validation),
            msg="Sanity check: it wasn't valid before, so it sure shouldn't be now.",
        )
        self.assertTrue(
            is_involved(mismatching_validation, connect_mismatching),
            msg="This time, the new connection in question _is_ involved in the "
            "validation failure, because of the mismatching URI.",
        )


if __name__ == "__main__":
    unittest.main()
