import unittest
from pyiron_ontology.parser import get_inputs_and_outputs, get_triples
from pyiron_workflow import Workflow
from semantikon.typing import u
from rdflib import Namespace


@Workflow.wrap.as_function_node("speed")
def calculate_speed(
    distance: u(float, units="meter"),
    time: u(float, units="second"),
) -> u(float, units="meter/second"):
    return distance / time


class TestParser(unittest.TestCase):
    def test_parser(self):
        c = calculate_speed()
        output_dict = get_inputs_and_outputs(c)
        for label in ["inputs", "outputs", "function", "label"]:
            self.assertIn(label, output_dict)

    def test_triples(self):
        EX = Namespace("http://example.org/")
        speed = calculate_speed()
        data = get_inputs_and_outputs(speed)
        graph = get_triples(data, EX)
        self.assertGreater(
            len(list(graph.triples((None, EX.hasUnits, EX["meter/second"])))), 0
        )


if __name__ == "__main__":
    unittest.main()
