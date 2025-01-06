import unittest
from pyiron_ontology.parser import get_inputs_and_outputs
from pyiron_workflow import Workflow
from semantikon.typing import u


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
        for label in ["input", "output", "function", "label"]:
            self.assertIn(label, output_dict)


if __name__ == "__main__":
    unittest.main()
