from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.channels import InputData, OutputData
from pyiron_contrib.workflow.io import Inputs, Outputs


class DummyNode:
    def __init__(self):
        self.running = False
        self.label = "node_label"

    def update(self):
        pass


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestIO(TestCase):

    @classmethod
    def setUp(self) -> None:
        node = DummyNode()
        self.inputs = [
            InputData(label="x", node=node, default=0, type_hint=float),
            InputData(label="y", node=node, default=1, type_hint=float)
        ]
        outputs = [
            OutputData(label="a", node=node, type_hint=float),
        ]

        self.post_facto_output = OutputData(label="b", node=node, type_hint=float)

        self.input = Inputs(*self.inputs)
        self.output = Outputs(*outputs)

    def test_access(self):
        self.assertEqual(self.input.x, self.input["x"])

    def test_assignment(self):
        with self.assertRaises(TypeError):
            self.input.foo = "not an input channel"

        with self.subTest("Can assign to a key that is not the label"):
            label_before_assignment = self.post_facto_output.label
            self.output.not_this_channels_name = self.post_facto_output
            self.assertIs(
                self.output.not_this_channels_name,
                self.post_facto_output,
                msg="Expected channel to get assigned"
            )
            self.assertEqual(
                self.post_facto_output.label,
                label_before_assignment,
                msg="Labels should not get updated on assignment of channels to IO "
                    "collections"
            )

        with self.assertRaises(TypeError):
            # Right label, and a channel, but wrong type of channel
            self.input.b = self.post_facto_output

        with self.subTest("Successful channel assignment"):
            self.output.b = self.post_facto_output

    def test_connection(self):
        self.input.x = self.input.y
        self.assertEqual(
            0,
            len(self.input.x.connections),
            msg="Shouldn't be allowed to connect two inputs, but only passes warning"
        )

        self.input.x = self.output.a
        self.assertIn(
            self.input.x,
            self.output.a.connections,
            msg="Should be able to create connections by assignment"
        )

        self.input.x = 7
        self.assertEqual(self.input.x.value, 7)

    def test_conversion(self):
        converted = self.input.to_value_dict()
        for template in self.inputs:
            self.assertEqual(template.default, converted[template.label])
        self.assertEqual(
            len(self.inputs),
            len(converted),
            msg="And it shouldn't have any extra items either"
        )

    def test_iteration(self):
        self.assertTrue(all([c.label in self.input.labels for c in self.input]))