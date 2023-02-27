from unittest import TestCase


from pyiron_contrib.workflow.channels import ChannelTemplate
from pyiron_contrib.workflow.io import Input, Output


class DummyNode:
    def update(self):
        pass


class TestIO(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.inputs = [
            ChannelTemplate(name="x", default=0, types=float),
            ChannelTemplate(name="y", default=1, types=float)
        ]
        outputs = [
            ChannelTemplate(name="a", types=float),
        ]
        node = DummyNode()

        cls.post_facto_output = ChannelTemplate(name="b", types=float).to_output(node)

        cls.input = Input(node, *cls.inputs)
        cls.output = Output(node, *outputs)

    def test_access(self):
        self.assertEqual(self.input.x, self.input["x"])

    def test_assignment(self):
        with self.assertRaises(TypeError):
            self.input.foo = "not an input channel"

        with self.assertRaises(ValueError):
            self.output.not_this_channels_name = self.post_facto_output

        with self.assertRaises(TypeError):
            # Right name, and a channel, but wrong type of channel
            self.input.b = self.post_facto_output

        with self.subTest("Successful channel assignment"):
            self.output.b = self.post_facto_output

    def test_connection(self):
        with self.assertRaises(TypeError):
            # Tries to make a connection, but can't connect to non-channels
            self.input.x = "foo"

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

    def test_conversion(self):
        converted = self.input.to_value_dict()
        for template in self.inputs:
            self.assertEqual(template.default, converted[template.name])
        self.assertEqual(
            len(self.inputs),
            len(converted),
            msg="And it shouldn't have any extra items either"
        )

    def test_iteration(self):
        self.assertTrue(all([c.name in self.input.names for c in self.input]))