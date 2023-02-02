from unittest import TestCase


from pyiron_contrib.workflow.channels import ChannelTemplate
from pyiron_contrib.workflow.io import Input, Output


class DummyNode:
    def update(self):
        pass


class TestIO(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        inputs = [
            ChannelTemplate(name="x", types=float),
            ChannelTemplate(name="y", types=float)
        ]
        outputs = [
            ChannelTemplate(name="a", types=float),
        ]
        node = DummyNode()

        cls.post_facto_output = ChannelTemplate(name="b", types=float).to_output(node)

        cls.input = Input(node, *inputs)
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


