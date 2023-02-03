from unittest import TestCase

from pyiron_contrib.workflow.node import Node, pass_all
from pyiron_contrib.workflow.channels import ChannelTemplate


def throw_error(x=None):
    raise RuntimeError


def plus_one(x):
    return {'y': x + 1}


class ChildNode(Node):
    """
    All sub-compnents are defined, but __init__ is not overriden -- should throw an
    error every time we give non-None input for these!
    """
    input_channels = [ChannelTemplate(name='x', types=(int))]
    preprocessor = staticmethod(pass_all)
    engine = staticmethod(plus_one)
    # postprocessor = staticmethod(pass_all)  # We could define it as a property instead
    output_channels = [ChannelTemplate(name='y')]

    @staticmethod
    def postprocessor(**kwargs):
        return pass_all(**kwargs)


class TestNode(TestCase):
    def test_defaults(self):
        node = Node()

    def test_instantiation_update(self):
        no_update = Node(
            engine=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            update_now=False
        )
        self.assertIsNone(no_update.output.y.value)

        update = Node(
            engine=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            update_now=True
        )
        self.assertEqual(2, update.output.y.value)

    def test_input_kwargs(self):
        node = Node(
            engine=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            x=2
        )
        self.assertEqual(3, node.output.y.value)

    def test_automatic_updates(self):
        node = Node(
            engine=throw_error,
            input_channels=[ChannelTemplate(name='x', types=int)],
        )

        with self.subTest("Shouldn't run for invalid input on update"):
            node.input.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.input.x.update(1)

    def test_double_definitions(self):
        child = ChildNode()

        for kwargs in [
            {"input_channels": ChildNode.input_channels},
            {"preprocessor": ChildNode.preprocessor},
            {"engine": ChildNode.engine},
            {"postprocessor": ChildNode.postprocessor},
            {"output_channels": ChildNode.output_channels},
        ]:
            with self.assertRaises(ValueError):
                ChildNode("input_tries_to_override_class", **kwargs)
