from unittest import TestCase
from typing import Optional

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

    def __init__(
            self,
            name: Optional[str] = None,
            update_automatically: bool = True,
            update_now: bool = True,
            **kwargs
    ):
        super().__init__(
                node_function=plus_one,
                name=name,
                input_channels=[ChannelTemplate(name='x', types=(int))],
                preprocessor=pass_all,
                postprocessor=self.my_pass_all,
                output_channels=[ChannelTemplate(name='y')],
                update_automatically=update_automatically,
                update_now=update_now,
                **kwargs,
        )

    @staticmethod
    def my_pass_all(**kwargs):
        return pass_all(**kwargs)


class TestNode(TestCase):
    def test_defaults(self):
        with self.assertRaises(RuntimeError):
            node = Node(node_function=throw_error)

    def test_instantiation_update(self):
        no_update = Node(
            node_function=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            update_now=False
        )
        self.assertIsNone(no_update.output.y.value)

        update = Node(
            node_function=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            update_now=True
        )
        self.assertEqual(2, update.output.y.value)

    def test_input_kwargs(self):
        node = Node(
            node_function=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            x=2
        )
        self.assertEqual(3, node.output.y.value, msg="Initialize from value")

        node2 = Node(
            node_function=plus_one,
            input_channels=[ChannelTemplate(name='x', default=1)],
            output_channels=[ChannelTemplate(name='y')],
            x=node.output.y
        )
        node.update()
        self.assertEqual(4, node2.output.y.value, msg="Initialize from connection")

    def test_automatic_updates(self):
        node = Node(
            node_function=throw_error,
            input_channels=[ChannelTemplate(name='x', types=int)],
        )

        with self.subTest("Shouldn't run for invalid input on update"):
            node.input.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.input.x.update(1)
