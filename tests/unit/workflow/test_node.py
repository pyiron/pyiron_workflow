from unittest import TestCase
from typing import Optional, get_args

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.channels import ChannelTemplate


def throw_error(x: Optional[int] = None):
    raise RuntimeError


def plus_one(x=1):
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
                output_names=("y",),
                name=name,
                update_automatically=update_automatically,
                update_now=update_now,
                **kwargs,
        )


class TestNode(TestCase):
    def test_defaults(self):
        node = Node(node_function=plus_one, output_names=["y"])

    def test_instantiation_update(self):
        no_update = Node(
            node_function=plus_one,
            output_names=("y",),
            update_now=False
        )
        self.assertIsNone(no_update.output.y.value)

        update = Node(
            node_function=plus_one,
            output_names=("y",),
            update_now=True
        )
        self.assertEqual(2, update.output.y.value)

    def test_input_kwargs(self):
        node = Node(
            node_function=plus_one,
            output_names=("y",),
            x=2
        )
        self.assertEqual(3, node.output.y.value, msg="Initialize from value")

        node2 = Node(
            node_function=plus_one,
            output_names=("y",),
            x=node.output.y
        )
        node.update()
        self.assertEqual(4, node2.output.y.value, msg="Initialize from connection")

    def test_automatic_updates(self):
        node = Node(
            node_function=throw_error,
            output_names=(),
            update_now=False,
        )

        with self.subTest("Shouldn't run for invalid input on update"):
            node.input.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.input.x.update(1)
