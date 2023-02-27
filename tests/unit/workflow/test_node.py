from unittest import TestCase
from typing import Optional, Union

from pyiron_contrib.workflow.node import Node


def throw_error(x: Optional[int] = None):
    raise RuntimeError


def plus_one(x=1) -> Union[int, float]:
    return x + 1


class ChildNode(Node):
    """
    All sub-compnents are defined, but __init__ is not overriden -- should throw an
    error every time we give non-None input for these!
    """

    def __init__(
            self,
            label: Optional[str] = None,
            update_automatically: bool = True,
            update_now: bool = True,
            **kwargs
    ):
        super().__init__(
                node_function=plus_one,
                output_labels=("y",),
                label=label,
                update_automatically=update_automatically,
                update_now=update_now,
                **kwargs,
        )


class TestNode(TestCase):
    def test_defaults(self):
        node = Node(node_function=plus_one, output_labels=["y"])

    def test_instantiation_update(self):
        no_update = Node(
            node_function=plus_one,
            output_labels=("y",),
            update_now=False
        )
        self.assertIsNone(no_update.outputs.y.value)

        update = Node(
            node_function=plus_one,
            output_labels=("y",),
            update_now=True
        )
        self.assertEqual(2, update.outputs.y.value)

    def test_input_kwargs(self):
        node = Node(
            node_function=plus_one,
            output_labels=("y",),
            x=2
        )
        self.assertEqual(3, node.outputs.y.value, msg="Initialize from value")

        node2 = Node(
            node_function=plus_one,
            output_labels=("y",),
            x=node.outputs.y
        )
        node.update()
        self.assertEqual(4, node2.outputs.y.value, msg="Initialize from connection")

    def test_automatic_updates(self):
        node = Node(
            node_function=throw_error,
            output_labels=(),
            update_now=False,
        )

        with self.subTest("Shouldn't run for invalid input on update"):
            node.inputs.x.update("not an int")

        with self.subTest("Valid data should trigger a run"):
            with self.assertRaises(RuntimeError):
                node.inputs.x.update(1)
