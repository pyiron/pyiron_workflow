from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class Workflow:
    def __init__(self, name: str, *nodes: Node):
        self.name = name
        self.nodes = [node for node in nodes]

    @property
    def input(self):
        # These are hideously named (but unique!!), and regenerating the whole thing at
        # each access is computationally inefficient, but for this toy example I don't
        # care
        return {
            f"{node.__class__.__name__}{i}_{name}": inp
            for i, node in enumerate(self.nodes)
            for name, inp in node.input.items()
            if len(inp.connections) == 0
        }

    @property
    def output(self):
        return {
            f"{node.__class__.__name__}{i}_{name}": out
            for i, node in enumerate(self.nodes)
            for name, out in node.output.items()
            if len(out.connections) == 0
        }

    # (De)serialization is necessary throughout these classes, but not implemented here
    def serialize(self):
        raise NotImplementedError

    def deserialize(self, source):
        raise NotImplementedError

    def run(self):
        # Maybe we need this? I'm not sure right now. It's not necessary for the example
        raise NotImplementedError