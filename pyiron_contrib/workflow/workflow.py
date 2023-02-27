from __future__ import annotations

from warnings import warn

from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.util import DotDict


class Workflow:
    """
    Workflows are an abstraction for holding a collection of related nodes.

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the `nodes` dot-dictionary, or just directly
    by dot-access on the workflow object itself.

    The workflow guarantees that each node it owns has a unique within the scope of this
    workflow, and that each node instance appears only once.

    Using the `input` and `output` attributes, the workflow gives access to all the
    IO channels among its nodes which are currently unconnected.

    (TODO) Workflows can be serialized.

    (TODO) Once you're satisfied with how a workflow is structured, you can export it
        as a macro node for use in other workflows. (Maybe we should allow for nested
        workflows without exporting to a node? I was concerned then what happens to the
        nesting abstraction if, instead of accessing IO through the workflow's IO flags,
        a user manually connects IO from individual nodes from two different, nested or
        sibling workflows when those connections were _previously internal to their own
        workflow_. This seems very unsafe. Maybe there is something like a lock we can
        apply that falls short of a full export, but still guarantees the internal
        integrity of workflows when they're used somewhere else?
    """
    def __init__(self, label: str, *nodes: Node):
        self.__dict__['label'] = label
        self.__dict__['nodes'] = DotDict()
        for node in nodes:
            self.add(node)

    def add(self, node: Node):
        if node in self.nodes.values():
            raise ValueError(f"The node {node.label} is already in the workflow")

        if node.label in self.__dir__():
            raise ValueError(
                f"Cannot add a node with label {node.label}, that is already an attribute")

        i = 0
        while node.label in self.nodes.keys():
            warn(f"{node.label} is already a node; appending an index to the label...")
            node.label = f"{node.label}{i}"

        self.nodes[node.label] = node

    def remove(self, node: Node | str):
        if isinstance(node, Node):
            del self.nodes[node.label]
        else:
            del self.nodes[node]

    def __setattr__(self, label: str, node: Node):
        if label in self.__dir__():
            warn(
                f"{label} is already an attribute of {self.label} and cannot be "
                f"reassigned. If this is a node, you can remove the existing node "
                f"first to free the namespace."
            )
        elif not isinstance(node, Node):
            raise TypeError(f"Can only assign nodes, but got {type(node)}")
        else:
            if node.label != label:
                warn(
                    f"Reassigning the node {node.label} to the label {label} when "
                    f"adding it to the workflow {self.label}."
                )
            # TODO: Make sure the node belongs to no other workflows
            node.label = label
            self.add(node)

    def __getattr__(self, key):
        return self.nodes[key]

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def __iter__(self):
        return self.nodes.values().__iter__()

    def __len__(self):
        return len(self.nodes)

    @property
    def input(self):
        return DotDict(
            {
                f"{node.label}_{channel.label}": channel
                for node in self.nodes.values()
                for channel in node.inputs
                if not channel.connected
            }
        )

    @property
    def output(self):
        return DotDict(
            {
                f"{node.label}_{channel.label}": channel
                for node in self.nodes.values()
                for channel in node.outputs
                if not channel.connected
            }
        )

    def to_node(self):
        """
        Export the workflow to a macro node, with the currently exposed IO mapped to
        new IO channels, and the workflow mapped into the node_function.
        """
        raise NotImplementedError

    # (De)serialization is necessary throughout these classes, but not implemented here
    def serialize(self):
        raise NotImplementedError

    def deserialize(self, source):
        raise NotImplementedError

    def update(self):
        for node in self.nodes.values():
            if node.outputs.connected and not node.inputs.connected:
                node.update()

    def run(self):
        # Maybe we need this if workflows can be used as nodes?
        raise NotImplementedError