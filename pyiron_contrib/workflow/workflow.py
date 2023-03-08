from __future__ import annotations

from functools import partial
from warnings import warn

from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.util import DotDict


class _NodeAdder:
    """
    This class exists to help with the misdirection required for the syntactic sugar
    that lets us add nodes to the workflow.

    TODO: Give access to pre-built fixed nodes under various domain names
    """
    def __init__(self, workflow: Workflow):
        self._workflow = workflow

    Node = Node

    def __getattribute__(self, key):
        value = super().__getattribute__(key)
        if value == Node:
            return partial(Node, workflow=self._workflow)
        return value

    def __call__(self, node: Node):
        if node.workflow is not None:
            raise ValueError(
                f"This node ({node.label}) already belongs to the workflow "
                f"{node.workflow.label}. Please remove it there before trying to add it"
                f"to this workflow ({self._workflow.label})."
            )

        if node.label in self._workflow.__dir__() and \
                not isinstance(getattr(self._workflow, node.label), Node):
            raise AttributeError(
                f"Cannot add a node with label {node.label}, that is already an "
                f"attribute"
            )

        if self._workflow.strict_naming and node.label in self._workflow.nodes.keys():
            raise AttributeError(
                f"Cannot add a node with label {node.label}, that is already a node."
            )

        # Otherwise, if not strict then iterate on name
        i = 0
        label = node.label
        while label in self._workflow.nodes.keys():
            warn(f"{label} is already a node; appending an index to the label...")
            label = f"{node.label}{i}"
            i += 1
        node.label = label
        # Or this while loop just terminates immediately if the name is unique

        self._workflow.nodes[node.label] = node
        node.workflow = self._workflow


class Workflow(HasToDict):
    """
    Workflows are an abstraction for holding a collection of related nodes.

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the `nodes` dot-dictionary, or just directly
    by dot-access on the workflow object itself.

    The workflow guarantees that each node it owns has a unique within the scope of this
    workflow, and that each node instance appears only once.

    Using the `input` and `output` attributes, the workflow gives access to all the
    IO channels among its nodes which are currently unconnected.

    Examples:
        We allow adding nodes to workflows in five equivalent ways:
        >>> from pyiron_contrib.workflow.workflow import Workflow
        >>> from pyiron_contrib.workflow.node import Node
        >>>
        >>> def fnc(x=0): return x + 1
        >>>
        >>> n1 = Node(fnc, "x", label="n1")
        >>>
        >>> wf = Workflow("my_workflow", n1)  # As *args at instantiation
        >>> wf.add(Node(fnc, "x", label="n2"))  # Passing a node to the add caller
        >>> wf.add.Node(fnc, "y", label="n3")  # Instantiating from add
        >>> wf.n4 = Node(fnc, "y", label="whatever_n4_gets_used")
        >>> # By attribute assignment
        >>> Node(fnc, "x", label="n5", workflow=wf)
        >>> # By instantiating the node with a workflow

        By default, the node naming scheme is strict, so if you try to add a node to a
        label that already exists, you will get an error. This behaviour can be changed
        at instantiation with the `strict_naming` kwarg, or afterwards with the
        `(de)activate_strict_naming()` method(s). When deactivated, repeated assignments
        to the same label just get appended with an index:
        >>> wf.deactivate_strict_naming()
        >>> wf.my_node = Node(fnc, "y", x=0)
        >>> wf.my_node = Node(fnc, "y", x=1)
        >>> wf.my_node = Node(fnc, "y", x=2)
        >>> print(wf.my_node.inputs.x, wf.my_node0.inputs.x, wf.my_node1.inputs.x)
        0, 1, 2

        We can also use pre-built nodes, e.g.
        >>> from pyiron_contrib.workflow import nodes
        >>>
        >>> wf = Workflow("with_prebuilt")
        >>>
        >>> wf.structure = nodes.BulkStructure(repeat=3, cubic=True, element="Al")
        >>> wf.engine = nodes.Lammps(structure=wf.structure.outputs.structure)
        >>> wf.calc = nodes.CalcMD(job=wf.engine.outputs.job)
        >>> wf.plot = nodes.Scatter(
        ...     x=wf.calc.outputs.steps,
        ...     y=wf.calc.outputs.temperature
        ... )


    TODO: Workflows can be serialized.

    TODO: Once you're satisfied with how a workflow is structured, you can export it
        as a macro node for use in other workflows. (Maybe we should allow for nested
        workflows without exporting to a node? I was concerned then what happens to the
        nesting abstraction if, instead of accessing IO through the workflow's IO flags,
        a user manually connects IO from individual nodes from two different, nested or
        sibling workflows when those connections were _previously internal to their own
        workflow_. This seems very unsafe. Maybe there is something like a lock we can
        apply that falls short of a full export, but still guarantees the internal
        integrity of workflows when they're used somewhere else?
    """
    def __init__(self, label: str, *nodes: Node, strict_naming=True):
        self.__dict__['label'] = label
        self.__dict__['nodes'] = DotDict()
        self.__dict__['add'] = _NodeAdder(self)
        self.__dict__['strict_naming'] = strict_naming
        # We directly assign using __dict__ because we override the setattr later

        for node in nodes:
            self.add(node)

    def activate_strict_naming(self):
        self.__dict__['strict_naming'] = True

    def deactivate_strict_naming(self):
        self.__dict__['strict_naming'] = False

    def remove(self, node: Node | str):
        if isinstance(node, Node):
            node.workflow = None
            node.disconnect()
            del self.nodes[node.label]
        else:
            del self.nodes[node]

    def __setattr__(self, label: str, node: Node):
        if label in self.__dir__() and not isinstance(getattr(self, label), Node):
            raise AttributeError(
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
            old_label = node.label
            old_workflow = node.workflow
            try:
                node.label = label
                self.add(node)
            except:
                node.label = old_label
                node.workflow = old_workflow
                raise

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

    def to_dict(self):
        return {
            "label": self.label,
            "nodes": {n.label: n.to_dict() for n in self.nodes.values()},
        }

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

    def __dir__(self):
        return set(super().__dir__() + list(self.nodes.keys()))
