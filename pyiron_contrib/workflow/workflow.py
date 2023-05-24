from __future__ import annotations

from functools import partial
from warnings import warn

from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.node import Node, node, fast_node, single_value_node
from pyiron_contrib.workflow.node_library import atomistics, package, standard
from pyiron_contrib.workflow.util import DotDict


class _NodeAdder:
    """
    This class exists to help with the misdirection required for the syntactic sugar
    that lets us add nodes to the workflow.

    TODO: Give access to pre-built fixed nodes under various domain names
    """

    def __init__(self, workflow: Workflow):
        self._workflow = workflow
        self.atomistics = package.NodePackage(self._workflow, *atomistics.nodes)
        self.standard = package.NodePackage(self._workflow, *standard.nodes)

    Node = Node

    def __getattribute__(self, key):
        value = super().__getattribute__(key)
        if value == Node:
            return partial(Node, workflow=self._workflow)
        return value

    def __call__(self, node: Node):
        self._workflow.add_node(node)


class _NodeDecoratorAccess:
    """An intermediate container to store node-creating decorators as class methods."""

    node = node
    fast_node = fast_node
    single_value_node = single_value_node


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

        The `Workflow` class is designed as a single point of entry for workflows, so
        you can also access decorators to define new node classes right from the
        workflow (cf. the `Node` docs for more detail on the node types).
        Let's use these to explore a workflow's input and output, which are dynamically
        generated from the unconnected IO of its nodes:
        >>> @Workflow.wrap_as.fast_node("y")
        >>> def plus_one(x: int = 0):
        ...     return x + 1
        >>>
        >>> wf = Workflow("io_workflow")
        >>> wf.first = plus_one()
        >>> wf.second = plus_one()
        >>> print(len(wf.inputs), len(wf.outputs))
        2 2

        If we connect the output of one node to the input of the other, there are fewer
        dangling channels for the workflow IO to find:
        >>> wf.second.inputs.x = wf.first.outputs.y
        >>> print(len(wf.inputs), len(wf.outputs))
        1 1

        The workflow joins node lavels and channel labels with a `_` character to
        provide direct access to the output:
        >>> print(wf.outputs.second_y.value)
        2

        Workflows also give access to packages of pre-built nodes under different
        namespaces, e.g.
        >>> wf = Workflow("with_prebuilt")
        >>>
        >>> wf.structure = wf.add.atomistics.BulkStructure(
        ...     repeat=3,
        ...     cubic=True,
        ...     element="Al"
        ... )
        >>> wf.engine = atomistics.Lammps(structure=wf.structure)
        >>> wf.calc = atomistics.CalcMd(
        ...     job=wf.engine,
        ...     run_on_updates=True,
        ...     update_on_instantiation=True,
        ... )
        >>> wf.plot = standard.Scatter(
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

    wrap_as = _NodeDecoratorAccess

    def __init__(self, label: str, *nodes: Node, strict_naming=True):
        self.__dict__["label"] = label
        self.__dict__["nodes"] = DotDict()
        self.__dict__["add"] = _NodeAdder(self)
        self.__dict__["strict_naming"] = strict_naming
        # We directly assign using __dict__ because we override the setattr later

        for node in nodes:
            self.add_node(node)

    def add_node(self, node: Node, label: str = None) -> None:
        """
        Assign a node to the workflow. Optionally provide a new label for that node.

        Args:
            node (pyiron_contrib.workflow.node.Node): The node to add.
            label (Optional[str]): The label for this node.

        Raises:

        """
        if not isinstance(node, Node):
            raise TypeError(
                f"Only new node instances may be added, but got {type(node)}."
            )

        label = self._ensure_label_is_unique(node.label if label is None else label)
        self._ensure_node_belongs_to_at_most_this_workflow(node, label)

        self.nodes[label] = node
        node.label = label
        node.workflow = self

    def _ensure_node_belongs_to_at_most_this_workflow(self, node: Node, label: str):
        if (
            node.workflow is self  # This should guarantee the node is in self.nodes
            and label != node.label
        ):
            assert self.nodes[node.label] is node  # Should be unreachable by users
            warn(
                f"Reassigning the node {node.label} to the label {label} when "
                f"adding it to the workflow {self.label}."
            )
            del self.nodes[node.label]
        elif node.workflow is not None:
            raise ValueError(
                f"The node ({node.label}) already belongs to the workflow "
                f"{node.workflow.label}. Please remove it there before trying to "
                f"add it to this workflow ({self.label})."
            )

    def _ensure_label_is_unique(self, label):
        if label in self.__dir__():
            if isinstance(getattr(self, label), Node):
                if self.strict_naming:
                    raise AttributeError(
                        f"{label} is already the label for a node. Please remove it "
                        f"before assigning another node to this label."
                    )
                else:
                    label = self._add_suffix_to_label(label)
            else:
                raise AttributeError(
                    f"{label} is an attribute or method of the {self.__class__} class, "
                    f"and cannot be used as a node label."
                )
        return label

    def _add_suffix_to_label(self, label):
        i = 0
        new_label = label
        while new_label in self.nodes.keys():
            warn(
                f"{label} is already a node; appending an index to the "
                f"node label instead: {label}{i}"
            )
            new_label = f"{label}{i}"
            i += 1
        return new_label

    def activate_strict_naming(self):
        self.__dict__["strict_naming"] = True

    def deactivate_strict_naming(self):
        self.__dict__["strict_naming"] = False

    def remove(self, node: Node | str):
        if isinstance(node, Node):
            node.workflow = None
            node.disconnect()
            del self.nodes[node.label]
        else:
            del self.nodes[node]

    def __setattr__(self, label: str, node: Node):
        if not isinstance(node, Node):
            raise TypeError(
                "Only new node instances may be assigned as attributes. This is "
                "syntacic sugar for adding new nodes to the .nodes collection"
            )
        self.add_node(node, label=label)

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
    def inputs(self):
        return DotDict(
            {
                f"{node.label}_{channel.label}": channel
                for node in self.nodes.values()
                for channel in node.inputs
                if not channel.connected
            }
        )

    @property
    def outputs(self):
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
