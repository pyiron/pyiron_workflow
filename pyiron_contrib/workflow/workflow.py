"""
Provides the main workhorse class for creating and running workflows.

This class is intended as the single point of entry for users making an import.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiron_contrib.workflow.composite import Composite
from pyiron_contrib.workflow.io import Inputs, Outputs


if TYPE_CHECKING:
    from pyiron_contrib.workflow.node import Node


class Workflow(Composite):
    """
    Workflows are a dynamic composite node -- i.e. they hold and run a collection of
    nodes (a subgraph) which can be dynamically modified (adding and removing nodes,
    and modifying their connections).

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the `nodes` dot-dictionary, or just directly
    by dot-access on the workflow object itself.

    Using the `input` and `output` attributes, the workflow gives access to all the
    IO channels among its nodes which are currently unconnected.

    Examples:
        We allow adding nodes to workflows in five equivalent ways:
        >>> from pyiron_contrib.workflow.workflow import Workflow
        >>> from pyiron_contrib.workflow.function import Function
        >>>
        >>> def fnc(x=0): return x + 1
        >>>
        >>> n1 = Function(fnc, "x", label="n1")
        >>>
        >>> wf = Workflow("my_workflow", n1)  # As *args at instantiation
        >>> wf.add(Function(fnc, "x", label="n2"))  # Passing a node to the add caller
        >>> wf.add.Function(fnc, "y", label="n3")  # Instantiating from add
        >>> wf.n4 = Function(fnc, "y", label="whatever_n4_gets_used")
        >>> # By attribute assignment
        >>> Function(fnc, "x", label="n5", parent=wf)
        >>> # By instantiating the node with a workflow

        By default, the node naming scheme is strict, so if you try to add a node to a
        label that already exists, you will get an error. This behaviour can be changed
        at instantiation with the `strict_naming` kwarg, or afterwards by assigning a
        bool to this property. When deactivated, repeated assignments to the same label
        just get appended with an index:
        >>> wf.deactivate_strict_naming()
        >>> wf.my_node = Function(fnc, "y", x=0)
        >>> wf.my_node = Function(fnc, "y", x=1)
        >>> wf.my_node = Function(fnc, "y", x=2)
        >>> print(wf.my_node.inputs.x, wf.my_node0.inputs.x, wf.my_node1.inputs.x)
        0, 1, 2

        The `Workflow` class is designed as a single point of entry for workflows, so
        you can also access decorators to define new node classes right from the
        workflow (cf. the `Node` docs for more detail on the node types).
        Let's use these to explore a workflow's input and output, which are dynamically
        generated from the unconnected IO of its nodes:
        >>> @Workflow.wrap_as.function_node("y")
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
        >>> wf.engine = wf.add.atomistics.Lammps(structure=wf.structure)
        >>> wf.calc = wf.add.atomistics.CalcMd(
        ...     job=wf.engine,
        ...     run_on_updates=True,
        ...     update_on_instantiation=True,
        ... )
        >>> wf.plot = wf.add.standard.Scatter(
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
        super().__init__(label=label, parent=None, strict_naming=strict_naming)

        for node in nodes:
            self.add_node(node)

    @property
    def inputs(self) -> Inputs:
        inputs = Inputs()
        for node_label, node in self.nodes.items():
            for channel in node.inputs:
                if not channel.connected:
                    inputs[f"{node_label}_{channel.label}"] = channel
        return inputs

    @property
    def outputs(self) -> Outputs:
        outputs = Outputs()
        for node_label, node in self.nodes.items():
            for channel in node.outputs:
                if not channel.connected:
                    outputs[f"{node_label}_{channel.label}"] = channel
        return outputs

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

    @property
    def parent(self) -> None:
        return None

    @parent.setter
    def parent(self, new_parent: None):
        # Currently workflows are not allowed to have a parent -- maybe we want to
        # change our minds on this in the future? If we do, we can just expose `parent`
        # as a kwarg and roll back this private var/property/setter protection and let
        # the super call in init handle everything
        if new_parent is not None:
            raise TypeError(
                f"{self.__class__} may only take None as a parent but got "
                f"{type(new_parent)}"
            )
