"""
Provides the main workhorse class for creating and running workflows.

This class is intended as the single point of entry for users making an import.
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from pyiron_workflow.composite import Composite
from pyiron_workflow.io import Inputs, Outputs


if TYPE_CHECKING:
    from bidict import bidict

    from pyiron_workflow.channels import InputData, OutputData
    from pyiron_workflow.node import Node


class Workflow(Composite):
    """
    Workflows are a dynamic composite node -- i.e. they hold and run a collection of
    nodes (a subgraph) which can be dynamically modified (adding and removing nodes,
    and modifying their connections).

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the `nodes` dot-dictionary, or just directly
    by dot-access on the workflow object itself.

    Using the `input` and `output` attributes, the workflow gives by-reference access
    to all the IO channels among its nodes which are currently unconnected.

    The `Workflow` class acts as a single-point-of-import for us;
    Directly from the class we can use the `create` method to instantiate workflow
    objects.
    When called from a workflow _instance_, any created nodes get their parent set to
    the workflow instance being used.

    Workflows are "living" -- i.e. their IO is always by reference to their owned nodes
    and you are meant to add and remove nodes as children -- and "parent-most" -- i.e.
    they sit at the top of any data dependency tree and may never have a parent of
    their own.
    They are flexible and great for development, but once you have a setup you like,
    you should consider reformulating it as a `Macro`, which operates somewhat more
    efficiently.

    Promises (in addition parent class promises):
    - Workflows are living, their IO always reflects their current state of child nodes
    - Workflows are parent-most objects, they cannot be a sub-graph of a larger graph

    Examples:
        We allow adding nodes to workflows in five equivalent ways:
        >>> from pyiron_workflow.workflow import Workflow
        >>>
        >>> @Workflow.wrap_as.single_value_node()
        ... def fnc(x=0):
        ...     return x + 1
        >>>
        >>> # (1) As *args at instantiation
        >>> n1 = fnc(label="n1")
        >>> wf = Workflow("my_workflow", n1)
        >>>
        >>> # (2) Being passed to the `add` method
        >>> n2 = wf.add_node(fnc(label="n2"))
        >>>
        >>> # (3) By attribute assignment
        >>> wf.n3 = fnc(label="anyhow_n3_gets_used")
        >>>
        >>> # (4) By creating from the workflow class but specifying the parent kwarg
        >>> n4 = fnc(label="n4", parent=wf)

        By default, the node naming scheme is strict, so if you try to add a node to a
        label that already exists, you will get an error. This behaviour can be changed
        at instantiation with the `strict_naming` kwarg, or afterwards by assigning a
        bool to this property. When deactivated, repeated assignments to the same label
        just get appended with an index:
        >>> wf.strict_naming = False
        >>> wf.my_node = fnc(x=0)
        >>> wf.my_node = fnc(x=1)
        >>> wf.my_node = fnc(x=2)
        >>> print(wf.my_node.inputs.x, wf.my_node0.inputs.x, wf.my_node1.inputs.x)
        0 1 2

        The `Workflow` class is designed as a single point of entry for workflows, so
        you can also access decorators to define new node classes right from the
        workflow (cf. the `Node` docs for more detail on the node types).
        Let's use these to explore a workflow's input and output, which are dynamically
        generated from the unconnected IO of its nodes:
        >>> @Workflow.wrap_as.function_node("y")
        ... def plus_one(x: int = 0):
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

        Then we just run the workflow
        >>> out = wf.run()

        The workflow joins node lavels and channel labels with a `_` character to
        provide direct access to the output:
        >>> print(wf.outputs.second__y.value)
        2

        These input keys can be used when calling the workflow to update the input. In
        our example, the nodes update automatically when their input gets updated, so
        all we need to do to see updated workflow output is update the input:
        >>> out = wf(first__x=10)
        >>> out
        {'second__y': 12}

        Note: this _looks_ like a dictionary, but has some extra convenience that we
        can dot-access data:
        >>> out.second__y
        12

        We can give more convenient names to IO, and even access IO that would normally
        be hidden (because it's connected) by specifying an `inputs_map` and/or
        `outputs_map`:
        >>> wf.inputs_map = {"first__x": "x"}
        >>> wf.outputs_map = {
        ...     "first__y": "intermediate",
        ...     "second__y": "y"
        ... }
        >>> wf(x=0)
        {'intermediate': 1, 'y': 2}

        Workflows can be visualized in the notebook using graphviz:
        >>> graphviz_graph = wf.draw()

        The resulting object can be saved as an image, e.g.
        >>> wf.draw().render(filename="demo", format="png")
        'demo.png'

        Let's clean up after ourselves (for when the CI runs the docstrings)
        >>> from os import remove
        >>> remove("demo")
        >>> remove("demo.png")

        Workflows also give access to packages of pre-built nodes under different
        namespaces. These need to be registered first, like the standard package is
        automatically registered:
        >>> Workflow.register("standard", "pyiron_workflow.node_library.standard")

        When your workflow's data follows a directed-acyclic pattern, it will determine
        the execution flow automatically.
        If you want or need more control, you can set the `automate_execution` flag to
        `False` and manually specify an execution flow.

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

    def __init__(
        self,
        label: str,
        *nodes: Node,
        run_after_init: bool = False,
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
        automate_execution: bool = True,
    ):
        super().__init__(
            label=label,
            parent=None,
            strict_naming=strict_naming,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )
        self.automate_execution = automate_execution

        for node in nodes:
            self.add_node(node)

    def _get_linking_channel(
        self,
        child_reference_channel: InputData | OutputData,
        composite_io_key: str,
    ) -> InputData | OutputData:
        """
        Build IO by reference: just return the child's channel itself.
        """
        return child_reference_channel

    @property
    def inputs(self) -> Inputs:
        return self._build_inputs()

    @property
    def outputs(self) -> Outputs:
        return self._build_outputs()

    def run(
        self,
        check_readiness: bool = True,
        force_local_execution: bool = False,
        **kwargs,
    ):
        # Note: Workflows may have neither parents nor siblings, so we don't need to
        # worry about running their data trees first, fetching their input, nor firing
        # their `ran` signal, hence the change in signature from Node.run
        if self.automate_execution:
            self.set_run_signals_to_dag_execution()
        return super().run(
            run_data_tree=False,
            run_parent_trees_too=False,
            fetch_input=False,
            check_readiness=check_readiness,
            force_local_execution=force_local_execution,
            emit_ran_signal=False,
            **kwargs,
        )

    def pull(self, run_parent_trees_too=False, **kwargs):
        """Workflows are a parent-most object, so this simply runs without pulling."""
        return self.run(**kwargs)

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
    def _parent(self) -> None:
        return None

    @_parent.setter
    def _parent(self, new_parent: None):
        # Currently workflows are not allowed to have a parent -- maybe we want to
        # change our minds on this in the future? If we do, we can just expose `parent`
        # as a kwarg and roll back this private var/property/setter protection and let
        # the super call in init handle everything
        if new_parent is not None:
            raise TypeError(
                f"{self.__class__} may only take None as a parent but got "
                f"{type(new_parent)}"
            )
