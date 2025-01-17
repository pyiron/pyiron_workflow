"""
Provides the main workhorse class for creating and running workflows.

This class is intended as the single point of entry for users making an import.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from bidict import bidict

from pyiron_workflow.io import Inputs
from pyiron_workflow.mixin.injection import OutputsWithInjection
from pyiron_workflow.nodes.composite import Composite

if TYPE_CHECKING:
    from pyiron_workflow.io import IO
    from pyiron_workflow.node import Node
    from pyiron_workflow.storage import StorageInterface


class ParentMostError(TypeError):
    """
    To be raised when assigning a parent to a parent-most object
    """


class NoArgsError(TypeError):
    """
    To be raised when *args can't be processed but are received
    """


class Workflow(Composite):
    """
    Workflows are a dynamic composite node -- i.e. they hold and run a collection of
    nodes (a subgraph) which can be dynamically modified (adding and removing nodes,
    and modifying their connections).

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the :attr:`nodes` dot-dictionary, or just
    directly by dot-access on the workflow object itself.

    Using the :attr:`input` and :attr:`output` attributes, the workflow gives
    by-reference access to all the IO channels among its nodes which are currently
    unconnected.

    The :class:`Workflow` class acts as a single-point-of-import for us;
    Directly from the class we can use the :meth:`create` method to instantiate
    workflow objects.
    When called from a workflow _instance_, any created nodes get their parent set to
    the workflow instance being used.

    Workflows are "living" -- i.e. their IO is always by reference to their owned nodes
    and you are meant to add and remove nodes as children -- and "parent-most" -- i.e.
    they sit at the top of any data dependency tree and may never have a parent of
    their own.
    They are flexible and great for development, but once you have a setup you like,
    you should consider reformulating it as a :class:`Macro`, which operates somewhat
    more efficiently.

    Because they are parent-most objects, and thus not being instantiated inside other
    (macro) nodes, they break the default behaviour of their parent class and _do_
    attempt to auto-load saved content at instantiation.

    Promises (in addition parent class promises):

    - Workflows are living, their IO always reflects their current state of child nodes
    - Workflows are parent-most objects, they cannot be a sub-graph of a larger graph
    - Bijective maps can be used to...
        - Rename IO
        - Force a child node's IO to appear
        - Force a child node's IO to _not_ appear

    Attribute:
        inputs/outputs_map (bidict|None): Maps in the form
        `{"node_label__channel_label": "some_better_name"}` that expose canonically
         named channels of child nodes under a new name. This can be used both for re-
         naming regular IO (i.e. unconnected child channels), as well as forcing the
         exposure of irregular IO (i.e. child channels that are already internally
         connected to some other child channel). Non-`None` values provided at input
         can be in regular dictionary form, but get re-cast as a clean bidict to ensure
         the bijective nature of the maps (i.e. there is a 1:1 connection between any
         IO exposed at the :class:`Composite` level and the underlying channels).
        children (bidict.bidict[pyiron_workflow.node.Node]): The owned nodes that
         form the composite subgraph.

    Examples:
        We allow adding nodes to workflows in five equivalent ways:

        >>> from pyiron_workflow.workflow import Workflow
        >>>
        >>> @Workflow.wrap.as_function_node
        ... def fnc(x=0):
        ...     return x + 1
        >>>
        >>> # (1) As *args at instantiation
        >>> n1 = fnc(label="n1")
        >>> wf = Workflow("my_workflow", n1)
        >>>
        >>> # (2) Being passed to the `add` method
        >>> n2 = wf.add_child(fnc(label="n2"))
        >>>
        >>> # (3) By attribute assignment
        >>> wf.n3 = fnc(label="anyhow_n3_gets_used")
        >>>
        >>> # (4) By creating from the workflow class but specifying the parent kwarg
        >>> n4 = fnc(label="n4", parent=wf)

        By default, the node naming scheme is strict, so if you try to add a node to a
        label that already exists, you will get an error. This behaviour can be changed
        at instantiation with the :attr:`strict_naming` kwarg, or afterwards by assigning a
        bool to this property. When deactivated, repeated assignments to the same label
        just get appended with an index:

        >>> wf.strict_naming = False
        >>> wf.my_node = fnc(x=0)
        >>> wf.my_node = fnc(x=1)
        >>> wf.my_node = fnc(x=2)
        >>> print(wf.my_node.inputs.x, wf.my_node0.inputs.x, wf.my_node1.inputs.x)
        0 1 2

        The :class:`Workflow` class is designed as a single point of entry for workflows, so
        you can also access decorators to define new node classes right from the
        workflow (cf. the :class:`Node` docs for more detail on the node types).
        Let's use these to explore a workflow's input and output, which are dynamically
        generated from the unconnected IO of its nodes:

        >>> @Workflow.wrap.as_function_node("y")
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
        be hidden (because it's connected) by specifying an :attr:`inputs_map` and/or
        :attr:`outputs_map`:

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

        When your workflow's data follows a directed-acyclic pattern, it will determine
        the execution flow automatically.
        If you want or need more control, you can set the `automate_execution` flag to
        `False` and manually specify an execution flow.

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
        delete_existing_savefiles: bool = False,
        autoload: Literal["pickle"] | StorageInterface | None = "pickle",
        autorun: bool = False,
        checkpoint: Literal["pickle"] | StorageInterface | None = None,
        strict_naming: bool = True,
        inputs_map: dict | bidict | None = None,
        outputs_map: dict | bidict | None = None,
        automate_execution: bool = True,
        **kwargs,
    ):
        self._inputs_map = self._sanitize_map(inputs_map)
        self._outputs_map = self._sanitize_map(outputs_map)
        self._inputs = None
        self._outputs = None
        self.automate_execution: bool = automate_execution

        super().__init__(
            *nodes,
            label=label,
            parent=None,
            delete_existing_savefiles=delete_existing_savefiles,
            autoload=autoload,
            autorun=autorun,
            checkpoint=checkpoint,
            strict_naming=strict_naming,
            **kwargs,
        )

    def _after_node_setup(
        self,
        *args,
        delete_existing_savefiles: bool = False,
        autoload: Literal["pickle"] | StorageInterface | None = None,
        autorun: bool = False,
        **kwargs,
    ):
        for node in args:
            self.add_child(node)
        super()._after_node_setup(
            autoload=autoload,
            delete_existing_savefiles=delete_existing_savefiles,
            autorun=autorun,
            **kwargs,
        )

    @property
    def inputs_map(self) -> bidict | None:
        if self._inputs_map is not None:
            self._deduplicate_nones(self._inputs_map)
        return self._inputs_map

    @inputs_map.setter
    def inputs_map(self, new_map: dict | bidict | None):
        self._inputs_map = self._sanitize_map(new_map)

    @property
    def outputs_map(self) -> bidict | None:
        if self._outputs_map is not None:
            self._deduplicate_nones(self._outputs_map)
        return self._outputs_map

    @outputs_map.setter
    def outputs_map(self, new_map: dict | bidict | None):
        self._outputs_map = self._sanitize_map(new_map)

    def _sanitize_map(self, new_map: dict | bidict | None) -> bidict | None:
        if new_map is not None:
            if isinstance(new_map, dict):
                self._deduplicate_nones(new_map)
            new_map = bidict(new_map)
        return new_map

    @staticmethod
    def _deduplicate_nones(some_map: dict | bidict):
        for k, v in some_map.items():
            if v is None:
                some_map[k] = (None, f"{k} disabled")

    @property
    def inputs(self) -> Inputs:
        return self._build_inputs()

    def _build_inputs(self):
        return self._build_io("inputs", self.inputs_map)

    @property
    def outputs(self) -> OutputsWithInjection:
        return self._build_outputs()

    def _build_outputs(self):
        return self._build_io("outputs", self.outputs_map)

    def _build_io(
        self,
        i_or_o: Literal["inputs", "outputs"],
        key_map: dict[str, str | None] | None,
    ) -> Inputs | OutputsWithInjection:
        """
        Build an IO panel for exposing child node IO to the outside world at the level
        of the composite node's IO.

        Args:
            target [Literal["inputs", "outputs"]]: Whether this is I or O.
            key_map [dict[str, str]|None]: A map between the default convention for
                mapping child IO to composite IO (`"{node.label}__{channel.label}"`) and
                whatever label you actually want to expose to the composite user. Also
                allows non-standards channel exposure, i.e. exposing
                internally-connected channels (which would not normally be exposed) by
                providing a string-to-string map, or suppressing unconnected channels
                (which normally would be exposed) by providing a string-None map.

        Returns:
            (Inputs|OutputsWithInjection): The populated panel.
        """
        key_map = {} if key_map is None else key_map
        io = Inputs() if i_or_o == "inputs" else OutputsWithInjection()
        for node in self.children.values():
            panel = getattr(node, i_or_o)
            for channel in panel:
                try:
                    io_panel_key = key_map[channel.scoped_label]
                    if isinstance(io_panel_key, str):
                        # Otherwise it's a None-str tuple, indicaticating that the
                        # channel has been deactivated
                        # This is a necessary misdirection to keep the bidict working,
                        # as we can't simply map _multiple_ keys to `None`
                        io[io_panel_key] = channel
                except KeyError:
                    if not channel.connected:
                        io[channel.scoped_label] = channel
        return io

    def _before_run(
        self,
        /,
        check_readiness: bool,
        run_data_tree: bool,
        run_parent_trees_too: bool,
        fetch_input: bool,
        emit_ran_signal: bool,
    ) -> tuple[bool, Any]:
        if self.automate_execution:
            self.set_run_signals_to_dag_execution()
        return super()._before_run(
            check_readiness=check_readiness,
            run_data_tree=run_data_tree,
            run_parent_trees_too=run_parent_trees_too,
            fetch_input=fetch_input,
            emit_ran_signal=emit_ran_signal,
        )

    def run(
        self,
        *args,
        check_readiness: bool = True,
        **kwargs,
    ):
        # Note: Workflows may have neither parents nor siblings, so we don't need to
        # worry about running their data trees first, fetching their input, nor firing
        # their `ran` signal, hence the change in signature from Node.run
        if len(args) > 0:
            raise NoArgsError(
                f"{self.__class__} does not know how to process *args on run, but "
                f"received {args}"
            )

        return super().run(
            run_data_tree=False,
            run_parent_trees_too=False,
            fetch_input=False,
            check_readiness=check_readiness,
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

    @property
    def _data_connections(self) -> list[tuple[tuple[str, str], tuple[str, str]]]:
        """
        A string-tuple representation of all connections between the data channels of
        child nodes.

        Intended for internal use during storage, so that connections can be
        represented in plain strings, and stored on an attribute to guarantee that the
        name does not conflict with a child node label.

        Returns:
            (list): Nested-pair tuples of (node label, channel label) data for
                (input, output) channels of data connections between children.
        """
        data_connections = []
        for node in self:
            for inp_label, inp in node.inputs.items():
                for conn in inp.connections:
                    data_connections.append(
                        ((node.label, inp_label), (conn.owner.label, conn.label))
                    )
        return data_connections

    @property
    def _signal_connections(self) -> list[tuple[tuple[str, str], tuple[str, str]]]:
        """
        A string-tuple representation of all connections between the signal channels of
        child nodes.

        Intended for internal use during storage, so that connections can be
        represented in plain strings, and stored on an attribute to guarantee that the
        name does not conflict

        Returns:
            (list): Nested-pair tuples of (node label, channel label) data for
                (input, output) channels of signal connections between children.
        """
        signal_connections = []
        for node in self:
            for inp_label, inp in node.signals.input.items():
                for conn in inp.connections:
                    signal_connections.append(
                        ((node.label, inp_label), (conn.owner.label, conn.label))
                    )
        return signal_connections

    def _rebuild_data_io(self):
        """
        Try to rebuild the IO.

        If an error is encountered, revert back to the existing IO then raise it.
        """
        old_inputs = self.inputs
        old_outputs = self.outputs
        connection_changes = []  # For reversion if there's an error
        try:
            self._inputs = self._build_inputs()
            self._outputs = self._build_outputs()
            for old, new in [(old_inputs, self.inputs), (old_outputs, self.outputs)]:
                for old_channel in old:
                    if old_channel.connected:
                        # If the old channel was connected to stuff, we'd better still
                        # have a corresponding channel and be able to copy these, or we
                        # should fail hard.
                        # But, if it wasn't connected, we don't even care whether or not
                        # we still have a corresponding channel to copy to
                        new_channel = new[old_channel.label]
                        new_channel.copy_connections(old_channel)
                        swapped_conenctions = old_channel.disconnect_all()  # Purge old
                        connection_changes.append(
                            (new_channel, old_channel, swapped_conenctions)
                        )
        except Exception as e:
            for new_channel, old_channel, swapped_conenctions in connection_changes:
                new_channel.disconnect(*swapped_conenctions)
                old_channel.connect(*swapped_conenctions)
            self._inputs = old_inputs
            self._outputs = old_outputs
            e.message = (
                f"Unable to rebuild IO for {self.full_label}; reverting to old IO."
                f"{e.message}"
            )
            raise e

    @property
    def _owned_io_panels(self) -> list[IO]:
        # Workflow data IO is just pointers to child IO, not actually owned directly
        # by the workflow; this is used in re-parenting channels, and we don't want to
        # override the real parent with this workflow!
        return [
            self.signals.input,
            self.signals.output,
        ]

    def replace_child(
        self, owned_node: Node | str, replacement: Node | type[Node]
    ) -> tuple[Node, Node]:
        replaced, replacement_node = super().replace_child(
            owned_node=owned_node, replacement=replacement
        )

        # Finally, make sure the IO is constructible with this new node, which will
        # catch things like incompatible IO maps
        try:
            # Make sure node-level IO is pointing to the new node and that macro-level
            # IO gets safely reconstructed
            self._rebuild_data_io()
        except Exception as e:
            # If IO can't be successfully rebuilt using this node, revert changes and
            # raise the exception
            self.replace_child(replacement_node, replaced)  # Guaranteed to work since
            # replacement in the other direction was already a success
            raise e

        return replaced, replacement_node

    @property
    def parent(self) -> None:
        return None

    @parent.setter
    def parent(self, new_parent: None):
        if new_parent is not None:
            raise ParentMostError(
                f"{self.label} is a {self.__class__} and may only take None as a "
                f"parent but got {type(new_parent)}"
            )
