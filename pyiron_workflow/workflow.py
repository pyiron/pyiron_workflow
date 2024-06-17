"""
Provides the main workhorse class for creating and running workflows.

This class is intended as the single point of entry for users making an import.
"""

from __future__ import annotations

from typing import Literal, Optional, TYPE_CHECKING

from bidict import bidict

from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.io import Inputs, Outputs
from pyiron_workflow.mixin.semantics import ParentMost


if TYPE_CHECKING:
    from pyiron_workflow.io import IO
    from pyiron_workflow.node import Node


class Workflow(ParentMost, Composite):
    """
    Workflows are a dynamic composite node -- i.e. they hold and run a collection of
    nodes (a subgraph) which can be dynamically modified (adding and removing nodes,
    and modifying their connections).

    Nodes can be added to the workflow at instantiation or with dot-assignment later on.
    They are then accessible either under the :attr:`nodes` dot-dictionary, or just directly
    by dot-access on the workflow object itself.

    Using the :attr:`input` and :attr:`output` attributes, the workflow gives by-reference access
    to all the IO channels among its nodes which are currently unconnected.

    The :class:`Workflow` class acts as a single-point-of-import for us;
    Directly from the class we can use the :meth:`create` method to instantiate workflow
    objects.
    When called from a workflow _instance_, any created nodes get their parent set to
    the workflow instance being used.

    Workflows are "living" -- i.e. their IO is always by reference to their owned nodes
    and you are meant to add and remove nodes as children -- and "parent-most" -- i.e.
    they sit at the top of any data dependency tree and may never have a parent of
    their own.
    They are flexible and great for development, but once you have a setup you like,
    you should consider reformulating it as a :class:`Macro`, which operates somewhat more
    efficiently.

    Promises (in addition parent class promises):

    - Workflows are living, their IO always reflects their current state of child nodes
    - Workflows are parent-most objects, they cannot be a sub-graph of a larger graph

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
        >>> @Workflow.wrap.as_function_node()
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

        Workflows also give access to packages of pre-built nodes under different
        namespaces. These need to be registered first, like the standard package is
        automatically registered:

        >>> Workflow.register("pyiron_workflow.nodes.standard", "standard")

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
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
        automate_execution: bool = True,
        **kwargs,
    ):
        self._inputs_map = None
        self._outputs_map = None
        self.inputs_map = inputs_map
        self.outputs_map = outputs_map
        self._inputs = None
        self._outputs = None
        self.automate_execution = automate_execution

        super().__init__(
            *nodes,
            label=label,
            parent=None,
            overwrite_save=overwrite_save,
            run_after_init=run_after_init,
            save_after_run=save_after_run,
            storage_backend=storage_backend,
            strict_naming=strict_naming,
            **kwargs,
        )

    def _after_node_setup(
        self,
        *args,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        **kwargs,
    ):

        for node in args:
            self.add_child(node)
        super()._after_node_setup(
            overwrite_save=overwrite_save, run_after_init=run_after_init, **kwargs
        )

    @property
    def inputs_map(self) -> bidict | None:
        self._deduplicate_nones(self._inputs_map)
        return self._inputs_map

    @inputs_map.setter
    def inputs_map(self, new_map: dict | bidict | None):
        self._deduplicate_nones(new_map)
        if new_map is not None:
            new_map = bidict(new_map)
        self._inputs_map = new_map

    @property
    def outputs_map(self) -> bidict | None:
        self._deduplicate_nones(self._outputs_map)
        return self._outputs_map

    @outputs_map.setter
    def outputs_map(self, new_map: dict | bidict | None):
        self._deduplicate_nones(new_map)
        if new_map is not None:
            new_map = bidict(new_map)
        self._outputs_map = new_map

    @staticmethod
    def _deduplicate_nones(some_map: dict | bidict | None) -> dict | bidict | None:
        if some_map is not None:
            for k, v in some_map.items():
                if v is None:
                    some_map[k] = (None, f"{k} disabled")

    @property
    def inputs(self) -> Inputs:
        return self._build_inputs()

    def _build_inputs(self):
        return self._build_io("inputs", self.inputs_map)

    @property
    def outputs(self) -> Outputs:
        return self._build_outputs()

    def _build_outputs(self):
        return self._build_io("outputs", self.outputs_map)

    def _build_io(
        self,
        i_or_o: Literal["inputs", "outputs"],
        key_map: dict[str, str | None] | None,
    ) -> Inputs | Outputs:
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
            (Inputs|Outputs): The populated panel.
        """
        key_map = {} if key_map is None else key_map
        io = Inputs() if i_or_o == "inputs" else Outputs()
        for node in self.children.values():
            panel = getattr(node, i_or_o)
            for channel in panel:
                try:
                    io_panel_key = key_map[channel.scoped_label]
                    if not isinstance(io_panel_key, tuple):
                        # Tuples indicate that the channel has been deactivated
                        # This is a necessary misdirection to keep the bidict working,
                        # as we can't simply map _multiple_ keys to `None`
                        io[io_panel_key] = channel
                except KeyError:
                    if not channel.connected:
                        io[channel.scoped_label] = channel
        return io

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

    def to_storage(self, storage):
        storage["package_requirements"] = list(self.package_requirements)
        storage["automate_execution"] = self.automate_execution
        storage["inputs_map"] = self.inputs_map
        storage["outputs_map"] = self.outputs_map
        super().to_storage(storage)

        storage["_data_connections"] = self._data_connections

        if not self.automate_execution:
            storage["_signal_connections"] = self._signal_connections
            storage["starting_nodes"] = [n.label for n in self.starting_nodes]

    def from_storage(self, storage):
        from pyiron_contrib.tinybase.storage import GenericStorage

        self.inputs_map = (
            storage["inputs_map"].to_object()
            if isinstance(storage["inputs_map"], GenericStorage)
            else storage["inputs_map"]
        )
        self.outputs_map = (
            storage["outputs_map"].to_object()
            if isinstance(storage["outputs_map"], GenericStorage)
            else storage["outputs_map"]
        )

        self._reinstantiate_children(storage)
        self.automate_execution = storage["automate_execution"]
        super().from_storage(storage)
        self._rebuild_data_io()  # To apply any map that was saved
        self._rebuild_connections(storage)

    def _reinstantiate_children(self, storage):
        # Parents attempt to reload their data on instantiation,
        # so there is no need to explicitly load any of these children
        for package_identifier in storage["package_requirements"]:
            self.register(package_identifier)

        for child_label in storage["nodes"]:
            child_data = storage[child_label]
            pid = child_data["package_identifier"]
            cls = child_data["class_name"]
            self.create[pid][cls](
                label=child_label, parent=self, storage_backend="tinybase"
            )

    def _rebuild_connections(self, storage):
        self._rebuild_data_connections(storage)
        if not self.automate_execution:
            self._rebuild_execution_graph(storage)

    def _rebuild_data_connections(self, storage):
        for data_connection in storage["_data_connections"]:
            (inp_label, inp_channel), (out_label, out_channel) = data_connection
            self.children[inp_label].inputs[inp_channel].connect(
                self.children[out_label].outputs[out_channel]
            )

    def _rebuild_execution_graph(self, storage):
        for signal_connection in storage["_signal_connections"]:
            (inp_label, inp_channel), (out_label, out_channel) = signal_connection
            self.children[inp_label].signals.input[inp_channel].connect(
                self.children[out_label].signals.output[out_channel]
            )
        self.starting_nodes = [
            self.children[label] for label in storage["starting_nodes"]
        ]

    def __getstate__(self):
        state = super().__getstate__()

        # Transform the IO maps into a datatype that plays well with h5io
        # (Bidict implements a custom reconstructor, which hurts us)
        state["_inputs_map"] = (
            None if self._inputs_map is None else dict(self._inputs_map)
        )
        state["_outputs_map"] = (
            None if self._outputs_map is None else dict(self._outputs_map)
        )

        return state

    def __setstate__(self, state):
        # Transform the IO maps back into the right class (bidict)
        state["_inputs_map"] = (
            None if state["_inputs_map"] is None else bidict(state["_inputs_map"])
        )
        state["_outputs_map"] = (
            None if state["_outputs_map"] is None else bidict(state["_outputs_map"])
        )

        super().__setstate__(state)

    def save(self):
        if self.storage_backend == "tinybase" and any(
            node.package_identifier is None for node in self
        ):
            raise NotImplementedError(
                f"{self.full_label} ({self.__class__.__name__}) can currently only "
                f"save itself to file if _all_ of its child nodes were created via the "
                f"creator and have an associated `package_identifier` -- otherwise we "
                f"won't know how to re-instantiate them at load time! Right now this "
                f"is as easy as moving your custom nodes to their own .py file and "
                f"registering it like any other node package. Remember that this new "
                f"module needs to be in your python path and importable at load time "
                f"too."
            )
        super().save()

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
    ) -> Node:
        super().replace_child(owned_node=owned_node, replacement=replacement)

        # Finally, make sure the IO is constructible with this new node, which will
        # catch things like incompatible IO maps
        try:
            # Make sure node-level IO is pointing to the new node and that macro-level
            # IO gets safely reconstructed
            self._rebuild_data_io()
        except Exception as e:
            # If IO can't be successfully rebuilt using this node, revert changes and
            # raise the exception
            self.replace_child(replacement, owned_node)  # Guaranteed to work since
            # replacement in the other direction was already a success
            raise e

        return owned_node
