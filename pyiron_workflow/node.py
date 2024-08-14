"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.

The workhorse class for the entire concept.
"""

from __future__ import annotations

from abc import ABC
from concurrent.futures import Future
from importlib import import_module
from typing import Any, Literal, Optional, TYPE_CHECKING

from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow.draw import Node as GraphvizNode
from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.has_to_dict import HasToDict
from pyiron_workflow.mixin.injection import HasIOWithInjection
from pyiron_workflow.mixin.run import Runnable, ReadinessError
from pyiron_workflow.mixin.semantics import Semantic
from pyiron_workflow.mixin.single_output import ExploitsSingleOutput
from pyiron_workflow.storage import StorageInterface, available_backends
from pyiron_workflow.topology import (
    get_nodes_in_data_tree,
    set_run_connections_according_to_linear_dag,
)
from pyiron_workflow.mixin.working import HasWorkingDirectory

if TYPE_CHECKING:
    from pathlib import Path

    import graphviz

    from pyiron_workflow.channels import OutputSignal
    from pyiron_workflow.nodes.composite import Composite


class Node(
    HasToDict,
    Semantic,
    Runnable,
    HasIOWithInjection,
    ExploitsSingleOutput,
    HasWorkingDirectory,
    ABC,
):
    """
    Nodes are elements of a computational graph.
    They have inputs and outputs to interface with the wider world, and perform some
    operation.
    By connecting multiple nodes' inputs and outputs together, computational graphs can
    be formed.
    These can be collected under a parent, such that new graphs can be composed of
    one or more sub-graphs.

    Promises:

    - Nodes perform some computation, but this is delayed and won't happen until asked
        for (the nature of the computation is left to child classes).
    - Nodes have input and output for interfacing with the outside world
        - Which can be connected to output/input to form a computation graph
        - These have a data flavour, to control the flow of information
        - And a signal flavour, to control the flow of execution
            - Execution flows can be specified manually, but in the case of data flows
                which form directed acyclic graphs (DAGs), this can be automated
            - Running can be triggered in an instantaneous (i.e. "or" applied to
                incoming signals) or accumulating way (i.e. "and" applied to incoming
                signals).
        - If the node has exactly one output channel, most standard python operations
            (attribute access, math, etc.) will fall back on attempting the same
            operation on this single output, if the operation failed on the node.
            Practically, that means that such "single-output" nodes get the same
            to form IO connections and inject new nodes that output channels have.
    - When running their computation, nodes may or may not:
        - First update their input data values using kwargs
            - (Note that since this happens first, if the "fetching" step later occurs,
                any values provided here will get overwritten by data that is flowing
                on the data graph)
        - Then instruct their parent node to ask all of the nodes
            upstream in its data connections to run (recursively to the parent-most
            super-graph)
        - Ask for the nodes upstream of them to run (in the local context of their own
            parent)
        - Fetch the latest output data, prioritizing the first actual data among their
            each of their inputs connections
        - Check if they are ready to run, i.e.
            - Status is neither running nor failed
            - Input is all ready, i.e. each input has data and that data is
                commensurate with type hints (if any)
        - Submit their computation to an executor for remote processing, or ignore any
            executor suggested and force the computation to be local (i.e. in the same
            python process that owns the node)
            - If computation is non-local, the node status will stay running and the
                futures object returned by the executor will be accessible
        - Emit their run-completed output signal to trigger runs in nodes downstream in
            the execution flow
    - Running the node (and all aliases of running) return a representation of data
        held by the output channels
    - If an error is encountered _after_ reaching the state of actually computing the
        node's task, the status will get set to failure
    - Nodes can be instructed to run at the end of their initialization, but will exit
        cleanly if they get to checking their readiness and find they are not ready
    - Nodes have a label by which they are identified
    - Nodes may open a working directory related to their label, their parent(age) and
        the python process working directory
    - Nodes can run their computation using remote resources by setting an executor
        - Any executor must have a :meth:`submit` method with the same interface as
            :class:`concurrent.futures.Executor`, must return a
            :class:`concurrent.futures.Future` (or child thereof) object.
        - Standard available nodes are pickleable and work with
            `concurrent.futures.ProcessPoolExecutor`, but if you define your node
            somewhere that it can't be imported (e.g. `__main__` in a jupyter
            notebook), or it is otherwise not pickleable (e.g. it holds un-pickleable
            io data), you will need a more powerful executor, e.g.
            `executorlib.Executor`.
        - On executing this way, a futures object will be returned instead of the usual
            result, this future will also be stored as an attribute, and a callback will
            be registered with the executor
        - Post-execution processing -- e.g. updating output and firing signals -- will
            not occur until the futures object is finished and the callback fires.
        - NOTE: Executors are only allowed in a "push" paradigm, and you will get an
            exception if you try to :meth:`pull` and one of the upstream nodes uses an
            executor
        - NOTE: Don't forget to :meth:`shutdown` any created executors outside of a
            `with` context when you're done with them; we give a convenience method for
            this.
    - Nodes can optionally cache their input to skip running altogether and use
        existing output when their current input matches the cached input.
    - [ALPHA FEATURE] Nodes can be saved to and loaded from file if python >= 3.11.
        - Save and load features wrap `(cloud)pickle` dumping and loading.
        - Everything in `pyiron_workflow` itself is (if not, alert developers),
            pickle-able (by exploiting `pyiron_snippets.factory`), but `pickle` will
            fall back to `cloudpickle` if trouble is encountered, e.g. because some
            IO data is not pickle-able.
        - Saving is triggered manually, or by setting a flag to save after the nodes
            runs.
        - At the end of instantiation, nodes can load automatically if they find saved
            content (default on the base class is to _not_ search for this. One good
            reason is that new macros might instantiate a child node that _happens_ to
            share a name with a saved node, but the intent is obviously not to load
            this.)
          - Discovered content can instead be deleted with a kwarg.
          - You can't load saved content _and_ run after instantiation at once.
        - The nodes must be defined somewhere importable, i.e. in a module, `__main__`,
            and as a class property are all fine, but, e.g., inside the `<locals>` of
            another function is not. If you did put them in `__main__`, make sure you
            re-execute that content before trying to load or the load will (sensibly)
            fail!
        - [ALPHA ISSUE] If the source code (cells, `.py` files...) for a saved graph is
            altered between saving and loading the graph, there are no guarantees about
            the loaded state; depending on the nature of the changes everything may
            work fine with the new node definition, the graph may load but silently
            behave unexpectedly (e.g. if node functionality has changed but the
            interface is the same), or may crash on loading (e.g. if IO channel labels
            have changed).
        - [ALPHA ISSUE] There is no filtering available, saving a node stores all of
            its IO and does the same thing recursively for its children; depending on
            your graph this could be expensive in terms of storage space and/or time.
        - [ALPHA ISSUE] Similarly, there is no way to save only part of a graph; only
            the entire graph may be saved at once.

    This is an abstract class.
    Children *must* define how :attr:`inputs` and :attr:`outputs` are constructed, what will
    happen :meth:`on_run`, the :attr:`run_args` that will get passed to :meth:`on_run`, and how to
    :meth:`process_run_result` once :meth:`on_run` finishes.
    They may optionally add additional signal channels to the signals IO.

    TODO:

        - Allow saving/loading at locations _other_ than the interpreter's working
            directory combined with the node's working directory, i.e. decouple the
            working directory from the interpreter's `cwd`.
        - Integration with more powerful tools for remote execution (anything obeying
            the standard interface of a :meth:`submit` method taking the callable and
            arguments and returning a futures object should work, as long as it can
            handle serializing dynamically defined objects.

    Attributes:
        connected (bool): Whether _any_ of the IO (including signals) are connected.
        failed (bool): Whether the node raised an error calling :meth:`run`. (Default
            is False.)
        fully_connected (bool): whether _all_ of the IO (including signals) are
            connected.
        future (concurrent.futures.Future | None): A futures object, if the node is
            currently running or has already run using an executor.
        import_ready (bool): Whether importing the node's class from its class's module
            returns the same thing as its type. (Recursive on sub-nodes for composites.)
        inputs (pyiron_workflow.io.Inputs): **Abstract.** Children must define
            a property returning an :class:`Inputs` object.
        label (str): A name for the node.
        outputs (pyiron_workflow.io.Outputs): **Abstract.** Children must define
            a property returning an :class:`Outputs` object.
        parent (pyiron_workflow.composite.Composite | None): The parent object
            owning this, if any.
        ready (bool): Whether the inputs are all ready and the node is neither
            already running nor already failed.
        graph_path (str): The file-path-like path of node labels from the parent-most
            node down to this node.
        graph_root (Node): The parent-most node in this graph.
        run_args (dict): **Abstract** the argmuments to use for actually running the
            node. Must be specified in child classes.
        running (bool): Whether the node has called :meth:`run` and has not yet
            received output from this call. (Default is False.)
        checkpoint (Literal["pickle"] | StorageInterface | None): Whether to trigger a
            save of the entire graph after each run of the node, and if so what storage
            back end to use. (Default is None, don't do any checkpoint saving.)
        autoload (Literal["pickle"] | StorageInterface | None): Whether to check
            for a matching saved node and what storage back end to use to do so (no
            auto-loading if the back end is `None`.)
        signals (pyiron_workflow.io.Signals): A container for input and output
            signals, which are channels for controlling execution flow. By default, has
            a :attr:`signals.inputs.run` channel which has a callback to the :meth:`run` method
            that fires whenever _any_ of its connections sends a signal to it, a
            :attr:`signals.inputs.accumulate_and_run` channel which has a callback to the
            :meth:`run` method but only fires after _all_ its connections send at least one
            signal to it, and `signals.outputs.ran` which gets called when the `run`
            method is finished.
            Additional signal channels in derived classes can be added to
            :attr:`signals.inputs` and  :attr:`signals.outputs` after this mixin class is
            initialized.
        use_cache (bool): Whether or not to cache the inputs and, when the current
            inputs match the cached input (by `==` comparison), to bypass running the
            node and simply continue using the existing outputs. Note that you may be
            able to trigger a false cache hit in some special case of non-idempotent
            nodes working on mutable data.

    Methods:
        __call__: An alias for :meth:`pull` that aggressively runs upstream nodes even
            _outside_ the local scope (i.e. runs parents' dependencies as well).
        (de)activate_strict_hints: Recursively (de)activate strict hints among data IO.
        disconnect: Remove all connections, including signals.
        draw: Use graphviz to visualize the node, its IO and, if composite in nature,
            its internal structure.
        execute: An alias for :meth:`run`, but with flags to run right here, right now, and
            with the input it currently has.
        on_run: **Abstract.** Do the thing. What thing must be specified by child
            classes.
        pull: An alias for :meth:`run` that runs everything upstream, then runs this node
            (but doesn't fire off the `ran` signal, so nothing happens farther
            downstream). "Upstream" may optionally break out of the local scope to run
            parent nodes' dependencies as well (all the way until the parent-most
            object is encountered).
        replace_with: If the node belongs to a parent, attempts to replace itself in
            that parent with a new provided node.
        run: Run the node function from :meth:`on_run`. Handles status automatically. Various
            execution options are available as boolean flags.
        set_input_values: Allows input channels' values to be updated without any
            running.

    Note:
        :meth:`__init__` ends with a routine :meth:`_after_node_setup` that may,
        depending on instantiation arguments, try to actually execute the node. Since
        child classes may need to get things done before this point, we want to make
        sure that this happens _after_ all the other setup. This can be accomplished
        by children (a) sticking stuff that is independent of `super().__init__` calls
        before the super call, and (b) overriding :meth:`_setup_node(self)` to do any
        remaining, parameter-free setup. This latter function gets called prior to any
        execution.

        Initialization will also try to parse any outstanding `args` and `kwargs` as
        input to the node's input channels. For node class developers, that means it's
        also important that `Node` parentage appear to the right-most of the
        inheritance set in the class definition, so that it's invokation of `__init__`
        appears as late as possible with the minimal set of args and kwargs.
    """

    use_cache = True

    # This isn't nice, just a technical necessity in the current implementation
    # Eventually, of course, this needs to be _at least_ file-format independent

    def __init__(
        self,
        *args,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        delete_existing_savefiles: bool = False,
        autoload: Literal["pickle"] | StorageInterface | None = None,
        autorun: bool = False,
        checkpoint: Literal["pickle"] | StorageInterface | None = None,
        **kwargs,
    ):
        """
        A parent class for objects that can form nodes in the graph representation of a
        computational workflow.

        Args:
            label (str): A name for this node.
            *args: Interpreted as node input data, in order of input channels.
            parent: (Composite|None): The composite node that owns this as a child.
            delete_existing_savefiles (bool): Whether to look for and delete any
                matching save files at instantiation. Uses all default storage
                back ends and anything passed to :param:`autoload`. (Default is False,
                leave those files alone!)
            autoload (Literal["pickle"] | StorageInterface | None): The back end
                to use for checking whether node data can be loaded from file. A None
                value indicates no auto-loading. (Default is "pickle".)
            autorun (bool): Whether to run at the end of initialization.
            checkpoint (Literal["pickle"] | StorageInterface | None): The storage
                back end to use for saving the overall graph at the end of this node's
                run. (Default is None, don't do checkpoint saves.)
            **kwargs: Interpreted as node input data, with keys corresponding to
                channel labels.
        """
        super().__init__(
            label=self.__class__.__name__ if label is None else label,
            parent=parent,
        )
        self.checkpoint = checkpoint
        self.cached_inputs = None
        self._user_data = {}  # A place for power-users to bypass node-injection

        self._setup_node()
        self._after_node_setup(
            *args,
            delete_existing_savefiles=delete_existing_savefiles,
            autoload=autoload,
            autorun=autorun,
            **kwargs,
        )

    def _setup_node(self) -> None:
        """
        Called _before_ :meth:`Node.__init__` finishes.

        Child node classes can use this for any parameter-free node setup that should
        happen _before_ :meth:`Node._after_node_setup` gets called.
        """
        pass

    def _after_node_setup(
        self,
        *args,
        delete_existing_savefiles: bool = False,
        autoload: Literal["pickle"] | StorageInterface | None = None,
        autorun: bool = False,
        **kwargs,
    ):
        if delete_existing_savefiles:
            self.delete_storage(backend=autoload)

        if autoload is not None:
            for backend in available_backends(backend=autoload):
                if backend.has_contents(self):
                    logger.info(
                        f"A saved file was found for the node {self.full_label} -- "
                        f"attempting to load it...(To delete the saved file instead, "
                        f"use `delete_existing_savefiles=True`) "
                    )
                    self.load(backend=autoload)
                    break

        self.set_input_values(*args, **kwargs)

        if autorun:
            try:
                self.run()
            except ReadinessError:
                pass

    @property
    def graph_path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node in this semantic
        path) down to this node.
        """
        prefix = self.parent.semantic_path if isinstance(self.parent, Node) else ""
        return prefix + self.semantic_delimiter + self.label

    @property
    def graph_root(self) -> Node:
        """The parent-most node in this semantic path."""
        return self.parent.graph_root if isinstance(self.parent, Node) else self

    def data_input_locked(self):
        return self.running

    @property
    def readiness_report(self) -> str:
        input_readiness_report = f"INPUTS:\n" + "\n".join(
            [f"{k} ready: {v.ready}" for k, v in self.inputs.items()]
        )
        return super().readiness_report + input_readiness_report

    @property
    def _readiness_error_message(self) -> str:
        return (
            f"{self.label} received a run command but is not ready. The node "
            f"should be neither running nor failed, and all input values should"
            f" conform to type hints.\n" + self.readiness_report
        )

    def run(
        self,
        *args,
        run_data_tree: bool = False,
        run_parent_trees_too: bool = False,
        fetch_input: bool = True,
        check_readiness: bool = True,
        force_local_execution: bool = False,
        emit_ran_signal: bool = True,
        **kwargs,
    ):
        """
        The master method for running in a variety of ways.
        By default, whatever data is currently available in upstream nodes will be
        fetched, if the input all conforms to type hints then this node will be run
        (perhaps using an executor), and  finally the `ran` signal will be emitted to
        trigger downstream runs.

        If executor information is specified, execution happens on that process, a
        callback is registered, and futures object is returned.

        Input values can be updated at call time with kwargs, but this happens _first_
        so any input updates that happen as a result of the computation graph will
        override these by default. If you really want to execute the node with a
        particular set of input, set it all manually and use `execute` (or `run` with
        carefully chosen flags).

        Args:
            run_data_tree (bool): Whether to first run all upstream nodes in the data
                graph. (Default is False.)
            run_parent_trees_too (bool): Whether to recursively run the data tree in
                parent nodes (if any). (Default is False.)
            fetch_input (bool): Whether to first update inputs with the
                highest-priority connections holding data (i.e. the first valid
                connection; and the most recently formed connections appear first
                unless the connections list has been manually tampered with). (Default
                is True.)
            check_readiness (bool): Whether to raise an exception if the node is not
                :attr:`ready` to run after fetching new input. (Default is True.)
            force_local_execution (bool): Whether to ignore any executor settings and
                force the computation to run locally. (Default is False.)
            emit_ran_signal (bool): Whether to fire off all the output `ran` signal
                afterwards. (Default is True.)
            **kwargs: Keyword arguments matching input channel labels; used to update
                the input channel values before running anything.

        Returns:
            (Any | Future): The result of running the node, or a futures object (if
                running on an executor).

        Note:
            Running data trees is a pull-based paradigm and only compatible with graphs
            whose data forms a directed acyclic graph (DAG).

        Note:
            Kwargs updating input channel values happens _first_ and will get
            overwritten by any subsequent graph-based data manipulation.
        """
        self.set_input_values(*args, **kwargs)

        if run_data_tree:
            self.run_data_tree(run_parent_trees_too=run_parent_trees_too)

        if fetch_input:
            self.inputs.fetch()

        if self.use_cache and self.cache_hit:  # Read and use cache

            if self.parent is None and emit_ran_signal:
                self.emit()
            elif self.parent is not None:
                self.parent.register_child_starting(self)
                self.parent.register_child_finished(self)
                if emit_ran_signal:
                    self.parent.register_child_emitting(self)

            return self._outputs_to_run_return()
        elif self.use_cache:  # Write cache and continue
            self.cached_inputs = self.inputs.to_value_dict()

        if self.parent is not None:
            self.parent.register_child_starting(self)

        return super().run(
            check_readiness=check_readiness,
            force_local_execution=force_local_execution,
            _finished_callback=(
                self._finish_run_and_emit_ran if emit_ran_signal else self._finish_run
            ),
        )

    def run_data_tree(self, run_parent_trees_too=False) -> None:
        """
        Use topological analysis to build a tree of all upstream dependencies and run
        them.

        Args:
            run_parent_trees_too (bool): First, call the same method on this node's
                parent (if one exists), and recursively up the parentage tree. (Default
                is False, only run nodes in this scope, i.e. sharing the same parent.)
        """
        if run_parent_trees_too and self.parent is not None:
            self.parent.run_data_tree(run_parent_trees_too=True)
            self.parent.inputs.fetch()

        label_map = {}
        nodes = {}

        data_tree_nodes = get_nodes_in_data_tree(self)
        for node in data_tree_nodes:
            if node.executor is not None:
                raise ValueError(
                    f"Running the data tree is pull-paradigm action, and is "
                    f"incompatible with using executors. While running "
                    f"{self.full_label}, an executor request was found on "
                    f"{node.full_label}"
                )

        for node in data_tree_nodes:
            modified_label = node.label + str(id(node))
            label_map[modified_label] = node.label
            node.label = modified_label  # Ensure each node has a unique label
            # This is necessary when the nodes do not have a workflow and may thus have
            # arbitrary labels.
            # This is pretty ugly; it would be nice to not depend so heavily on labels.
            # Maybe we could switch a bunch of stuff to rely on the unique ID?
            nodes[modified_label] = node

        try:
            disconnected_pairs, starters = set_run_connections_according_to_linear_dag(
                nodes
            )
            data_tree_starters = list(set(starters).intersection(data_tree_nodes))
        except Exception as e:
            # If the dag setup fails it will repair any connections it breaks before
            # raising the error, but we still need to repair our label changes
            for modified_label, node in nodes.items():
                node.label = label_map[modified_label]
            raise e

        try:
            parent_starting_nodes = (
                self.parent.starting_nodes if self.parent is not None else None
            )  # We need these for state recovery later, even if we crash

            if len(data_tree_starters) == 1 and data_tree_starters[0] is self:
                # If you're the only one in the data tree, there's nothing upstream to
                # run.
                pass
            else:
                for node in set(nodes.values()).difference(data_tree_nodes):
                    # Disconnect any nodes not in the data tree to avoid unnecessary
                    # execution
                    node.signals.disconnect_run()

                self.signals.disconnect_run()
                # Don't let anything upstream trigger _this_ node

                if self.parent is None:
                    for starter in data_tree_starters:
                        starter.run()  # Now push from the top
                else:
                    # Run the special exec connections from above with the parent

                    # Workflow parents will attempt to automate execution on run,
                    # undoing all our careful execution
                    # This heinous hack breaks in and stops that behaviour
                    # I recognize this is dirty, but let's be pragmatic about getting
                    # the features playing together. Workflows and pull are anyhow
                    # already both very annoying on their own...
                    from pyiron_workflow.workflow import Workflow

                    if isinstance(self.parent, Workflow):
                        automated = self.parent.automate_execution
                        self.parent.automate_execution = False

                    self.parent.starting_nodes = data_tree_starters
                    self.parent.run()

                    # And revert our workflow hack
                    if isinstance(self.parent, Workflow):
                        self.parent.automate_execution = automated
        finally:
            # No matter what, restore the original connections and labels afterwards
            for modified_label, node in nodes.items():
                node.label = label_map[modified_label]
                node.signals.disconnect_run()
            for c1, c2 in disconnected_pairs:
                c1.connect(c2)
            if self.parent is not None:
                self.parent.starting_nodes = parent_starting_nodes

    @property
    def cache_hit(self):
        try:
            return self.inputs.to_value_dict() == self.cached_inputs
        except:
            return False

    def _outputs_to_run_return(self):
        return DotDict(self.outputs.to_value_dict())

    def _finish_run(self, run_output: tuple | Future) -> Any | tuple:
        try:
            processed_output = super()._finish_run(run_output=run_output)
            if self.parent is not None:
                self.parent.register_child_finished(self)
            return processed_output
        finally:
            if self.checkpoint is not None:
                self.save_checkpoint(self.checkpoint)

    def _finish_run_and_emit_ran(self, run_output: tuple | Future) -> Any | tuple:
        processed_output = self._finish_run(run_output)
        if self.parent is None:
            self.emit()
        else:
            self.parent.register_child_emitting(self)
        return processed_output

    _finish_run_and_emit_ran.__doc__ = (
        Runnable._finish_run.__doc__
        + """

    Finally, fire the `ran` signal.
    """
    )

    @property
    def emitting_channels(self) -> tuple[OutputSignal]:
        return (self.signals.output.ran,)

    def emit(self):
        for channel in self.emitting_channels:
            channel()

    def execute(self, *args, **kwargs):
        """
        A shortcut for :meth:`run` with particular flags.

        Run the node with whatever input it currently has (or is given as kwargs here),
        run it on this python process, and don't emit the `ran` signal afterwards.

        Intended to be useful for debugging by just forcing the node to do its thing
        right here, right now, and as-is.
        """
        return self.run(
            *args,
            run_data_tree=False,
            run_parent_trees_too=False,
            fetch_input=False,
            check_readiness=False,
            force_local_execution=True,
            emit_ran_signal=False,
            **kwargs,
        )

    def pull(self, *args, run_parent_trees_too=False, **kwargs):
        """
        A shortcut for :meth:`run` with particular flags.

        Runs nodes upstream in the data graph, then runs this node without triggering
        any downstream runs. By default only runs sibling nodes, but can optionally
        require the parent node to pull in its own upstream runs (this is recursive
        up to the parent-most object).

        Args:
            run_parent_trees_too (bool): Whether to (recursively) require the parent to
                first pull.
        """
        return self.run(
            *args,
            run_data_tree=True,
            run_parent_trees_too=run_parent_trees_too,
            fetch_input=True,
            check_readiness=True,
            force_local_execution=False,
            emit_ran_signal=False,
            **kwargs,
        )

    def __call__(self, *args, **kwargs) -> None:
        """
        A shortcut for :meth:`pull` that automatically runs the entire set of upstream data
        dependencies all the way to the parent-most graph object.
        """
        return self.pull(*args, run_parent_trees_too=True, **kwargs)

    @property
    def ready(self) -> bool:
        return super().ready and self.inputs.ready

    @property
    def color(self) -> str:
        """A hex code color for use in drawing."""
        return SeabornColors.white

    def draw(
        self,
        depth: int = 1,
        rankdir: Literal["LR", "TB"] = "LR",
        size: Optional[tuple] = None,
        save: bool = False,
        view: bool = False,
        directory: Optional[Path | str] = None,
        filename: Optional[Path | str] = None,
        format: Optional[str] = None,
        cleanup: bool = True,
    ) -> graphviz.graphs.Digraph:
        """
        Draw the node structure and return it as a graphviz object.

        A selection of the :func:`graphviz.Graph.render` method options are exposed, and if
        :param:`view` or :param:`filename` is provided, this will be called before returning the
        graph.
        The graph file and rendered image will be stored in the node's working
        directory.
        This is purely for convenience -- since we directly return a graphviz object
        you can instead use this to leverage the full power of graphviz.

        Args:
            depth (int): How deeply to decompose the representation of composite nodes
                to reveal their inner structure. (Default is 1, which will show owned
                nodes if _this_ is a composite node, but all children will be drawn
                at the level of showing their IO only.) A depth value greater than the
                max depth of the node will have no adverse side effects.
            rankdir ("LR" | "TB"): Use left-right or top-bottom graphviz `rankdir` to
                orient the flow of the graph.
            size (tuple[int | float, int | float] | None): The size of the diagram, in
                inches(?); respects ratio by scaling until at least one dimension
                matches the requested size. (Default is None, automatically size.)
            save (bool): Render the graph image. (Default is False. When True, all
                other defaults will yield a PDF in the node's working directory.)
            view (bool): `graphviz.Graph.render` argument, open the rendered result
                with the default application. (Default is False. When True, default
                values for the directory and filename are supplied by the node working
                directory and label.)
            directory (Path|str|None): `graphviz.Graph.render` argument, (sub)directory
                for source saving and rendering. (Default is None, which uses the
                node's working directory.)
            filename (Path|str): `graphviz.Graph.render` argument, filename for saving
                the source. (Default is None, which uses the node label + `"_graph"`.
            format (str|None): `graphviz.Graph.render` argument, the output format used
                for rendering ('pdf', 'png', etc.).
            cleanup (bool): `graphviz.Graph.render` argument, delete the source file
                after successful rendering. (Default is True -- unlike graphviz.)

        Returns:
            (graphviz.graphs.Digraph): The resulting graph object.

        """

        if size is not None:
            size = f"{size[0]},{size[1]}"
        graph = GraphvizNode(self, depth=depth, rankdir=rankdir, size=size).graph
        if save or view or filename is not None:
            directory = self.working_directory.path if directory is None else directory
            filename = self.label + "_graph" if filename is None else filename
            graph.render(
                view=view,
                directory=directory,
                filename=filename,
                format=format,
                cleanup=cleanup,
            )
        return graph

    def __str__(self):
        return (
            f"{self.label} ({self.__class__.__name__}):\n"
            f"{str(self.inputs)}\n"
            f"{str(self.outputs)}\n"
            f"{str(self.signals)}"
        )

    def replace_with(self, other: Node | type[Node]):
        """
        If this node has a parent, invokes `self.parent.replace_child(self, other)` to swap
        out this node for the other node in the parent graph.

        The replacement must have fully compatible IO, i.e. its IO must be a superset of
        this node's IO with all the same labels and type hints (although the latter is
        not strictly enforced and will only cause trouble if there is an incompatibility
        that causes trouble in the process of copying over connections)

        Args:
            other (Node|type[Node]): The replacement.
        """
        if self.parent is not None:
            self.parent.replace_child(self, other)
        else:
            logger.info(f"Could not replace_child {self.label}, as it has no parent.")

    _save_load_warnings = """
        HERE BE DRAGONS!!!

        Warning:
            This almost certainly only fails for subclasses of :class:`Node` that don't
            override `node_function` or `macro_creator` directly, as these are expected 
            to be part of the class itself (and thus already present on our instantiated 
            object) and are never stored. Nodes created using the provided decorators 
            should all work.

        Warning:
            If you modify a `Macro` class in any way (changing its IO maps, rewiring 
            internal connections, or replacing internal nodes), don't expect 
            saving/loading to work.

        Warning:
            If the underlying source code has changed since saving (i.e. the node doing 
            the loading does not use the same code as the node doing the saving, or the 
            nodes in some node package have been modified), then all bets are off.

    """

    def save(self, backend: str | StorageInterface = "pickle"):
        """
        Writes the node to file using the requested interface as a back end.
        """
        for backend in available_backends(backend=backend, only_requested=True):
            backend.save(self)

    save.__doc__ += _save_load_warnings

    def save_checkpoint(self, backend: str | StorageInterface = "pickle"):
        """
        Triggers a save on the parent-most node.
        """
        self.graph_root.save(backend=backend)

    def load(
        self,
        backend: str | StorageInterface = "pickle",
        only_requested=False
    ):
        """
        Loads the node file and set the loaded state as the node's own.

        Raises:
            TypeError: when the saved node has a different class name.
        """
        for backend in available_backends(
            backend=backend,
            only_requested=only_requested
        ):
            if backend.has_contents(self):
                inst = backend.load(self)
                break
            raise FileNotFoundError(f"{self.label} could not find saved content.")

        if inst.__class__ != self.__class__:
            raise TypeError(
                f"{self.label} cannot load, as it has type "
                f"{self.__class__.__name__},  but the saved node has type "
                f"{inst.__class__.__name__}"
            )
        self.__setstate__(inst.__getstate__())

    load.__doc__ += _save_load_warnings

    def delete_storage(
        self,
        backend: Literal["pickle"] | StorageInterface | None = None,
        only_requested: bool = False,
    ):
        """Remove save file(s)."""
        for backend in available_backends(
            backend=backend, only_requested=only_requested
        ):
            backend.delete(self)

    def has_savefile(self, backend: Literal["pickle"] | StorageInterface | None = None):
        return any(be.has_contents(self) for be in available_backends(backend=backend))

    @property
    def import_ready(self) -> bool:
        """
        Checks whether `importlib` can find this node's class, and if so whether the
        imported object matches the node's type.

        Returns:
            (bool): Whether the imported module and name of this node's class match
                its type.
        """
        try:
            module = self.__class__.__module__
            class_ = getattr(import_module(module), self.__class__.__name__)
            if module == "__main__":
                logger.warning(f"{self.label} is only defined in __main__")
            return type(self) is class_
        except (ModuleNotFoundError, AttributeError):
            return False

    @property
    def import_readiness_report(self):
        print(self.report_import_readiness())

    def report_import_readiness(self, tabs=0, report_so_far=""):
        newline = "\n" if len(report_so_far) > 0 else ""
        tabspace = tabs * "\t"
        return (
            report_so_far + f"{newline}{tabspace}{self.label}: "
            f"{'ok' if self.import_ready else 'NOT IMPORTABLE'}"
        )
