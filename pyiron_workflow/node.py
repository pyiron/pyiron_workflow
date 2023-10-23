"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.
"""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Literal, Optional, TYPE_CHECKING

from pyiron_workflow.channels import NotData
from pyiron_workflow.draw import Node as GraphvizNode
from pyiron_workflow.executors import CloudpickleProcessPoolExecutor as Executor
from pyiron_workflow.files import DirectoryObject
from pyiron_workflow.has_to_dict import HasToDict
from pyiron_workflow.io import Signals, InputSignal, OutputSignal
from pyiron_workflow.util import SeabornColors

if TYPE_CHECKING:
    import graphviz

    from pyiron_workflow.channels import Channel
    from pyiron_workflow.composite import Composite
    from pyiron_workflow.io import Inputs, Outputs


def manage_status(node_method):
    """
    Decorates methods of nodes that might be time-consuming, i.e. their main run
    functionality.

    Sets `running` to true until the method completes and either fails or returns
    something other than a `concurrent.futures.Future` instance; sets `failed` to true
    if the method raises an exception; raises a `RuntimeError` if the node is already
    `running` or `failed`.
    """

    def wrapped_method(node: Node, *args, **kwargs):  # rather node:Node
        if node.running:
            raise RuntimeError(f"{node.label} is already running")
        elif node.failed:
            raise RuntimeError(f"{node.label} has a failed status")

        node.running = True
        try:
            out = node_method(node, *args, **kwargs)
            return out
        except Exception as e:
            node.failed = True
            out = None
            raise e
        finally:
            # Leave the status as running if the method returns a future
            node.running = isinstance(out, Future)

    return wrapped_method


class Node(HasToDict, ABC):
    """
    Nodes are elements of a computational graph.
    They have input and output data channels that interface with the outside
    world, and a callable that determines what they actually compute, and input and
    output signal channels that can be used to customize the execution flow of their
    graph;
    Together these channels represent edges on the dual data and execution computational
    graphs.

    Nodes can be run to force their computation, or more gently updated, which will
    trigger a run only if all of the input is ready (i.e. channel values conform to
    any type hints provided).

    Nodes may have a `parent` node that owns them as part of a sub-graph.

    Every node must be named with a `label`, and may use this label to attempt to create
    a working directory in memory for itself if requested.
    These labels also help to identify nodes in the wider context of (potentially
    nested) computational graphs.

    By default, nodes' signals input comes with `run` and `ran` IO ports, which invoke
    the `run()` method and emit after running the node, respectfully.
    (Whether we get all the way to emitting the `ran` signal depends on how the node
    was invoked -- it is possible to computing things with the node without sending
    any more signals downstream.)
    These signal connections can be made manually by reference to the node signals
    channel, or with the `>` symbol to indicate a flow of execution. This syntactic
    sugar can be mixed between actual signal channels (output signal > input signal),
    or nodes, but when referring to nodes it is always a shortcut to the `run`/`ran`
    channels.

    The `run()` method returns a representation of the node output (possible a futures
    object, if the node is running on an executor), and consequently `update()` also
    returns this output if the node is `ready`. Both `run()` and `update()` will raise
    errors if the node is already running or has a failed status.

    Calling an already instantiated node allows its input channels to be updated using
    keyword arguments corresponding to the channel labels, performing a batch-update of
    all supplied input and then calling `run()`.
    As such, calling the node _also_ returns a representation of the output (or `None`
    if the node is not set to run on updates, or is otherwise unready to run).

    Nodes have a status, which is currently represented by the `running` and `failed`
    boolean flag attributes.
    These are updated automatically when the node's operation is invoked, e.g. with
    `run`, `execute`, `pull`, or by calling the node instance.

    Nodes can be run on the main python process that owns them, or by setting their
    `executor` attribute to `True`, in which case a
    `pyiron_workflow.executors.CloudPickleExecutor` will be used to run the node on a
    new process on a single core (in the future, the interface will look a little
    different and you'll have more options than that).
    In case they are run with an executor, their `future` attribute will be populated
    with the resulting future object.
    WARNING: Executors are currently only working when the node executable function does
        not use `self`.

    This is an abstract class.
    Children *must* define how `inputs` and `outputs` are constructed, and what will
    happen `on_run`.
    They may also override the `run_args` property to specify input passed to the
    defined `on_run` method, and may add additional signal channels to the signals IO.

    # TODO: Everything with (de)serialization and executors for running on something
    #       other than the main python process.

    Attributes:
        connected (bool): Whether _any_ of the IO (including signals) are connected.
        failed (bool): Whether the node raised an error calling `run`. (Default
            is False.)
        fully_connected (bool): whether _all_ of the IO (including signals) are
            connected.
        future (concurrent.futures.Future | None): A futures object, if the node is
            currently running or has already run using an executor.
        inputs (pyiron_workflow.io.Inputs): **Abstract.** Children must define
            a property returning an `Inputs` object.
        label (str): A name for the node.
        outputs (pyiron_workflow.io.Outputs): **Abstract.** Children must define
            a property returning an `Outputs` object.
        parent (pyiron_workflow.composite.Composite | None): The parent object
            owning this, if any.
        ready (bool): Whether the inputs are all ready and the node is neither
            already running nor already failed.
        running (bool): Whether the node has called `run` and has not yet
            received output from this call. (Default is False.)
        signals (pyiron_workflow.io.Signals): A container for input and output
            signals, which are channels for controlling execution flow. By default, has
            a `signals.inputs.run` channel which has a callback to the `run` method,
            and `signals.outputs.ran` which should be called at when the `run` method
            is finished.
            Additional signal channels in derived classes can be added to
            `signals.inputs` and  `signals.outputs` after this mixin class is
            initialized.

    Methods:
        disconnect: Remove all connections, including signals.
        draw: Use graphviz to visualize the node, its IO and, if composite in nature,
            its internal structure.
        on_run: **Abstract.** Do the thing.
        run: A wrapper to handle all the infrastructure around executing `on_run`.
        set_input_values: Allows input channels' values to be updated without any running.
    """

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Composite] = None,
        **kwargs,
    ):
        """
        A mixin class for objects that can form nodes in the graph representation of a
        computational workflow.

        Args:
            label (str): A name for this node.
            *args: Arguments passed on with `super`.
            **kwargs: Keyword arguments passed on with `super`.
        """
        super().__init__(*args, **kwargs)
        self.label: str = label
        self.parent = parent
        if parent is not None:
            parent.add(self)
        self.running = False
        self.failed = False
        self.signals = self._build_signal_channels()
        self._working_directory = None
        self.executor = False
        # We call it an executor, but it's just whether to use one.
        # This is a simply stop-gap as we work out more sophisticated ways to reference
        # (or create) an executor process without ever trying to pickle a `_thread.lock`
        self.future: None | Future = None

    @property
    @abstractmethod
    def inputs(self) -> Inputs:
        pass

    @property
    @abstractmethod
    def outputs(self) -> Outputs:
        pass

    @property
    @abstractmethod
    def on_run(self) -> callable[..., Any | tuple]:
        """
        What the node actually does!
        """
        pass

    @property
    @abstractmethod
    def run_args(self) -> dict:
        """
        Any data needed for `on_run`, will be passed as **kwargs.
        """

    @abstractmethod
    def process_run_result(self, run_output):
        """
        What to _do_ with the results of `on_run` once you have them.

        By extracting this as a separate method, we allow the node to pass the actual
        execution off to another entity and release the python process to do other
        things. In such a case, this function should be registered as a callback
        so that the node can process the result of that process.

        Args:
            run_output: The results of a `self.on_run(self.run_args)` call.
        """

    def run(
        self,
        first_fetch_input: bool = True,
        then_emit_output_signals: bool = True,
        force_local_execution: bool = False,
    ):
        """
        Update the input (with whatever is currently available -- does _not_ trigger
        any other nodes to run) and use it to perform the node's operation. After,
        emit all output signals.

        If executor information is specified, execution happens on that process, a
        callback is registered, and futures object is returned.

        Args:
            first_fetch_input (bool): Whether to first update inputs with the
                highest-priority connections holding data. (Default is True.)
            then_emit_output_signals (bool): Whether to fire off all output signals
                (e.g. `ran`) afterwards. (Default is True.)
            force_local_execution (bool): Whether to ignore any executor settings and
                force the computation to run locally. (Default is False.)

        Returns:
            (Any | Future): The result of running the node, or a futures object (if
                running on an executor).
        """
        if first_fetch_input:
            self.inputs.fetch()
        return self._run(
            finished_callback=self._finish_run_and_emit_ran if then_emit_output_signals
            else self._finish_run,
            force_local_execution=force_local_execution,
        )

    @manage_status
    def _run(
        self,
        finished_callback: callable,
        force_local_execution: bool,
    ) -> Any | tuple | Future:
        """
        Executes the functionality of the node defined in `on_run`.
        Handles the status of the node, and communicating with any remote
        computing resources.
        """
        if force_local_execution or not self.executor:
            # Run locally
            run_output = self.on_run(**self.run_args)
            return finished_callback(run_output)
        else:
            # Just blindly try to execute -- as we nail down the executor interaction
            # we'll want to fail more cleanly here.
            executor = Executor()
            self.future = executor.submit(self.on_run, **self.run_args)
            self.future.add_done_callback(finished_callback)
            return self.future

    def _finish_run(self, run_output: tuple | Future) -> Any | tuple:
        """
        Switch the node status, then process and return the run result.

        Sets the `failed` status to true if an exception is encountered.
        """
        if isinstance(run_output, Future):
            run_output = run_output.result()

        self.running = False
        try:
            processed_output = self.process_run_result(run_output)
            return processed_output
        except Exception as e:
            self.failed = True
            raise e

    def _finish_run_and_emit_ran(self, run_output: tuple | Future) -> Any | tuple:
        processed_output = self._finish_run(run_output)
        self.signals.output.ran()
        return processed_output

    _finish_run_and_emit_ran.__doc__ = (
        _finish_run.__doc__
        + """

    Finally, fire the `ran` signal.
    """
    )

    def execute(self):
        """
        Run the node with whatever input it currently has, run it on this python
        process, and don't emit the `ran` signal afterwards.

        Intended to be useful for debugging by just forcing the node to do its thing
        right here, right now, and as-is.
        """
        return self.run(
            first_fetch_input=False,
            then_emit_output_signals=False,
            force_local_execution=True
        )

    def pull(self):
        """
        Use topological analysis to build a tree of all upstream dependencies; run them
        first, then run this node to get an up-to-date result. Does _not_ fire the `ran`
        signal afterwards.
        """
        raise NotImplementedError
        # Need to implement everything for on-the-fly construction of the upstream
        # graph and its execution
        # Then,
        return self.run(then_emit_output_signals=False)

    def __call__(self, **kwargs) -> None:
        self.set_input_values(**kwargs)
        return self.run()

    def set_input_values(self, **kwargs) -> None:
        """
        Match keywords to input channels and update their values.

        Throws a warning if a keyword is provided that cannot be found among the input
        keys.

        Args:
            **kwargs: input key - input value (including channels for connection) pairs.
        """
        for k, v in kwargs.items():
            if k in self.inputs.labels:
                self.inputs[k] = v
            else:
                warnings.warn(
                    f"The keyword '{k}' was not found among input labels. If you are "
                    f"trying to update a node keyword, please use attribute assignment "
                    f"directly instead of calling"
                )

    def _build_signal_channels(self) -> Signals:
        signals = Signals()
        signals.input.run = InputSignal("run", self, self.run)
        signals.output.ran = OutputSignal("ran", self)
        return signals

    def update(self) -> Any | tuple | Future | None:
        if self.ready:
            return self.run()

    @property
    def working_directory(self):
        if self._working_directory is None:
            if self.parent is not None and hasattr(self.parent, "working_directory"):
                parent_dir = self.parent.working_directory
                self._working_directory = parent_dir.create_subdirectory(self.label)
            else:
                self._working_directory = DirectoryObject(self.label)
        return self._working_directory

    def disconnect(self):
        """
        Disconnect all connections belonging to inputs, outputs, and signals channels.

        Returns:
            [list[tuple[Channel, Channel]]]: A list of the pairs of channels that no
                longer participate in a connection.
        """
        destroyed_connections = []
        destroyed_connections.extend(self.inputs.disconnect())
        destroyed_connections.extend(self.outputs.disconnect())
        destroyed_connections.extend(self.signals.disconnect())
        return destroyed_connections

    @property
    def ready(self) -> bool:
        return not (self.running or self.failed) and self.inputs.ready

    @property
    def connected(self) -> bool:
        return self.inputs.connected or self.outputs.connected or self.signals.connected

    @property
    def fully_connected(self):
        return (
            self.inputs.fully_connected
            and self.outputs.fully_connected
            and self.signals.fully_connected
        )

    @property
    def color(self) -> str:
        """A hex code color for use in drawing."""
        return SeabornColors.white

    def draw(
        self, depth: int = 1, rankdir: Literal["LR", "TB"] = "LR"
    ) -> graphviz.graphs.Digraph:
        """
        Draw the node structure.

        Args:
            depth (int): How deeply to decompose the representation of composite nodes
                to reveal their inner structure. (Default is 1, which will show owned
                nodes if _this_ is a composite node, but all children will be drawn
                at the level of showing their IO only.) A depth value greater than the
                max depth of the node will have no adverse side effects.
            rankdir ("LR" | "TB"): Use left-right or top-bottom graphviz `rankdir` to
                orient the flow of the graph.

        Returns:
            (graphviz.graphs.Digraph): The resulting graph object.

        Note:
            The graphviz docs will elucidate all the possibilities of what to do with
            the returned object, but the thing you are most likely to need is the
            `render` method, which allows you to save the resulting graph as an image.
            E.g. `self.draw().render(filename="my_node", format="png")`.
        """
        return GraphvizNode(self, depth=depth, rankdir=rankdir).graph

    def __str__(self):
        return (
            f"{self.label} ({self.__class__.__name__}):\n"
            f"{str(self.inputs)}\n"
            f"{str(self.outputs)}\n"
            f"{str(self.signals)}"
        )

    def connect_output_signal(self, signal: OutputSignal):
        self.signals.input.run.connect(signal)

    def __gt__(self, other: InputSignal | Node):
        """
        Allows users to connect run and ran signals like: `first_node > second_node`.
        """
        other.connect_output_signal(self.signals.output.ran)
        return True

    def get_parent_proximate_to(self, composite: Composite) -> Composite | None:
        parent = self.parent
        while parent is not None and parent.parent is not composite:
            parent = parent.parent
        return parent

    def get_first_shared_parent(self, other: Node) -> Composite | None:
        our, their = self, other
        while our.parent is not None:
            while their.parent is not None:
                if our.parent is their.parent:
                    return our.parent
                their = their.parent
            our = our.parent
            their = other
        return None

    def copy_io(
        self,
        other: Node,
        connections_fail_hard: bool = True,
        values_fail_hard: bool = False,
    ) -> None:
        """
        Copies connections and values from another node's IO onto this node's IO.
        Other channels with no connections are ignored for copying connections, and all
        data channels without data are ignored for copying data.
        Otherwise, default behaviour is to throw an exception if any of the other node's
        connections fail to copy, but failed value copies are simply ignored (e.g.
        because this node does not have a channel with a commensurate label or the
        value breaks a type hint).
        This error throwing/passing behaviour can be controlled with boolean flags.

        In the case that an exception is thrown, all newly formed connections are broken
        and any new values are reverted to their old state before the exception is
        raised.

        Args:
            other (Node): The other node whose IO to copy.
            connections_fail_hard: Whether to raise exceptions encountered when copying
                connections. (Default is True.)
            values_fail_hard (bool): Whether to raise exceptions encountered when
                copying values. (Default is False.)
        """
        new_connections = self._copy_connections(other, fail_hard=connections_fail_hard)
        try:
            self._copy_values(other, fail_hard=values_fail_hard)
        except Exception as e:
            for this, other in new_connections:
                this.disconnect(other)
            raise e

    def _copy_connections(
        self,
        other: Node,
        fail_hard: bool = True,
    ) -> list[tuple[Channel, Channel]]:
        """
        Copies all the connections in another node to this one.
        Expects all connected channels on the other node to have a counterpart on this
        node -- i.e. the same label, type, and (for data) a type hint compatible with
        all the existing connections being copied.
        This requirement can be optionally relaxed such that any failures encountered
        when attempting to make a connection (i.e. this node has no channel with a
        corresponding label as the other node, or the new connection fails its validity
        check), such that we simply continue past these errors and make as many
        connections as we can while ignoring errors.

        This node may freely have additional channels not present in the other node.
        The other node may have additional channels not present here as long as they are
        not connected.

        If an exception is going to be raised, any connections copied so far are
        disconnected first.

        Args:
            other (Node): the node whose connections should be copied.
            fail_hard (bool): Whether to raise an error an exception is encountered
                when trying to reproduce a connection. (Default is True; revert new
                connections then raise the exception.)

        Returns:
            list[tuple[Channel, Channel]]: A list of all the newly created connection
                pairs (for reverting changes).
        """
        new_connections = []
        for my_panel, other_panel in [
            (self.inputs, other.inputs),
            (self.outputs, other.outputs),
            (self.signals.input, other.signals.input),
            (self.signals.output, other.signals.output),
        ]:
            for key, channel in other_panel.items():
                for target in channel.connections:
                    try:
                        my_panel[key].connect(target)
                        new_connections.append((my_panel[key], target))
                    except Exception as e:
                        if fail_hard:
                            # If you run into trouble, unwind what you've done
                            for this, other in new_connections:
                                this.disconnect(other)
                            raise e
                        else:
                            continue
        return new_connections

    def _copy_values(
        self,
        other: Node,
        fail_hard: bool = False,
    ) -> list[tuple[Channel, Any]]:
        """
        Copies all data from input and output channels in the other node onto this one.
        Ignores other channels that hold non-data.
        Failures to find a corresponding channel on this node (matching label, type, and
        compatible type hint) are ignored by default, but can optionally be made to
        raise an exception.

        If an exception is going to be raised, any values updated so far are
        reverted first.

        Args:
            other (Node): the node whose data values should be copied.
            fail_hard (bool): Whether to raise an error an exception is encountered
                when trying to duplicate a value. (Default is False, just keep going
                past other's channels with no compatible label here and past values
                that don't match type hints here.)

        Returns:
            list[tuple[Channel, Any]]: A list of tuples giving channels whose value has
                been updated and what it used to be (for reverting changes).
        """
        old_values = []
        for my_panel, other_panel in [
            (self.inputs, other.inputs),
            (self.outputs, other.outputs),
        ]:
            for key, to_copy in other_panel.items():
                if to_copy.value is not NotData:
                    try:
                        old_value = my_panel[key].value
                        my_panel[key].copy_value(to_copy)
                        old_values.append((my_panel[key], old_value))
                    except Exception as e:
                        if fail_hard:
                            # If you run into trouble, unwind what you've done
                            for channel, value in old_values:
                                channel.value = value
                            raise e
                        else:
                            continue
        return old_values

    def replace_with(self, other: Node | type[Node]):
        """
        If this node has a parent, invokes `self.parent.replace(self, other)` to swap
        out this node for the other node in the parent graph.

        The replacement must have fully compatible IO, i.e. its IO must be a superset of
        this node's IO with all the same labels and type hints (although the latter is
        not strictly enforced and will only cause trouble if there is an incompatibility
        that causes trouble in the process of copying over connections)

        Args:
            other (Node|type[Node]): The replacement.
        """
        if self.parent is not None:
            self.parent.replace(self, other)
        else:
            warnings.warn(f"Could not replace {self.label}, as it has no parent.")

    def __getstate__(self):
        state = self.__dict__
        state["parent"] = None
        # I am not at all confident that removing the parent here is the _right_
        # solution.
        # In order to run composites on a parallel process, we ship off just the nodes
        # and starting nodes.
        # When the parallel process returns these, they're obviously different
        # instances, so we re-parent them back to the receiving composite.
        # At the same time, we want to make sure that the _old_ children get orphaned.
        # Of course, we could do that directly in the composite method, but it also
        # works to do it here.
        # Something I like about this, is it also means that when we ship groups of
        # nodes off to another process with cloudpickle, they're definitely not lugging
        # along their parent, its connections, etc. with them!
        # This is all working nicely as demonstrated over in the macro test suite.
        # However, I have a bit of concern that when we start thinking about
        # serialization for storage instead of serialization to another process, this
        # might introduce a hard-to-track-down bug.
        # For now, it works and I'm going to be super pragmatic and go for it, but
        # for the record I am admitting that the current shallowness of my understanding
        # may cause me/us headaches in the future.
        # -Liam
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
