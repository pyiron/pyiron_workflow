"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.
"""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Literal, Optional, TYPE_CHECKING

from pyiron_workflow.draw import Node as GraphvizNode
from pyiron_workflow.files import DirectoryObject
from pyiron_workflow.has_to_dict import HasToDict
from pyiron_workflow.io import Signals, InputSignal, OutputSignal
from pyiron_workflow.util import SeabornColors

if TYPE_CHECKING:
    import graphviz

    from pyiron_workflow.composite import Composite
    from pyiron_workflow.io import IO, Inputs, Outputs


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

    By default, nodes' signals input comes with `run` and `ran` IO ports which force
    the `run()` method and which emit after `finish_run()` is completed, respectfully.
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
    Their value is controlled automatically in the defined `run` and `finish_run`
    methods.

    Nodes can be run on the main python process that owns them, or by assigning an
    appropriate executor to their `executor` attribute.
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
        # TODO: Move from a traditional "sever" to a tinybase "executor"
        # TODO: Provide support for actually computing stuff with the executor
        self.signals = self._build_signal_channels()
        self._working_directory = None
        self.executor = None
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

    @manage_status
    def execute(self):
        """
        Perform the node's operation with its current data.

        Execution happens directly on this python process.
        """
        return self.process_run_result(self.on_run(**self.run_args))

    def run(self):
        """
        Update the input (with whatever is currently available -- does _not_ trigger
        any other nodes to run) and use it to perform the node's operation.

        If executor information is specified, execution happens on that process, a
        callback is registered, and futures object is returned.

        Once complete, fire `ran` signal to propagate execution in the computation graph
        that owns this node (if any).
        """
        self.update_input()
        return self._run(finished_callback=self.finish_run_and_emit_ran)

    def pull(self):
        raise NotImplementedError
        # Need to implement everything for on-the-fly construction of the upstream
        # graph and its execution
        # Then,
        self.update_input()
        return self._run(finished_callback=self.finish_run)

    def update_input(self, **kwargs) -> None:
        """
        Fetch the latest and highest-priority input values from connections, then
        overwrite values with keywords arguments matching input channel labels.

        Any channel that has neither a connection nor a kwarg update at time of call is
        left unchanged.

        Throws a warning if a keyword is provided that cannot be found among the input
        keys.

        If you really want to update just a single value without any other side-effects,
        this can always be accomplished by following the full semantic path to the
        channel's value: `my_node.input.my_channel.value = "foo"`.

        Args:
            **kwargs: input key - input value (including channels for connection) pairs.
        """
        self.inputs.fetch()
        for k, v in kwargs.items():
            if k in self.inputs.labels:
                self.inputs[k] = v
            else:
                warnings.warn(
                    f"The keyword '{k}' was not found among input labels. If you are "
                    f"trying to update a node keyword, please use attribute assignment "
                    f"directly instead of calling"
                )

    @manage_status
    def _run(self, finished_callback: callable) -> Any | tuple | Future:
        """
        Executes the functionality of the node defined in `on_run`.
        Handles the status of the node, and communicating with any remote
        computing resources.
        """
        if self.executor is None:
            run_output = self.on_run(**self.run_args)
            return finished_callback(run_output)
        else:
            # Just blindly try to execute -- as we nail down the executor interaction
            # we'll want to fail more cleanly here.
            self.future = self.executor.submit(self.on_run, **self.run_args)
            self.future.add_done_callback(finished_callback)
            return self.future

    def finish_run(self, run_output: tuple | Future) -> Any | tuple:
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

    def finish_run_and_emit_ran(self, run_output: tuple | Future) -> Any | tuple:
        processed_output = self.finish_run(run_output)
        self.signals.output.ran()
        return processed_output

    finish_run_and_emit_ran.__doc__ = (
        finish_run.__doc__
        + """
    
    Finally, fire the `ran` signal.
    """
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

    def __call__(self, **kwargs) -> None:
        self.update_input(**kwargs)
        return self.run()

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

    def copy_connections(
        self,
        other: Node,
        fail_hard: bool = True,
    ) -> None:
        """
        Copies all the connections in another node to this one.
        Expects the channels available on this node to be commensurate to those on the
        other, i.e. same label, compatible type hint for the connections that exist.
        This node may freely have additional channels not present in the other node.
        The other node may have additional channels not present here as long as they are
        not connected.
        This final condition can optionally be relaxed, such that as many connections as
        possible are copied, and any failures are simply overlooked.

        If an exception is going to be raised, any connections copied so far are
        disconnected first.

        Args:
            other (Node): the node whose connections should be copied.
            fail_hard (bool): Whether to raise an error an exception is encountered
                when trying to reproduce a connection.

        Raises:
            (Exception): Any exception encountered when a connection is attempted and
                fails (only when `fail_hard` is True, otherwise we `continue` past any
                and all exceptions encountered).
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
                            for connection in new_connections:
                                connection[0].disconnect(connection[1])
                            raise e
                        else:
                            continue

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
