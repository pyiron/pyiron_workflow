"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.
"""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Literal, Optional, TYPE_CHECKING

from pyiron_contrib.executors import CloudpickleProcessPoolExecutor
from pyiron_contrib.workflow.draw import Node as GraphvizNode
from pyiron_contrib.workflow.files import DirectoryObject
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.io import Signals, InputSignal, OutputSignal
from pyiron_contrib.workflow.util import SeabornColors

if TYPE_CHECKING:
    import graphviz

    from pyiron_base.jobs.job.extension.server.generic import Server

    from pyiron_contrib.workflow.composite import Composite
    from pyiron_contrib.workflow.io import Inputs, Outputs


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
    returns this output if the node is `ready`.

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
        inputs (pyiron_contrib.workflow.io.Inputs): **Abstract.** Children must define
            a property returning an `Inputs` object.
        label (str): A name for the node.
        outputs (pyiron_contrib.workflow.io.Outputs): **Abstract.** Children must define
            a property returning an `Outputs` object.
        parent (pyiron_contrib.workflow.composite.Composite | None): The parent object
            owning this, if any.
        ready (bool): Whether the inputs are all ready and the node is neither
            already running nor already failed.
        running (bool): Whether the node has called `run` and has not yet
            received output from this call. (Default is False.)
        server (Optional[pyiron_base.jobs.job.extension.server.generic.Server]): A
            server object for computing things somewhere else. Default (and currently
            _only_) behaviour is to compute things on the main python process owning
            the node.
        signals (pyiron_contrib.workflow.io.Signals): A container for input and output
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
        # TODO: Replace running and failed with a state object
        self._server: Server | None = (
            None  # Or "task_manager" or "executor" -- we'll see what's best
        )
        # TODO: Move from a traditional "sever" to a tinybase "executor"
        # TODO: Provide support for actually computing stuff with the server/executor
        self.signals = self._build_signal_channels()
        self._working_directory = None
        self.executor: None | CloudpickleProcessPoolExecutor = None
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
    def run_args(self) -> dict:
        """
        Any data needed for `on_run`, will be passed as **kwargs.
        """
        return {}

    def process_run_result(self, run_output: Any | tuple) -> None:
        """
        What to _do_ with the results of `on_run` once you have them.

        Args:
            run_output (tuple): The results of a `self.on_run(self.run_args)` call.
        """
        pass

    def run(self) -> Any | tuple | Future:
        """
        Executes the functionality of the node defined in `on_run`.
        Handles the status of the node, and communicating with any remote
        computing resources.
        """
        if self.running:
            raise RuntimeError(f"{self.label} is already running")

        self.running = True
        self.failed = False

        if self.executor is None:
            try:
                run_output = self.on_run(**self.run_args)
            except Exception as e:
                self.running = False
                self.failed = True
                raise e
            return self.finish_run(run_output)
        elif isinstance(self.executor, CloudpickleProcessPoolExecutor):
            self.future = self.executor.submit(self.on_run, **self.run_args)
            self.future.add_done_callback(self.finish_run)
            return self.future
        else:
            raise NotImplementedError(
                "We currently only support executing the node functionality right on "
                "the main python process or with a "
                "pyiron_contrib.workflow.util.CloudpickleProcessPoolExecutor."
            )

    def finish_run(self, run_output: tuple | Future) -> Any | tuple:
        """
        Switch the node status, process the run result, then fire the ran signal.

        By extracting this as a separate method, we allow the node to pass the actual
        execution off to another entity and release the python process to do other
        things. In such a case, this function should be registered as a callback
        so that the node can finish "running" and, e.g. push its data forward when that
        execution is finished. In such a case, a `concurrent.futures.Future` object is
        expected back and must be unpacked.
        """
        if isinstance(run_output, Future):
            run_output = run_output.result()

        self.running = False
        try:
            self.process_run_result(run_output)
            self.signals.output.ran()
            return run_output
        except Exception as e:
            self.failed = True
            raise e

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

    @property
    def server(self) -> Server | None:
        return self._server

    @server.setter
    def server(self, server: Server | None):
        self._server = server

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

    def update_input(self, **kwargs) -> None:
        """
        Match keywords to input channel labels and update input values.

        Args:
            **kwargs: input label - input value (including channels for connection)
             pairs.
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
