"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING

from pyiron_contrib.executors import CloudpickleProcessPoolExecutor
from pyiron_contrib.workflow.files import DirectoryObject
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.io import Signals, InputSignal, OutputSignal

if TYPE_CHECKING:
    from pyiron_base.jobs.job.extension.server.generic import Server

    from pyiron_contrib.workflow.composite import Composite
    from pyiron_contrib.workflow.io import Inputs, Outputs


class Node(HasToDict, ABC):
    """
    Nodes are elements of a computational graph.
    They have input and output data channels that interface with the outside
    world, and a callable that determines what they actually compute, and input and
    output signal channels that can be used to customize the execution flow of the
    graph;
    Together these channels represent edges on the computational graph.

    Nodes can be run to force their computation, or more gently updated, which will
    trigger a run only if the `run_on_update` flag is set to true and all of the input
    is ready (i.e. channel values conform to any type hints provided).

    Nodes may have a `parent` node that owns them as part of a sub-graph.

    Every node must be named with a `label`, and may use this label to attempt to create
    a working directory in memory for itself if requested.
    These labels also help to identify nodes in the wider context of (potentially
    nested) computational graphs.

    By default, nodes' signals input comes with `run` and `ran` IO ports which force
    the `run()` method and which emit after `finish_run()` is completed, respectfully.

    Nodes have a status, which is currently represented by the `running` and `failed`
    boolean flags.
    Their value is controlled automatically in the defined `run` and `finish_run`
    methods.

    Nodes can be run on the main python process that owns them, or by assigning an
    appropriate executor to their `executor` attribute.
    In case they are run with an executor, their `future` attribute will be populated
    with the resulting future object.

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
        run_on_updates (bool): Whether to run when you are updated and all your input
            is ready and your status does not prohibit running. (Default is False).
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
        on_run: **Abstract.** Do the thing.
        run: A wrapper to handle all the infrastructure around executing `on_run`.
    """

    def __init__(
        self,
        label: str,
        *args,
        parent: Optional[Composite] = None,
        run_on_updates: bool = False,
        **kwargs,
    ):
        """
        A mixin class for objects that can form nodes in the graph representation of a
        computational workflow.

        Args:
            label (str): A name for this node.
            *args: Arguments passed on with `super`.
            **kwargs: Keyword arguments passed on with `super`.

        TODO: Shouldn't `update_on_instantiation` and `run_on_updates` both live here??
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
        self.run_on_updates: bool = run_on_updates
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
    def on_run(self) -> callable[..., tuple]:
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

    def process_run_result(self, run_output: tuple) -> None:
        """
        What to _do_ with the results of `on_run` once you have them.

        Args:
            run_output (tuple): The results of a `self.on_run(self.run_args)` call.
        """
        pass

    def run(self) -> None:
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
            self.finish_run(run_output)
        elif isinstance(self.executor, CloudpickleProcessPoolExecutor):
            self.future = self.executor.submit(self.on_run, **self.run_args)
            self.future.add_done_callback(self.finish_run)
        else:
            raise NotImplementedError(
                "We currently only support executing the node functionality right on "
                "the main python process or with a "
                "pyiron_contrib.workflow.util.CloudpickleProcessPoolExecutor."
            )

    def finish_run(self, run_output: tuple | Future):
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
        except Exception as e:
            self.failed = True
            raise e

    def _build_signal_channels(self) -> Signals:
        signals = Signals()
        signals.input.run = InputSignal("run", self, self.run)
        signals.output.ran = OutputSignal("ran", self)
        return signals

    def update(self) -> None:
        if self.run_on_updates and self.ready:
            self.run()

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
        self.inputs.disconnect()
        self.outputs.disconnect()
        self.signals.disconnect()

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
