"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from pyiron_contrib.workflow.files import DirectoryObject
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.io import Signals, InputSignal, OutputSignal

if TYPE_CHECKING:
    from pyiron_base.jobs.job.extension.server.generic import Server

    from pyiron_contrib.workflow.composite import Composite
    from pyiron_contrib.workflow.io import Inputs, Outputs


class Node(HasToDict, ABC):
    """
    A mixin class for objects that can form nodes in the graph representation of a
    computational workflow.

    Nodal objects have `inputs` and `outputs` channels for passing data, and `signals`
    channels for making callbacks on the class (input) and controlling execution flow
    (output) when connected to other nodal objects.

    Nodal objects can `run` to complete some computational task, or call a softer
    `update` which will run the task only if it is `ready` -- i.e. it is not currently
    running, has not previously tried to run and failed, and all of its inputs are ready
    (i.e. populated with data that passes type requirements, if any).

    Attributes:
        connected (bool): Whether _any_ of the IO (including signals) are connected.
        failed (bool): Whether the nodal object raised an error calling `run`. (Default
            is False.)
        fully_connected (bool): whether _all_ of the IO (including signals) are
            connected.
        inputs (pyiron_contrib.workflow.io.Inputs): **Abstract.** Children must define
            a property returning an `Inputs` object.
        label (str): A name for the nodal object.
        output (pyiron_contrib.workflow.io.Outputs): **Abstract.** Children must define
            a property returning an `Outputs` object.
        parent (pyiron_contrib.workflow.composite.Composite | None): The parent object
            owning this, if any.
        ready (bool): Whether the inputs are all ready and the nodal object is neither
            already running nor already failed.
        run_on_updates (bool): Whether to run when you are updated and all your input
            is ready and your status does not prohibit running. (Default is False).
        running (bool): Whether the nodal object has called `run` and has not yet
            received output from from this call. (Default is False.)
        server (Optional[pyiron_base.jobs.job.extension.server.generic.Server]): A
            server object for computing things somewhere else. Default (and currently
            _only_) behaviour is to compute things on the main python process owning
            the nodal object.
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
        run: **Abstract.** Do the thing.
        update: **Abstract.** Do the thing if you're ready and you run on updates.
            TODO: Once `run_on_updates` is in this class, we can un-abstract this.
    """

    def __init__(
            self,
            label: str,
            *args,
            parent: Optional[Composite] = None,
            run_on_updates: bool = False,
            **kwargs
    ):
        """
        A mixin class for objects that can form nodes in the graph representation of a
        computational workflow.

        Args:
            label (str): A name for this nodal object.
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
        What the nodal object actually does!
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
        Executes the functionality of the nodal object defined in `on_run`.
        Handles the status of the nodal object, and communicating with any remote
        computing resources.
        """
        if self.running:
            raise RuntimeError(f"{self.label} is already running")

        self.running = True
        self.failed = False

        if self.server is None:
            try:
                run_output = self.on_run(**self.run_args)
            except Exception as e:
                self.running = False
                self.failed = True
                raise e
            self.finish_run(run_output)
        else:
            raise NotImplementedError(
                "We currently only support executing the node functionality right on "
                "the main python process that the node instance lives on. Come back "
                "later for cool new features."
            )
            # TODO: Send the `on_run` callable and the `run_args` data off to remote
            #       resources and register `finish_run` as a callback.

    def finish_run(self, run_output: tuple):
        """
        Process the run result, then wrap up statuses etc.

        By extracting this as a separate method, we allow the node to pass the actual
        execution off to another entity and release the python process to do other
        things. In such a case, this function should be registered as a callback
        so that the node can finish "running" and, e.g. push its data forward when that
        execution is finished.
        """
        try:
            self.process_run_result(run_output)
        except Exception as e:
            self.running = False
            self.failed = True
            raise e

        self.signals.output.ran()
        self.running = False

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
