"""
A base class for objects that can form nodes in the graph representation of a
computational workflow.

The workhorse class for the entire concept.
"""

from __future__ import annotations

import contextlib
import pathlib
import shutil
from abc import ABC, abstractmethod
from concurrent.futures import Future
from importlib import import_module
from typing import TYPE_CHECKING, Any, Literal, cast

from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow import overloading
from pyiron_workflow.channels import (
    AccumulatingInputSignal,
    Channel,
    InputLockedError,
    InputSignal,
    OutputSignal,
)
from pyiron_workflow.draw import Node as GraphvizNode
from pyiron_workflow.executors.wrapped_executorlib import CacheOverride
from pyiron_workflow.io import IO, Inputs, Signals
from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.display_state import HasStateDisplay
from pyiron_workflow.mixin.injection import (
    InjectsOnChannel,
    OutputDataWithInjection,
    OutputsWithInjection,
)
from pyiron_workflow.mixin.lexical import Lexical
from pyiron_workflow.mixin.run import ReadinessError, Runnable
from pyiron_workflow.storage import (
    BackendIdentifier,
    StorageInterface,
    available_backends,
)
from pyiron_workflow.topology import (
    get_nodes_in_data_tree,
    set_run_connections_according_to_linear_dag,
)

if TYPE_CHECKING:
    from pathlib import Path

    import graphviz

    from pyiron_workflow.nodes.composite import Composite


class WaitingForFutureError(ValueError): ...


class AmbiguousOutputError(ValueError):
    """Raised when searching for exactly one output, but multiple are found."""


class ConnectionCopyError(ValueError):
    """Raised when trying to copy IO, but connections cannot be copied"""


class ValueCopyError(ValueError):
    """Raised when trying to copy IO, but values cannot be copied"""


class Node(
    HasStateDisplay,
    Lexical["Composite"],
    Runnable,
    InjectsOnChannel,
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

    This is an abstract class.
    Children *must* define how :attr:`inputs` and :attr:`outputs` are constructed,
    what will happen :meth:`_on_run`, the :attr:`run_args` that will get passed to
    :meth:`_on_run`, and how to :meth:`process_run_result` once :meth:`_on_run` finishes.
    They may optionally add additional signal channels to the signals IO.

    Attributes:
        failed (bool): Whether the node raised an error calling :meth:`run`. (Default
            is False.)
        future (concurrent.futures.Future | None): A futures object, if the node is
            currently running or has already run using an executor.
        label (str): A name for the node.
        parent (pyiron_workflow.composite.Composite | None): The parent object
            owning this, if any.
        recovery: (BackendIdentifier | StorageInterface | None): The storage
            backend to use for saving a "recovery" file if the node execution crashes
            and this is the parent-most node. Default is `"pickle"`, setting `None`
            will prevent any file from being saved.
        running (bool): Whether the node has called :meth:`run` and has not yet
            received output from this call. (Default is False.)
        checkpoint (BackendIdentifier | StorageInterface | None): Whether to trigger a
            save of the entire graph after each run of the node, and if so what storage
            back end to use. (Default is None, don't do any checkpoint saving.)
        use_cache (bool): Whether or not to cache the inputs and, when the current
            inputs match the cached input (by `==` comparison), to bypass running the
            node and simply continue using the existing outputs. Note that you may be
            able to trigger a false cache hit in some special case of non-idempotent
            nodes working on mutable data.
    """

    use_cache = True

    def __init__(
        self,
        *args,
        label: str | None = None,
        parent: Composite | None = None,
        delete_existing_savefiles: bool = False,
        autoload: BackendIdentifier | StorageInterface | None = None,
        autorun: bool = False,
        checkpoint: BackendIdentifier | StorageInterface | None = None,
        **kwargs,
    ):
        """
        A parent class for objects that can form nodes in the graph representation of a
        computational workflow.

        Initialization ends with a routine :meth:`_after_node_setup` that may,
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

        Args:
            label (str): A name for this node.
            *args: Interpreted as node input data, in order of input channels.
            parent: (Composite|None): The composite node that owns this as a child.
            delete_existing_savefiles (bool): Whether to look for and delete any
                matching save files at instantiation. Uses all default storage
                back ends and anything passed to :param:`autoload`. (Default is False,
                leave those files alone!)
            autoload (BackendIdentifier | StorageInterface | None): The back end
                to use for checking whether node data can be loaded from file. A None
                value indicates no auto-loading. (Default is "pickle".)
            autorun (bool): Whether to run at the end of initialization.
            checkpoint (BackendIdentifier | StorageInterface | None): The storage
                back end to use for saving the overall graph at the end of this node's
                run. (Default is None, don't do checkpoint saves.)
            **kwargs: Interpreted as node input data, with keys corresponding to
                channel labels.
        """
        super().__init__(label=label, parent=parent)
        self._validate_ontologies = True  # Back-door to turn off the alpha feature

        self._signals = Signals()
        self._signals.input.run = InputSignal("run", self, self.run)
        self._signals.input.accumulate_and_run = AccumulatingInputSignal(
            "accumulate_and_run", self, self.run
        )
        self._signals.output.ran = OutputSignal("ran", self)
        self._signals.output.failed = OutputSignal("failed", self)

        self.checkpoint = checkpoint
        self.recovery: BackendIdentifier | StorageInterface | None = "pickle"
        self._remove_executorlib_cache: bool = True  # Power-user override for cleaning
        # up temporary serialized results from runs with executorlib; intended to be
        # used for testing
        self._cached_inputs: dict[str, Any] | None = None

        self._user_data: dict[str, Any] = {}
        # A place for power-users to bypass node-injection

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

    def _after_node_setup(
        self,
        *args,
        delete_existing_savefiles: bool = False,
        autoload: BackendIdentifier | StorageInterface | None = None,
        autorun: bool = False,
        **kwargs,
    ):
        if delete_existing_savefiles:
            self.delete_storage(backend=autoload)

        if autoload is not None:
            for backend in available_backends(backend=autoload):
                if backend.has_saved_content(self):
                    logger.info(
                        f"A saved file was found for the node {self.full_label} -- "
                        f"attempting to load it...(To delete the saved file instead, "
                        f"use `delete_existing_savefiles=True`) "
                    )
                    self.load(backend=autoload)
                    break

        self.set_input_values(*args, **kwargs)

        if autorun:
            with contextlib.suppress(ReadinessError):
                self.run()

    @property
    def channel(self) -> OutputDataWithInjection:
        """
        The single output channel. Fulfills the interface expectations for the
        :class:`HasChannel` mixin and allows this object to be used directly for
        forming connections, etc.

        Returns:
            (OutputDataWithInjection): The single output channel.

        Raises:
            AmbiguousOutputError: If there is not exactly one output channel.
        """
        if len(self.outputs) != 1:
            raise AmbiguousOutputError(
                f"Tried to access the channel value of {self.label}, but this is only "
                f"possible when there is a single output channel -- {self.label} has: "
                f"{self.outputs.labels}. Access probably occurred attempting to use "
                f"this object like an output channel, e.g. with injection or to form a "
                f"connection. Either make sure it has exactly one output channel, or "
                f"use the particular channel you want directly."
            )
        else:
            return self.outputs[self.outputs.labels[0]]

    @property
    def graph_path(self) -> str:
        """
        The path of node labels from the graph root (parent-most node in this lexical
        path) down to this node.
        """
        prefix = self.parent.lexical_path if isinstance(self.parent, Node) else ""
        return prefix + self.lexical_delimiter + self.label

    @property
    def graph_root(self) -> Node:
        """The parent-most node in this lexical path."""
        return self.parent.graph_root if isinstance(self.parent, Node) else self

    @property
    @abstractmethod
    def inputs(self) -> Inputs: ...

    @property
    @abstractmethod
    def outputs(self) -> OutputsWithInjection: ...

    @property
    def signals(self) -> Signals:
        """
        A container for input and output signals, which are channels for controlling
        execution flow. By default, has a :attr:`signals.inputs.run` channel which has
        a callback to the :meth:`run` method that fires whenever _any_ of its
        connections sends a signal to it, a :attr:`signals.inputs.accumulate_and_run`
        channel which has a callback to the :meth:`run` method but only fires after
        _all_ its connections send at least one signal to it, and `signals.outputs.ran`
        which gets called when the `run` method is finished.

        Additional signal channels in derived classes can be added to
        :attr:`signals.inputs` and  :attr:`signals.outputs` after this mixin class is
        initialized.
        """
        return self._signals

    @property
    def connected(self) -> bool:
        """Whether _any_ of the IO (including signals) are connected."""
        return self.inputs.connected or self.outputs.connected or self.signals.connected

    @property
    def fully_connected(self) -> bool:
        """Whether _all_ of the IO (including signals) are connected."""
        return (
            self.inputs.fully_connected
            and self.outputs.fully_connected
            and self.signals.fully_connected
        )

    def disconnect(self) -> list[tuple[Channel, Channel]]:
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

    def activate_strict_hints(self) -> None:
        """Enable type hint checks for all data IO"""
        self.inputs.activate_strict_hints()
        self.outputs.activate_strict_hints()

    def deactivate_strict_hints(self) -> None:
        """Disable type hint checks for all data IO"""
        self.inputs.deactivate_strict_hints()
        self.outputs.deactivate_strict_hints()

    def _connect_output_signal(self, signal: OutputSignal) -> None:
        self.signals.input.run.connect(signal)

    def __rshift__(self, other: InputSignal | Node) -> InputSignal | Node:
        """
        Allows users to connect run and ran signals like: `first >> second`.
        """
        other._connect_output_signal(self.signals.output.ran)
        return other

    def _connect_accumulating_input_signal(
        self, signal: AccumulatingInputSignal
    ) -> None:
        self.signals.output.ran.connect(signal)

    def __lshift__(self, others: tuple[OutputSignal | Node, ...]):
        """
        Connect one or more `ran` signals to `accumulate_and_run` signals like:
        `this << some_object, another_object, or_by_channel.signals.output.ran`
        """
        self.signals.input.accumulate_and_run << others

    def _get_complete_input(self, *args, **kwargs) -> dict[str, Any]:
        if len(args) > len(self.inputs.labels):
            raise ValueError(
                f"Received {len(args)} args, but only have {len(self.inputs.labels)} "
                f"input channels available"
            )
        keyed_args = dict(zip(self.inputs.labels, args, strict=False))

        if len(set(keyed_args.keys()).intersection(kwargs.keys())) > 0:
            raise ValueError(
                f"n args are interpreted using the first n input channels "
                f"({self.inputs.labels}), but this conflicted with received kwargs "
                f"({list(kwargs.keys())}) -- perhaps the input was ordered differently "
                f"than expected? Got args {args} and kwargs {kwargs}."
            )

        kwargs.update(keyed_args)

        self._ensure_all_input_keys_present(kwargs.keys(), self.inputs.labels)

        return kwargs

    def set_input_values(self, *args, **kwargs) -> None:
        """
        Match keywords to input channels and update their values.

        Throws a warning if a keyword is provided that cannot be found among the input
        keys.

        Args:
            *args: values assigned to inputs in order of appearance.
            **kwargs: input key - input value (including channels for connection) pairs.

        Raises:
            (ValueError): If more args are received than there are inputs available.
            (ValueError): If there is any overlap between channels receiving values
                from `args` and those from `kwargs`.
            (ValueError): If any of the `kwargs` keys do not match available input
                labels.
        """
        for k, v in self._get_complete_input(*args, **kwargs).items():
            self.inputs[k] = v

    @staticmethod
    def _ensure_all_input_keys_present(used_keys, available_keys):
        diff = set(used_keys).difference(available_keys)
        if len(diff) > 0:
            raise ValueError(
                f"{diff} not found among available inputs: {available_keys}"
            )

    @property
    def _owned_io_panels(self) -> list[IO]:
        return [
            self.inputs,
            self.outputs,
            self.signals.input,
            self.signals.output,
        ]

    def data_input_locked(self):
        return self.running

    @property
    def _readiness_dict(self) -> dict[str, bool]:
        dict = super()._readiness_dict
        for k, v in self.inputs.items():
            dict[f"inputs.{k}"] = v.ready
        return dict

    @property
    def _readiness_error_message(self) -> str:
        return (
            f"{self.label} received a run command but is not ready. The node "
            f"should be neither running nor failed, and all input values should"
            f" conform to type hints.\n" + self.readiness_report
        )

    def _is_using_wrapped_excutorlib_executor(self) -> bool:
        return self.executor is not None and (
            isinstance(self.executor, CacheOverride)
            or (
                isinstance(self.executor, tuple)
                and isinstance(self.executor[0], type)
                and issubclass(self.executor[0], CacheOverride)
            )
        )

    def _clean_wrapped_executorlib_executor_cache(self) -> None:
        self._wrapped_executorlib_cache_file.unlink()
        cache_subdir = self.as_path() / CacheOverride.override_cache_file_name
        if pathlib.Path(cache_subdir).is_dir():
            shutil.rmtree(cache_subdir)
        self.clean_path()

    @property
    def _wrapped_executorlib_cache_file(self) -> Path:
        """For internal use to clean up cached executorlib files"""
        # Depends on executorlib implementation details not protected by semver
        file_name = CacheOverride.override_cache_file_name + "_o.h5"
        return self.as_path() / file_name

    def on_run(self, *args, **kwargs) -> Any:
        return self._on_run(*args, **kwargs)

    @abstractmethod
    def _on_run(self, *args, **kwargs) -> Any: ...

    def run(
        self,
        *args,
        run_data_tree: bool = False,
        run_parent_trees_too: bool = False,
        fetch_input: bool = True,
        check_readiness: bool = True,
        raise_run_exceptions: bool = True,
        rerun: bool = False,
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
            raise_run_exceptions (bool): Whether to raise exceptions encountered during
                the run, or just ignore them. (Default is True, raise them!)
            rerun (bool): Whether to force-set :attr:`running` and :attr:`failed` to
                `False` before running. (Default is False.)
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

        return super().run(
            check_readiness=check_readiness,
            raise_run_exceptions=raise_run_exceptions,
            rerun=rerun,
            before_run_kwargs={
                "run_data_tree": run_data_tree,
                "run_parent_trees_too": run_parent_trees_too,
                "fetch_input": fetch_input,
                "emit_ran_signal": emit_ran_signal,
                "input_args": args,
                "input_kwargs": kwargs,
            },
            run_finally_kwargs={
                "emit_ran_signal": emit_ran_signal,
                "raise_run_exceptions": raise_run_exceptions,
            },
        )

    def _before_run(
        self,
        /,
        check_readiness: bool,
        rerun: bool,
        run_data_tree: bool,
        run_parent_trees_too: bool,
        fetch_input: bool,
        emit_ran_signal: bool,
        input_args: tuple[Any, ...],
        input_kwargs: dict[str, Any],
    ) -> tuple[bool, Any]:
        if self.running:
            if len(input_args) > 0 or len(input_kwargs) > 0:
                raise InputLockedError(
                    f"Node {self.label} is running. Input values are not allowed to be "
                    f"updated in this state, but got args {input_args} and kwargs "
                    f"{input_kwargs}."
                )
            if self.future is not None:
                if rerun:
                    raise WaitingForFutureError(
                        f"Node {self.label} is running and has a future attached to "
                        f"it. It cannot be rerun in this state."
                    )
                else:
                    return True, self.future
            if self._is_using_wrapped_excutorlib_executor():
                return False, None  # Let it cook
            elif not rerun:
                raise ReadinessError(self._readiness_error_message)

        if self.failed and check_readiness and not rerun:
            raise ReadinessError(self._readiness_error_message)

        if run_data_tree:
            self.run_data_tree(run_parent_trees_too=run_parent_trees_too)

        self.set_input_values(*input_args, **input_kwargs)
        if fetch_input:
            self.inputs.fetch()

        if self.use_cache and self.cache_hit:  # Read and use cache
            self._on_cache_hit()
            if (self.parent is None or not self.parent.running) and emit_ran_signal:
                self.emit()
            elif self.parent is not None and self.parent.running:
                self.parent.register_child_starting(self)
                self.parent.register_child_finished(self)
                if emit_ran_signal:
                    self.parent.register_child_emitting(self)
            return True, self._outputs_to_run_return()
        else:
            self._on_cache_miss()
            if self.use_cache:  # Write cache and continue
                self._cache_inputs()

        return super()._before_run(check_readiness=check_readiness, rerun=rerun)

    def _on_cache_hit(self) -> None:
        """A hook for subclasses to act on cache hits"""
        return

    def _on_cache_miss(self) -> None:
        """A hook for subclasses to act on cache misses"""
        return

    def _cache_inputs(self):
        self._cached_inputs = self.inputs.to_value_dict()

    def clear_cache(self):
        self._cached_inputs = None

    def _run(
        self,
        raise_run_exceptions: bool,
        run_exception_kwargs: dict,
        run_finally_kwargs: dict,
        finish_run_kwargs: dict,
    ) -> Any | tuple | Future:
        if self.parent is not None and self.parent.running:
            self.parent.register_child_starting(self)
        return super()._run(
            raise_run_exceptions=raise_run_exceptions,
            run_exception_kwargs=run_exception_kwargs,
            run_finally_kwargs=run_finally_kwargs,
            finish_run_kwargs=finish_run_kwargs,
        )

    def _run_finally(self, /, emit_ran_signal: bool, raise_run_exceptions: bool):
        super()._run_finally()
        if self.parent is not None and self.parent.running:
            self.parent.register_child_finished(self)
        if self.checkpoint is not None:
            self.save_checkpoint(self.checkpoint)

        if emit_ran_signal:
            if self.parent is None or not self.parent.running:
                self.emit()
            else:
                self.parent.register_child_emitting(self)

        if (
            self.failed
            and raise_run_exceptions
            and self.recovery is not None
            and self.graph_root is self
        ):
            self.save(
                backend=self.recovery, filename=self.as_path().joinpath("recovery")
            )

        if (
            self._remove_executorlib_cache
            and self._is_using_wrapped_excutorlib_executor()
        ):
            self._clean_wrapped_executorlib_executor_cache()

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

        data_tree_nodes = get_nodes_in_data_tree(self)
        # If we have a parent, delegate to it
        if self.parent is not None:
            self.parent.run_data_tree_for_child(self)
            return

        # The rest of this method handles the case when self.parent is None
        label_map = {}
        nodes = {}

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
            if len(data_tree_starters) > 1 or data_tree_starters[0] is not self:
                self.signals.disconnect_run()
                # Don't let anything upstream trigger _this_ node

                for starter in data_tree_starters:
                    starter.run()  # Now push from the top
            # Otherwise the requested node is the only one in the data tree, so there's
            # nothing upstream to run.
        finally:
            # No matter what, restore the original connections and labels afterwards
            for modified_label, node in nodes.items():
                node.label = label_map[modified_label]
                node.signals.disconnect_run()
            for c1, c2 in disconnected_pairs:
                c1.connect(c2)

    @property
    def cache_hit(self) -> bool:
        try:
            return self.inputs.to_value_dict() == self._cached_inputs
        except Exception:
            return False

    def _outputs_to_run_return(self):
        return DotDict(self.outputs.to_value_dict())

    @property
    def emitting_channels(self) -> tuple[OutputSignal, ...]:
        if self.failed:
            return (self.signals.output.failed,)
        else:
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
            rerun=False,
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
            rerun=False,
            emit_ran_signal=False,
            **kwargs,
        )

    def push(self, *args, **kwargs):
        """
        Exactly like :meth:`run` with all the same flags, _except_ it handles an edge
        case where you are trying to directly run the child node of a
        :class:`pyiron_workflow.workflow.Workflow` before it has had any chance to
        configure its execution signals.
        _If_ the parent is a workflow set up to automate execution flow, does that
        _first_ then runs as usual.
        """
        # Alright, time for more egregious hacking
        # Normally, running will work in a push-like mode _BUT_, because Workflow's are
        # a flexible dynamic thing, they normally construct their execution signals on
        # the fly at each run invocation. This is not the case for Macros, where the
        # run configuration, if automated at all, happens right at macro instantiation.
        # So there's this horrible edge case where you build a workflow, then
        # immediately try to run one of its children directly, naively expecting that
        # the run will push downstream executions like it does in a macro -- except it
        # _doesn't_ because there are _no signal connections at all yet!_
        # Building these on _every_ run would be needlessly expensive, so this method
        # exists as a hacky guaranteed way to secure push-like run behaviour regardless
        # of the context you're calling from.
        if self.parent is not None:
            return self.parent.push_child(self, *args, **kwargs)
        else:
            return self.run(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> None:
        """
        A shortcut for :meth:`pull` that automatically runs the entire set of upstream data
        dependencies all the way to the parent-most graph object.
        """
        return self.pull(*args, run_parent_trees_too=True, **kwargs)

    @property
    def ready(self) -> bool:
        """
        Whether the inputs are all ready and the node is neither already running nor
        already failed.
        """
        return super().ready and self.inputs.ready

    @property
    def color(self) -> str:
        """A hex code color for use in drawing."""
        return SeabornColors.white

    def draw(
        self,
        depth: int = 1,
        rankdir: Literal["LR", "TB"] = "LR",
        size: tuple | None = None,
        save: bool = False,
        view: bool = False,
        directory: Path | str | None = None,
        filename: Path | str | None = None,
        format: str | None = None,
        cleanup: bool = True,
    ) -> graphviz.graphs.Digraph:
        """
        Draw the node structure and return it as a graphviz object.

        A selection of the :func:`graphviz.Graph.render` method options are exposed,
        and if :param:`view` or :param:`filename` is provided, this will be called
        before returning the graph.
        The graph file and rendered image will be stored in a directory based of the
        node's lexical path, unless a :param:`directory` is explicitly set.
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
        size_str = f"{size[0]},{size[1]}" if size is not None else None
        graph = GraphvizNode(self, depth=depth, rankdir=rankdir, size=size_str).graph
        if save or view or filename is not None:
            directory = self.as_path() if directory is None else Path(directory)
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

    def save(
        self,
        backend: BackendIdentifier | StorageInterface = "pickle",
        filename: str | Path | None = None,
        **kwargs,
    ):
        """
        Writes the node to file using the requested interface as a back end.

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            filename (str | Path | None): The name of the file (without extensions) at
                which to save the node. (Default is None, which uses the node's
                lexical path.)
            **kwargs: Back end-specific keyword arguments.
        """
        for selected_backend in available_backends(
            backend=backend, only_requested=True
        ):
            selected_backend.save(node=self, filename=filename, **kwargs)

    save.__doc__ = cast(str, save.__doc__) + _save_load_warnings

    def save_checkpoint(self, backend: BackendIdentifier | StorageInterface = "pickle"):
        """
        Triggers a save on the parent-most node.

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
        """
        self.graph_root.save(backend=backend)

    @classmethod
    def _new_instance_from_storage(
        cls,
        backend: BackendIdentifier | StorageInterface = "pickle",
        only_requested=False,
        filename: str | Path | None = None,
        _node: Node | None = None,
        **kwargs,
    ):
        """
        Loads a node from file returns its instance.

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            only_requested (bool): Whether to _only_ try loading from the specified
                backend, or to loop through all available backends. (Default is False,
                try to load whatever you can find.)
            filename (str | Path | None): The name of the file (without extensions)
                from which to load the node. (Default is None, which uses the node's
                lexical path.)
            **kwargs: back end-specific arguments (only likely to work in combination
                with :param:`only_requested`, otherwise there's nothing to be specific
                _to_.)

        Raises:
            FileNotFoundError: when nothing got loaded.
        """
        inst = None
        for selected_backend in available_backends(
            backend=backend, only_requested=only_requested
        ):
            inst = selected_backend.load(node=_node, filename=filename, **kwargs)
            if inst is not None:
                break
        if inst is None:
            raise FileNotFoundError(
                f"Could not find saved content at {filename} using backend={backend} "
                f"using only_request={only_requested}."
            )
        return inst

    def _update_instance_from_storage(
        self,
        backend: BackendIdentifier | StorageInterface = "pickle",
        only_requested=False,
        filename: str | Path | None = None,
        **kwargs,
    ):
        """
        Loads the node file and set the loaded state as the node's own.

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            only_requested (bool): Whether to _only_ try loading from the specified
                backend, or to loop through all available backends. (Default is False,
                try to load whatever you can find.)
            filename (str | Path | None): The name of the file (without extensions)
                from which to load the node. (Default is None, which uses the node's
                lexical path.)
            **kwargs: back end-specific arguments (only likely to work in combination
                with :param:`only_requested`, otherwise there's nothing to be specific
                _to_.)

        Raises:
            FileNotFoundError: when nothing got loaded.
            TypeError: when the saved node has a different class name.
        """
        if self.running:
            raise ValueError(
                "Cannot load a node while it is running. If you are sure loading now "
                "is the correct thing to do, you can set `self.running=True` where "
                "`self` is this node object."
            )
        inst = self.__class__._new_instance_from_storage(
            backend=backend,
            only_requested=only_requested,
            filename=filename,
            _node=self if filename is None else None,
            **kwargs,
        )

        if inst.__class__ != self.__class__:
            raise TypeError(
                f"{self.label} cannot load, as it has type "
                f"{self.__class__.__name__},  but the saved node has type "
                f"{inst.__class__.__name__}"
            )
        self.__setstate__(inst.__getstate__())

    @overloading.overloaded_classmethod(class_method=_new_instance_from_storage)
    def load(
        self,
        backend: BackendIdentifier | StorageInterface = "pickle",
        only_requested=False,
        filename: str | Path | None = None,
        **kwargs,
    ):
        """
        Load a node from storage, either as a new instance (when used as a class
        method) or by updating the current instance (when called as a regular instance
        method).

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            only_requested (bool): Whether to _only_ try loading from the specified
                backend, or to loop through all available backends. (Default is False,
                try to load whatever you can find.)
            filename (str | Path | None): The name of the file (without extensions)
                from which to load the node. (Default is None, which uses the node's
                lexical path.)
            **kwargs: back end-specific arguments (only likely to work in combination
                with :param:`only_requested`, otherwise there's nothing to be specific
                _to_.)

        Raises:
            FileNotFoundError: when nothing got loaded.
            TypeError: when loading into an exisiting instance and the saved node has a
                different class name.
        """
        return self._update_instance_from_storage(
            backend=backend,
            only_requested=only_requested,
            filename=filename,
            **kwargs,
        )

    load.__doc__ = cast(str, load.__doc__) + _save_load_warnings

    def delete_storage(
        self,
        backend: BackendIdentifier | StorageInterface | None = None,
        only_requested: bool = False,
        filename: str | Path | None = None,
        *,
        delete_even_if_not_empty: bool = False,
        **kwargs,
    ):
        """
        Remove save file(s).

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            only_requested (bool): Whether to _only_ search for files using the
                specifiedmbackend, or to loop through all available backends. (Default
                is False, try to remove whatever you can find.)
            filename (str | Path | None): The name of the file (without extensions) to
                remove. (Default is None, which uses the node's lexical path.)
            delete_even_if_not_empty (bool): Whether to delete the file even if it is
                not empty. (Default is False, which will only delete the file if it is
                empty, i.e. has no content in it.)
            **kwargs: back end-specific arguments (only likely to work in combination
                with :param:`only_requested`, otherwise there's nothing to be specific
                _to_.)
        """
        for selected_backend in available_backends(
            backend=backend, only_requested=only_requested
        ):
            selected_backend.delete(
                node=self if filename is None else None,
                filename=filename,
                delete_even_if_not_empty=delete_even_if_not_empty,
                **kwargs,
            )

    def has_saved_content(
        self,
        backend: BackendIdentifier | StorageInterface | None = None,
        only_requested: bool = False,
        filename: str | Path | None = None,
        **kwargs,
    ):
        """
        Whether any save files can be found at the canonical location for this node.

        Args:
            backend (str | StorageInterface): The interface to use for serializing the
                node. (Default is "pickle", which loads the standard pickling back end.)
            only_requested (bool): Whether to _only_ search for files using the
                specified backend, or to loop through all available backends. (Default
                is False, try to finding whatever you can find.)
            filename (str | Path | None): The name of the file (without extensions) to
                look for. (Default is None, which uses the node's lexical path.)
            **kwargs: back end-specific arguments (only likely to work in combination
                with :param:`only_requested`, otherwise there's nothing to be specific
                _to_.)

        Returns:
            bool: Whether any save files were found
        """
        return any(
            be.has_saved_content(
                node=self if filename is None else None, filename=filename, **kwargs
            )
            for be in available_backends(backend=backend, only_requested=only_requested)
        )

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

    def display_state(self, state=None, ignore_private=True):
        state = dict(self.__getstate__()) if state is None else state
        if self.parent is not None:
            state["parent"] = self.parent.full_label
        if len(state["_user_data"]) > 0:
            self._make_entry_public(state, "_user_data", "user_data")
        return super().display_state(state=state, ignore_private=ignore_private)

    @classmethod
    def _extra_info(cls) -> str:
        """
        Any additional info that may be particularly useful for users of the node class.
        """
        return ""
