"""
A base class for macro nodes, which are composite like workflows but have a static
interface and are not intended to be internally modified after instantiation.
"""

from __future__ import annotations

from abc import ABC
import inspect
from typing import Any, get_args, get_type_hints, Literal, Optional, TYPE_CHECKING
import warnings

from pyiron_workflow.channels import InputData, OutputData, NOT_DATA
from pyiron_workflow.composite import Composite
from pyiron_workflow.create import HasCreator
from pyiron_workflow.has_interface_mixins import HasChannel
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.output_parser import ParseOutput

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel


class AbstractMacro(Composite, ABC):
    """
    A macro is a composite node that holds a graph with a fixed interface, like a
    pre-populated workflow that is the same every time you instantiate it.

    At instantiation, the macro uses a provided callable to build and wire the graph,
    then builds a static IO interface for this graph.
    This callable must use the macro object itself as the first argument (e.g. adding
    nodes to it).
    As with :class:`Workflow` objects, macros leverage `inputs_map` and `outputs_map` to
    control macro-level IO access to child IO.
    As with :class:`Workflow`, default behaviour is to expose all unconnected child IO.
    The provided callable may optionally specify further args and kwargs, which are used
    to pre-populate the macro with :class:`UserInput` nodes;
    This can be especially helpful when more than one child node needs access to the
    same input value.
    Similarly, the callable may return any number of child nodes' output channels (or
    the node itself in the case of single-output nodes) as long as a commensurate
    number of labels for these outputs were provided to the class constructor.
    These function-like definitions of the graph creator callable can be used
    to build only input xor output, or both together.
    Each that is used switches its IO map to a "whitelist" paradigm, so any I/O _not_
    provided in the callable signature/return values and output labels will be disabled.
    Manual modifications of the IO maps inside the callable always take priority over
    this whitelisting behaviour, so you always retain full control over what IO is
    exposed, and the whitelisting is only for your convenience.

    Macro IO is _value linked_ to the child IO, so that their values stay synchronized,
    but the child nodes of a macro form an isolated sub-graph.

    As with function nodes, subclasses of :class:`Macro` may define a method for creating the
    graph.

    As with :class:`Workflow``, all DAG macros can determine their execution flow
    automatically, if you have cycles in your data flow, or otherwise want more control
    over the execution, all you need to do is specify the `node.signals.input.run`
    connections and :attr:`starting_nodes` list yourself.
    If only _one_ of these is specified, you'll get an error, but if you've provided
    both then no further checks of their validity/reasonableness are performed, so be
    careful.
    Unlike :class:`Workflow`, this execution flow automation is set up once at instantiation;
    If the macro is modified post-facto, you may need to manually re-invoke
    :meth:`configure_graph_execution`.

    Promises (in addition parent class promises):

    - IO is...
        - Only built at instantiation, after child node replacement, or at request, so
            it is "static" for improved efficiency
        - By value, i.e. the macro has its own IO channel instances and children are
            duly encapsulated inside their own sub-graph
        - Value-linked to the values of their corresponding child nodes' IO -- i.e.
            updating a macro input value changes a child node's input value, and a
            child node updating its output value changes a macro output value (if that
            child's output is regularly included in the macro's output, e.g. because it
            is disconnected or otherwise included in the outputs map)
    - Macros will attempt to set the execution graph automatically for DAGs, as long as
        no execution flow is set in the function that builds the sub-graph
    - A default node label can be generated using the name of the callable that builds
        the graph.

    Examples:
        Let's consider the simplest case of macros that just consecutively add 1 to
        their input:

        >>> from pyiron_workflow.macro import Macro
        >>>
        >>> def add_one(x):
        ...     result = x + 1
        ...     return result
        >>>
        >>> def add_three_macro(macro, one__x):
        ...     macro.one = macro.create.Function(add_one, x=one__x)
        ...     macro.two = macro.create.Function(add_one, macro.one)
        ...     macro.three = macro.create.Function(add_one, macro.two)
        ...     macro.one >> macro.two >> macro.three
        ...     macro.starting_nodes = [macro.one]
        ...     return macro.three

        In this case we had _no need_ to specify the execution order and starting nodes
        --it's just an extremely simple DAG after all! -- but it's done here to
        demonstrate the syntax.

        We can make a macro by passing this graph-building function (that takes a macro
        as its first argument, i.e. `self` from the macro's perspective) to the :class:`Macro`
        class. Then, we can use it like a regular node! Just like a workflow, the
        io is constructed from unconnected owned-node IO by combining node and channel
        labels.

        >>> macro = Macro(add_three_macro, output_labels="three__result")
        >>> out = macro(one__x=3)
        >>> out.three__result
        6

        We can also nest macros, rename their IO, and provide access to
        internally-connected IO by inputs and outputs maps:

        >>> def nested_macro(macro, inp):
        ...     macro.a = macro.create.Function(add_one, x=inp)
        ...     macro.b = macro.create.Macro(
        ...         add_three_macro, one__x=macro.a, output_labels="three__result"
        ...     )
        ...     macro.c = macro.create.Function(add_one, x=macro.b)
        ...     return macro.c, macro.b
        >>>
        >>> macro = Macro(
        ...     nested_macro, output_labels=("out", "intermediate")
        ... )
        >>> macro(inp=1)
        {'out': 6, 'intermediate': 5}

        Macros and workflows automatically generate execution flows when their data
        is acyclic.
        Let's build a simple macro with two independent tracks:

        >>> def modified_flow_macro(macro, a__x=0, b__x=0):
        ...     macro.a = macro.create.Function(add_one, x=a__x)
        ...     macro.b = macro.create.Function(add_one, x=b__x)
        ...     macro.c = macro.create.Function(add_one, x=macro.b)
        ...     return macro.a, macro.c
        >>>
        >>> m = Macro(modified_flow_macro, output_labels=("a", "c"))
        >>> m(a__x=1, b__x=2)
        {'a': 2, 'c': 4}

        We can override which nodes get used to start by specifying the
        :attr:`starting_nodes` property and (if necessary) reconfiguring the execution
        signals.
        Care should be taken here, as macro nodes may be creating extra input
        nodes that need to be considered.
        It's advisable to use :meth:`draw()` or to otherwise inspect the macro's
        children and their connections before manually updating execution flows.

        Let's use this and then observe how the `a` sub-node no longer gets run:

        >>> _ = m.disconnect_run()
        >>> m.starting_nodes = [m.b__x]
        >>> _ = m.b__x >> m.b >> m.c
        >>> m(a__x=1000, b__x=2000)
        {'a': 2, 'c': 2002}

        (The `_` is just to catch and ignore output for the doctest, you don't
        typically need this.)

        Note how the `a` node is no longer getting run, so the output is not updated!
        Manually controlling execution flow is necessary for cyclic graphs (cf. the
        while loop meta-node), but best to avoid when possible as it's easy to miss
        intended connections in complex graphs.

        If there's a particular macro we're going to use again and again, we might want
        to consider making a new class for it using the decorator, just like we do for
        function nodes:

        >>> @Macro.wrap_as.macro_node("three__result")
        ... def AddThreeMacro(macro, one__x):
        ...     add_three_macro(macro, one__x=one__x)
        ...     # We could also simply have decorated that function to begin with
        ...     return macro.three
        >>>
        >>> macro = AddThreeMacro()
        >>> macro(one__x=0).three__result
        3

        Alternatively (and not recommended) is to make a new child class of
        :class:`AbstractMacro` that overrides the :meth:`graph_creator` arg such that
        the same graph is always created.

        >>> from pyiron_workflow.macro import AbstractMacro
        >>> class AddThreeMacro(AbstractMacro):
        ...     _provided_output_labels = ["three__result"]
        ...
        ...     @staticmethod
        ...     def graph_creator(macro, one__x):
        ...         add_three_macro(macro, one__x=one__x)
        ...         return macro.three
        >>>
        >>> macro = AddThreeMacro()
        >>> macro(one__x=0).three__result
        3

        Notice here that we're inheriting from `AbstractMacro` and not just
        `Macro` we were using before. Under the hood, `Macro` is actually a
        very minimal class that is _dynamically_ creating a new child of
        `AbstractMacro` that uses the provided `graph_creator` and returning you an
        instance of this new dynamic class! So you can't inherit from it directly.
        Anyhow, it is recommended to use the decorator on a function rather than direct
        inheritance.

        We can also modify an existing macro at runtime by replacing nodes within it, as
        long as the replacement has fully compatible IO. There are three syntacic ways
        to do this. Let's explore these by going back to our `add_three_macro` and
        replacing each of its children with a node that adds 2 instead of 1.

        >>> @Macro.wrap_as.function_node()
        ... def add_two(x):
        ...     result = x + 2
        ...     return result
        >>>
        >>> adds_six_macro = Macro(add_three_macro, output_labels="three__result")
        >>> # With the replace method
        >>> # (replacement target can be specified by label or instance,
        >>> # the replacing node can be specified by instance or class)
        >>> replaced = adds_six_macro.replace_child(adds_six_macro.one, add_two())
        >>> # With the replace_with method
        >>> adds_six_macro.two.replace_with(add_two())
        >>> # And by assignment of a compatible class to an occupied node label
        >>> adds_six_macro.three = add_two
        >>> adds_six_macro(one__x=1)
        {'three__result': 7}

        It's possible for the macro to hold nodes which are not publicly exposed for
        data and signal connections, but which will still internally execute and store
        data, e.g.:

        >>> @Macro.wrap_as.macro_node("lout", "n_plus_2")
        ... def LikeAFunction(macro, lin: list,  n: int = 1):
        ...     macro.plus_two = n + 2
        ...     macro.sliced_list = lin[n:macro.plus_two]
        ...     macro.double_fork = 2 * n
        ...     return macro.sliced_list, macro.plus_two.channel
        >>>
        >>> like_functions = LikeAFunction(lin=[1,2,3,4,5,6], n=3)
        >>> sorted(like_functions().items())
        [('lout', [4, 5]), ('n_plus_2', 5)]

        >>> like_functions.double_fork.value
        6


    """

    _provided_output_labels: tuple[str] | None = None

    def __init__(
        self,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        strict_naming: bool = True,
        **kwargs,
    ):
        super().__init__(
            label=label if label is not None else self.graph_creator.__name__,
            parent=parent,
            save_after_run=save_after_run,
            storage_backend=storage_backend,
            strict_naming=strict_naming,
        )

        ui_nodes = self._prepopulate_ui_nodes_from_graph_creator_signature(
            storage_backend=storage_backend
        )
        returned_has_channel_objects = self.graph_creator(self, *ui_nodes)
        if returned_has_channel_objects is None:
            returned_has_channel_objects = ()
        elif isinstance(returned_has_channel_objects, HasChannel):
            returned_has_channel_objects = (returned_has_channel_objects,)
        self._configure_graph_execution(ui_nodes)
        self._inputs = Inputs(
            *(
                self._get_linking_channel(n.inputs.user_input, n.label)
                for n in ui_nodes
            )
        )

        self._outputs = Outputs(
            *(
                self._get_linking_channel(c.channel, label)
                for (c, label) in zip(
                    () if returned_has_channel_objects is None
                    else returned_has_channel_objects,
                    () if self._provided_output_labels is None
                    else self._provided_output_labels
                )
            )
        )

        self.set_input_values(**kwargs)

    @classmethod
    def _validate_output_labels(cls) -> tuple[str]:
        """
        Ensure that output_labels, if provided, are commensurate with graph creator
        return values, if provided, and return them as a tuple.
        """
        graph_creator_returns = ParseOutput(cls.graph_creator).output
        output_labels = cls._provided_output_labels
        if graph_creator_returns is not None or output_labels is not None:
            error_suffix = (
                f"but {cls.__name__} macro class got return values: "
                f"{graph_creator_returns} and labels: {output_labels}."
            )
            try:
                if len(output_labels) != len(graph_creator_returns):
                    raise ValueError(
                        "The number of return values in the graph creator must exactly "
                        "match the number of output labels provided, " + error_suffix
                    )
            except TypeError:
                raise TypeError(
                    f"Output labels and graph creator return values must either both "
                    f"or neither be present, " + error_suffix
                )

    @classmethod
    def _type_hints(cls):
        """The result of :func:`typing.get_type_hints` on the :meth:`graph_creator`."""
        return get_type_hints(cls.graph_creator)

    @classmethod
    def preview_output_channels(cls) -> dict[str, Any]:
        """
        Gives a class-level peek at the expected output channels.

        Returns:
            dict[str, tuple[Any, Any]]: The channel name and its corresponding type
                hint.
        """
        labels = cls._get_output_labels()
        try:
            type_hints = cls._type_hints()["return"]
            if len(labels) > 1:
                type_hints = get_args(type_hints)
                if not isinstance(type_hints, tuple):
                    raise TypeError(
                        f"With multiple return labels expected to get a tuple of type "
                        f"hints, but got type {type(type_hints)}"
                    )
                if len(type_hints) != len(labels):
                    raise ValueError(
                        f"Expected type hints and return labels to have matching "
                        f"lengths, but got {len(type_hints)} hints and "
                        f"{len(labels)} labels: {type_hints}, {labels}"
                    )
            else:
                # If there's only one hint, wrap it in a tuple, so we can zip it with
                # *return_labels and iterate over both at once
                type_hints = (type_hints,)
        except KeyError:  # If there are no return hints
            type_hints = [None] * len(labels)
            # Note that this nicely differs from `NoneType`, which is the hint when
            # `None` is actually the hint!
        return {label: hint for label, hint in zip(labels, type_hints)}

    @classmethod
    def _get_output_labels(cls):
        """
        Return output labels provided on the class if not None.
        """
        return cls._provided_output_labels

    @classmethod
    def preview_input_channels(cls) -> dict[str, tuple[Any, Any]]:
        """
        Gives a class-level peek at the expected input channels.

        Returns:
            dict[str, tuple[Any, Any]]: The channel name and a tuple of its
                corresponding type hint and default value.
        """
        type_hints = cls._type_hints()
        scraped: dict[str, tuple[Any, Any]] = {}
        for i, (label, value) in enumerate(cls._input_args().items()):
            if i == 0:
                continue  # Skip the macro argument itself, it's like `self` here
            elif label in cls._init_keywords():
                # We allow users to parse arbitrary kwargs as channel initialization
                # So don't let them choose bad channel names
                raise ValueError(
                    f"The Input channel name {label} is not valid. Please choose a "
                    f"name _not_ among {cls._init_keywords()}"
                )

            try:
                type_hint = type_hints[label]
            except KeyError:
                type_hint = None

            default = (
                value.default if value.default is not inspect.Parameter.empty
                else NOT_DATA
            )

            scraped[label] = (type_hint, default)
        return scraped

    @classmethod
    def _input_args(cls):
        return inspect.signature(cls.graph_creator).parameters

    @classmethod
    def _init_keywords(cls):
        return list(inspect.signature(cls.__init__).parameters.keys())

    def _prepopulate_ui_nodes_from_graph_creator_signature(
        self, storage_backend: Literal["h5io", "tinybase"]
    ):
        return tuple(
            self.create.standard.UserInput(
                default,
                label=label,
                parent=self,
                type_hint=type_hint,
                storage_backend=storage_backend,
            )
            for label, (type_hint, default) in self.preview_input_channels().items()
        )

    def _get_linking_channel(
        self,
        child_reference_channel: InputData | OutputData,
        composite_io_key: str,
    ) -> InputData | OutputData:
        """
        Build IO by value: create a new channel just like the child's channel.

        In the case of input data, we also form a value link from the composite channel
        down to the child channel, so that the child will stay up-to-date.
        """
        composite_channel = child_reference_channel.__class__(
            label=composite_io_key,
            owner=self,
            default=child_reference_channel.default,
            type_hint=child_reference_channel.type_hint,
        )
        composite_channel.value = child_reference_channel.value

        if isinstance(composite_channel, InputData):
            composite_channel.strict_hints = child_reference_channel.strict_hints
            composite_channel.value_receiver = child_reference_channel
        elif isinstance(composite_channel, OutputData):
            child_reference_channel.value_receiver = composite_channel
        else:
            raise TypeError(
                "This should not be an accessible state, please contact the developers"
            )

        return composite_channel

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> Outputs:
        return self._outputs

    def _parse_remotely_executed_self(self, other_self):
        local_connection_data = [
            [(c, c.label, c.connections) for c in io_panel]
            for io_panel in [
                self.inputs,
                self.outputs,
                self.signals.input,
                self.signals.output,
            ]
        ]
        super()._parse_remotely_executed_self(other_self)

        for old_data, io_panel in zip(
            local_connection_data,
            [self.inputs, self.outputs, self.signals.input, self.signals.output],
            # Get fresh copies of the IO panels post-update
        ):
            for original_channel, label, connections in old_data:
                new_channel = io_panel[label]  # Fetch it from the fresh IO panel
                new_channel.connections = connections
                for other_channel in connections:
                    self._replace_connection(
                        other_channel, original_channel, new_channel
                    )

    @staticmethod
    def _replace_connection(
        channel: Channel, old_connection: Channel, new_connection: Channel
    ):
        """Brute-force replace an old connection in a channel with a new one"""
        channel.connections = [
            c if c is not old_connection else new_connection for c in channel
        ]

    def _configure_graph_execution(self, ui_nodes):
        run_signals = self.disconnect_run()

        has_signals = len(run_signals) > 0
        has_starters = len(self.starting_nodes) > 0

        if has_signals and has_starters:
            # Assume the user knows what they're doing
            self._reconnect_run(run_signals)
            # Then put the UI upstream of the original starting nodes
            for n in self.starting_nodes:
                n << ui_nodes
            self.starting_nodes = ui_nodes
        elif not has_signals and not has_starters:
            # Automate construction of the execution graph
            self.set_run_signals_to_dag_execution()
        else:
            raise ValueError(
                f"The macro '{self.label}' has {len(run_signals)} run signals "
                f"internally and {len(self.starting_nodes)} starting nodes. Either "
                f"the entire execution graph must be specified manually, or both run "
                f"signals and starting nodes must be left entirely unspecified for "
                f"automatic construction of the execution graph."
            )

    def _reconnect_run(self, run_signal_pairs_to_restore):
        self.disconnect_run()
        for pairs in run_signal_pairs_to_restore:
            pairs[0].connect(pairs[1])

    def to_workfow(self):
        raise NotImplementedError

    def from_storage(self, storage):
        super().from_storage(storage)
        # Nodes instantiated in macros probably aren't aware of their parent at
        # instantiation time, and thus may be clean (un-loaded) objects --
        # reload their data
        for label, node in self.children.items():
            node.from_storage(storage[label])

    @property
    def _input_value_links(self):
        """
        Value connections between child output and macro in string representation based
        on labels.

        The string representation helps storage, and having it as a property ensures
        the name is protected.
        """
        return [
            (c.label, (c.value_receiver.owner.label, c.value_receiver.label))
            for c in self.inputs
        ]

    @property
    def _output_value_links(self):
        """
        Value connections between macro and child input in string representation based
        on labels.

        The string representation helps storage, and having it as a property ensures
        the name is protected.
        """
        return [
            ((c.owner.label, c.label), c.value_receiver.label)
            for child in self
            for c in child.outputs
            if c.value_receiver is not None
        ]

    def __getstate__(self):
        state = super().__getstate__()
        state["_input_value_links"] = self._input_value_links
        state["_output_value_links"] = self._output_value_links
        return state

    def __setstate__(self, state):
        # Purge value links from the state
        input_links = state.pop("_input_value_links")
        output_links = state.pop("_output_value_links")

        super().__setstate__(state)

        # Re-forge value links
        for inp, (child, child_inp) in input_links:
            self.inputs[inp].value_receiver = self.children[child].inputs[child_inp]

        for (child, child_out), out in output_links:
            self.children[child].outputs[child_out].value_receiver = self.outputs[out]


class Macro(HasCreator):
    """
    Not an actual macro class, just a mis-direction that dynamically creates a new
    child of :class:`AbstractMacro` using the provided :func:`graph_creator` and
    creates an instance of that.

    Quacks like a :class:`Composite` for the sake of creating and registering nodes.
    """

    def __new__(
        cls,
        graph_creator,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        strict_naming: bool = True,
        output_labels: Optional[str | list[str] | tuple[str]] = None,
        **kwargs,
    ):
        if not callable(graph_creator):
            # `Function` quacks like a class, even though it's a function and
            # dynamically creates children of `AbstractFunction` by providing the necessary
            # callable to the decorator
            raise AttributeError(
                f"Expected `graph_creator` to be callable but got {graph_creator}"
            )

        if output_labels is None:
            output_labels = ()
        elif isinstance(output_labels, str):
            output_labels = (output_labels,)

        return macro_node(*output_labels)(graph_creator)(
            label=label,
            parent=parent,
            overwrite_save=overwrite_save,
            run_after_init=run_after_init,
            storage_backend=storage_backend,
            save_after_run=save_after_run,
            strict_naming=strict_naming,
            **kwargs,
        )

    # Quack like an AbstractMacro
    @classmethod
    def allowed_backends(cls):
        return tuple(AbstractMacro._storage_interfaces().keys())


def macro_node(*output_labels):
    """
    A decorator for dynamically creating macro classes from graph-creating functions.

    Decorates a function.
    Returns a :class:`Macro` subclass whose name is the camel-case version of the
    graph-creating function, and whose signature is modified to exclude this function
    and provided kwargs.

    Optionally takes output labels as args in case the node function uses the
    like-a-function interface to define its IO. (The number of output labels must match
    number of channel-like objects returned by the graph creating function _exactly_.)

    Optionally takes any keyword arguments of :class:`Macro`.
    """
    output_labels = None if len(output_labels) == 0 else output_labels

    def as_node(graph_creator: callable[[Macro, ...], Optional[tuple[HasChannel]]]):
        node_class = type(
            graph_creator.__name__,
            (AbstractMacro,),  # Define parentage
            {
                "graph_creator": staticmethod(graph_creator),
                "_provided_output_labels": output_labels,
                "__module__": graph_creator.__module__,
            },
        )
        try:
            node_class._validate_output_labels()
        except OSError:
            warnings.warn(
                f"Could not find the source code to validate {node_class.__name__} "
                f"output labels"
            )
        return node_class

    return as_node
