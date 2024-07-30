"""
A base class for macro nodes, which are composite like workflows but have a static
interface and are not intended to be internally modified after instantiation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import re
from typing import Literal, TYPE_CHECKING

from pyiron_snippets.factory import classfactory

from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.mixin.has_interface_mixins import HasChannel
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.mixin.preview import ScrapesIO
from pyiron_workflow.nodes.static_io import StaticNode

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel


class Macro(Composite, StaticNode, ScrapesIO, ABC):
    """
    A macro is a composite node that holds a graph with a fixed interface, like a
    pre-populated workflow that is the same every time you instantiate it.

    At instantiation, the macro uses a provided callable to build and wire the graph,
    then builds a static IO interface for this graph.
    This callable must use the macro object itself as the first argument (e.g. adding
    nodes to it).
    The provided callable may optionally specify further args and kwargs; these are
    used to pre-populate the macro with :class:`UserInput` nodes, although they may
    later be trimmed if the IO can be connected directly to child node IO without any
    loss of functionality.
    This can be especially helpful when more than one child node needs access to the
    same input value.
    Similarly, the callable may return any number of child nodes' output channels (or
    the node itself in the case of single-output nodes) as long as a commensurate
    number of labels for these outputs were provided to the class constructor.
    These function-like definitions of the graph creator callable can be used
    to build only input xor output, or both together.
    Macro input channel labels are scraped from the signature of the graph creator;
    for output, output labels can be provided explicitly as a class attribute or, as a
    fallback, they are scraped from the graph creator code return statement (stripping
    off the "{first argument}.", where {first argument} is whatever the name of the
    first argument is.

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
    Unlike :class:`Workflow`, this execution flow automation is set up once at
    instantiation;
    If the macro is modified post-facto, you may need to manually re-invoke
    :meth:`configure_graph_execution`.

    Promises (in addition parent class promises):

    - IO is...
        - Statically defined at the class level
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

        >>> from pyiron_workflow import Macro, as_macro_node, macro_node
        >>> def add_one(x):
        ...     result = x + 1
        ...     return result
        >>>
        >>> def add_three_macro(self, one__x):
        ...     self.one = self.create.function_node(add_one, x=one__x)
        ...     self.two = self.create.function_node(add_one, self.one)
        ...     self.three = self.create.function_node(add_one, self.two)
        ...     self.one >> self.two >> self.three
        ...     self.starting_nodes = [self.one]
        ...     return self.three

        In this case we had _no need_ to specify the execution order and starting nodes
        --it's just an extremely simple DAG after all! -- but it's done here to
        demonstrate the syntax.

        We can make a macro by passing this graph-building function (that takes a macro
        as its first argument, i.e. `self` from the macro's perspective) to the :class:`Macro`
        class. Then, we can use it like a regular node! Just like a workflow, the
        io is constructed from unconnected owned-node IO by combining node and channel
        labels.

        >>> macro = macro_node(add_three_macro, output_labels="three__result")
        >>> out = macro(one__x=3)
        >>> out.three__result
        6

        We can also nest macros, rename their IO, and provide access to
        internally-connected IO by inputs and outputs maps:

        >>> def nested_macro(self, inp):
        ...     self.a = self.create.function_node(add_one, x=inp)
        ...     self.b = self.create.macro_node(
        ...         add_three_macro, one__x=self.a, output_labels="three__result"
        ...     )
        ...     self.c = self.create.function_node(add_one, x=self.b)
        ...     return self.c, self.b
        >>>
        >>> macro = macro_node(
        ...     nested_macro, output_labels=("out", "intermediate")
        ... )
        >>> macro(inp=1)
        {'out': 6, 'intermediate': 5}

        Macros and workflows automatically generate execution flows when their data
        is acyclic.
        Let's build a simple macro with two independent tracks:

        >>> def modified_flow_macro(self, a__x=0, b__x=0):
        ...     self.a = self.create.function_node(add_one, x=a__x)
        ...     self.b = self.create.function_node(add_one, x=b__x)
        ...     self.c = self.create.function_node(add_one, x=self.b)
        ...     return self.a, self.c
        >>>
        >>> m = macro_node(modified_flow_macro, output_labels=("a", "c"))
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
        >>> m.starting_nodes = [m.b]
        >>> _ = m.b >> m.c
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
        function nodes. If no output labels are explicitly provided, these are scraped
        from the function return value, just like for function nodes (except the
        initial `macro.` (or whatever the first argument is named) on any return values
        is ignored):

        >>> @Macro.wrap.as_macro_node()
        ... def AddThreeMacro(self, x):
        ...     add_three_macro(self, one__x=x)
        ...     # We could also simply have decorated that function to begin with
        ...     return self.three
        >>>
        >>> macro = AddThreeMacro()
        >>> macro(x=0).three
        3

        Alternatively (and not recommended) is to make a new child class of
        :class:`Macro` that overrides the :meth:`graph_creator` arg such that
        the same graph is always created.

        >>> class AddThreeMacro(Macro):
        ...     _output_labels = ["three"]
        ...
        ...     @staticmethod
        ...     def graph_creator(self, x):
        ...         add_three_macro(self, one__x=x)
        ...         return self.three
        >>>
        >>> macro = AddThreeMacro()
        >>> macro(x=0).three
        3

        We can also modify an existing macro at runtime by replacing nodes within it, as
        long as the replacement has fully compatible IO. There are three syntacic ways
        to do this. Let's explore these by going back to our `add_three_macro` and
        replacing each of its children with a node that adds 2 instead of 1.

        >>> @Macro.wrap.as_function_node()
        ... def add_two(x):
        ...     result = x + 2
        ...     return result
        >>>
        >>> adds_six_macro = macro_node(add_three_macro, output_labels="three__result")
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

        >>> @Macro.wrap.as_macro_node("lout", "n_plus_2")
        ... def LikeAFunction(self, lin: list,  n: int = 1):
        ...     self.plus_two = n + 2
        ...     self.sliced_list = lin[n:self.plus_two]
        ...     self.double_fork = 2 * n
        ...     return self.sliced_list, self.plus_two.channel
        >>>
        >>> like_functions = LikeAFunction(lin=[1,2,3,4,5,6], n=3)
        >>> sorted(like_functions().items())
        [('lout', [4, 5]), ('n_plus_2', 5)]

        >>> like_functions.double_fork.value
        6


    """

    def _setup_node(self) -> None:
        super()._setup_node()

        ui_nodes = self._prepopulate_ui_nodes_from_graph_creator_signature(
            storage_backend=self.storage_backend
        )
        returned_has_channel_objects = self.graph_creator(self, *ui_nodes)
        if returned_has_channel_objects is None:
            returned_has_channel_objects = ()
        elif isinstance(returned_has_channel_objects, HasChannel):
            returned_has_channel_objects = (returned_has_channel_objects,)

        for node in ui_nodes:
            self.inputs[node.label].value_receiver = node.inputs.user_input

        for node, output_channel_label in zip(
            returned_has_channel_objects,
            () if self._output_labels is None else self._output_labels,
        ):
            node.channel.value_receiver = self.outputs[output_channel_label]

        remaining_ui_nodes = self._purge_single_use_ui_nodes(ui_nodes)
        self._configure_graph_execution(remaining_ui_nodes)

    @staticmethod
    @abstractmethod
    def graph_creator(self, *args, **kwargs) -> callable:
        """Build the graph the node will run."""

    @classmethod
    def _io_defining_function(cls) -> callable:
        return cls.graph_creator

    _io_defining_function_uses_self = True

    @classmethod
    def _scrape_output_labels(cls):
        scraped_labels = super(Macro, cls)._scrape_output_labels()

        if scraped_labels is not None:
            # Strip off the first argument, e.g. self.foo just becomes foo
            self_argument = list(cls._get_input_args().keys())[0]
            cleaned_labels = [
                re.sub(r"^" + re.escape(f"{self_argument}."), "", label)
                for label in scraped_labels
            ]
            if any("." in label for label in cleaned_labels):
                raise ValueError(
                    f"Tried to scrape cleaned labels for {cls.__name__}, but at least "
                    f"one of {cleaned_labels} still contains a '.' -- please provide "
                    f"explicit labels"
                )
            return cleaned_labels
        else:
            return scraped_labels

    def _prepopulate_ui_nodes_from_graph_creator_signature(
        self, storage_backend: Literal["h5io", "tinybase"]
    ):
        ui_nodes = []
        for label, (type_hint, default) in self.preview_inputs().items():
            n = self.create.standard.UserInput(
                default,
                label=label,
                parent=self,
                storage_backend=storage_backend,
            )
            n.inputs.user_input.type_hint = type_hint
            ui_nodes.append(n)
        return tuple(ui_nodes)

    def _purge_single_use_ui_nodes(self, ui_nodes):
        """
        We (may) create UI nodes based on the :meth:`graph_creator` signature;
        If these are connected to only a single node actually defined in the creator,
        they are superfluous, and we can remove them -- linking the macro input
        directly to the child node input.
        """
        remaining_ui_nodes = list(ui_nodes)
        for macro_input in self.inputs:
            target_node = macro_input.value_receiver.owner
            if (
                target_node in ui_nodes  # Value link is a UI node
                and target_node.channel.value_receiver is None  # That doesn't forward
                # its value directly to the output
                and len(target_node.channel.connections) <= 1  # And isn't forked to
                # multiple children
            ):
                if len(target_node.channel.connections) == 1:
                    macro_input.value_receiver = target_node.channel.connections[0]
                self.remove_child(target_node)
                remaining_ui_nodes.remove(target_node)
        return tuple(remaining_ui_nodes)

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
            self.starting_nodes = ui_nodes if len(ui_nodes) > 0 else self.starting_nodes
        elif not has_signals and not has_starters:
            # Automate construction of the execution graph
            self.set_run_signals_to_dag_execution()
        else:
            raise ValueError(
                f"The macro {self.full_label} has {len(run_signals)} run signals "
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


@classfactory
def macro_node_factory(
    graph_creator: callable,
    validate_output_labels: bool,
    use_cache: bool = True,
    /,
    *output_labels: str,
):
    """
    Create a new :class:`Macro` subclass using the given graph creator function.

    Args:
        graph_creator (callable): Function to create the graph for the :class:`Macro`.
        validate_output_labels (bool): Whether to validate the output labels against
            the return values of the wrapped function.
        use_cache (bool): Whether nodes of this type should default to caching their
            values.
        output_labels (tuple[str, ...]): Optional labels for the :class:`Macro`'s
            outputs.

    Returns:
        type[Macro]: A new :class:`Macro` subclass.
    """
    return (
        graph_creator.__name__,
        (Macro,),  # Define parentage
        {
            "graph_creator": staticmethod(graph_creator),
            "__module__": graph_creator.__module__,
            "__qualname__": graph_creator.__qualname__,
            "_output_labels": None if len(output_labels) == 0 else output_labels,
            "_validate_output_labels": validate_output_labels,
            "__doc__": graph_creator.__doc__,
            "use_cache": use_cache,
        },
        {},
    )


def as_macro_node(
    *output_labels: str, validate_output_labels: bool = True, use_cache: bool = True
):
    """
    Decorator to convert a function into a :class:`Macro` node.

    Args:
        *output_labels (str): Optional labels for the :class:`Macro`'s outputs.
        validate_output_labels (bool): Whether to validate the output labels.
        use_cache (bool): Whether nodes of this type should default to caching their
            values. (Default is True.)

    Returns:
        callable: A decorator that converts a function into a Macro node.
    """

    def decorator(graph_creator):
        macro_node_factory.clear(graph_creator.__name__)  # Force a fresh class
        factory_made = macro_node_factory(
            graph_creator, validate_output_labels, use_cache, *output_labels
        )
        factory_made._class_returns_from_decorated_function = graph_creator
        factory_made.preview_io()
        return factory_made

    return decorator


def macro_node(
    graph_creator: callable,
    *node_args,
    output_labels: str | tuple[str, ...] | None = None,
    validate_output_labels: bool = True,
    use_cache: bool = True,
    **node_kwargs,
):
    """
    Create and return a :class:`Macro` node instance using the given node function.

    Args:
        graph_creator (callable): Function to create the graph for the :class:`Macro`.
        node_args: Positional arguments for the :class:`Macro` initialization --
            parsed as node input data.
        output_labels (str | tuple[str, ...] | None): Labels for the :class:`Macro`'s
            outputs. Default is None, which scrapes these from the return statement in
            the decorated function's source code.
        validate_output_labels (bool): Whether to validate the output labels. Defaults
            to True.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        node_kwargs: Keyword arguments for the :class:`Macro` initialization --
            parsed as node input data when the keyword matches an input channel.

    Returns:
        Macro: An instance of the :class:`Macro` subclass.
    """
    if output_labels is None:
        output_labels = ()
    elif isinstance(output_labels, str):
        output_labels = (output_labels,)
    macro_node_factory.clear(graph_creator.__name__)  # Force a fresh class
    factory_made = macro_node_factory(
        graph_creator, validate_output_labels, use_cache, *output_labels
    )
    factory_made.preview_io()
    return factory_made(*node_args, **node_kwargs)
