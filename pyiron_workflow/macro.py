"""
A base class for macro nodes, which are composite like workflows but have a static
interface and are not intended to be internally modified after instantiation.
"""

from __future__ import annotations

from functools import partialmethod
import inspect
from typing import get_type_hints, Literal, Optional

from bidict import bidict

from pyiron_workflow.channels import InputData, OutputData, NotData
from pyiron_workflow.composite import Composite
from pyiron_workflow.has_channel import HasChannel
from pyiron_workflow.io import Outputs, Inputs
from pyiron_workflow.output_parser import ParseOutput


class Macro(Composite):
    """
    A macro is a composite node that holds a graph with a fixed interface, like a
    pre-populated workflow that is the same every time you instantiate it.

    At instantiation, the macro uses a provided callable to build and wire the graph,
    then builds a static IO interface for this graph.
    This callable must use the macro object itself as the first argument (e.g. adding
    nodes to it).
    As with `Workflow` objects, macros leverage `inputs_map` and `outputs_map` to
    control macro-level IO access to child IO.
    As with `Workflow`, default behaviour is to expose all unconnected child IO.
    The provided callable may optionally specify further args and kwargs, which are used
    to pre-populate the macro with `UserInput` nodes;
    This can be especially helpful when more than one child node needs access to the
    same input value.
    Similarly, the callable may return any number of child nodes' output channels (or
    the node itself in the case of `SingleValue` nodes) and commensurate
    `output_labels` to define macro-level output.
    These function-like definitions of the graph creator callable can be used
    independently or together.
    Each that is used switches its IO map to a "whitelist" paradigm, so any I/O _not_
    provided in the callable signature/return values and output labels will be disabled.
    Manual modifications of the IO maps inside the callable always take priority over
    this whitelisting behaviour, so you always retain full control over what IO is
    exposed, and the whitelisting is only for your convenience.

    Macro IO is _value linked_ to the child IO, so that their values stay synchronized,
    but the child nodes of a macro form an isolated sub-graph.

    As with function nodes, subclasses of `Macro` may define a method for creating the
    graph.

    As with `Workflow``, all DAG macros can determine their execution flow
    automatically, if you have cycles in your data flow, or otherwise want more control
    over the execution, all you need to do is specify the `node.signals.input.run`
    connections and `starting_nodes` list yourself.
    If only _one_ of these is specified, you'll get an error, but if you've provided
    both then no further checks of their validity/reasonableness are performed, so be
    careful.
    Unlike `Workflow`, this execution flow automation is set up once at instantiation;
    If the macro is modified post-facto, you may need to manually re-invoke
    `configure_graph_execution`.

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
        >>> def add_three_macro(macro):
        ...     macro.one = macro.create.SingleValue(add_one)
        ...     macro.two = macro.create.SingleValue(add_one, macro.one)
        ...     macro.three = macro.create.SingleValue(add_one, macro.two)
        ...     macro.one >> macro.two >> macro.three
        ...     macro.starting_nodes = [macro.one]

        In this case we had _no need_ to specify the execution order and starting nodes
        --it's just an extremely simple DAG after all! -- but it's done here to
        demonstrate the syntax.

        We can make a macro by passing this graph-building function (that takes a macro
        as its first argument, i.e. `self` from the macro's perspective) to the `Macro`
        class. Then, we can use it like a regular node! Just like a workflow, the
        io is constructed from unconnected owned-node IO by combining node and channel
        labels.
        >>> macro = Macro(add_three_macro)
        >>> out = macro(one__x=3)
        >>> out.three__result
        6

        If there's a particular macro we're going to use again and again, we might want
        to consider making a new child class of `Macro` that overrides the
        `graph_creator` arg such that the same graph is always created. We could
        override `__init__` the normal way, but it's even faster to just use
        `partialmethod`:
        >>> from functools import partialmethod
        >>> class AddThreeMacro(Macro):
        ...     @staticmethod
        ...     def graph_creator(self):
        ...         add_three_macro(self)
        ...
        ...     __init__ = partialmethod(
        ...         Macro.__init__,
        ...         None,  # We directly define the graph creator method on the class
        ...     )
        >>>
        >>> macro = AddThreeMacro()
        >>> macro(one__x=0).three__result
        3

        We can also nest macros, rename their IO, and provide access to
        internally-connected IO by inputs and outputs maps:
        >>> def nested_macro(macro):
        ...     macro.a = macro.create.SingleValue(add_one)
        ...     macro.b = macro.create.Macro(add_three_macro, one__x=macro.a)
        ...     macro.c = macro.create.SingleValue(
        ...         add_one, x=macro.b.outputs.three__result
        ...     )
        >>>
        >>> macro = Macro(
        ...     nested_macro,
        ...     inputs_map={"a__x": "inp"},
        ...     outputs_map={"c__result": "out", "b__three__result": "intermediate"},
        ... )
        >>> macro(inp=1)
        {'intermediate': 5, 'out': 6}

        Macros and workflows automatically generate execution flows when their data
        is acyclic.
        Let's build a simple macro with two independent tracks:
        >>> def modified_flow_macro(macro):
        ...     macro.a = macro.create.SingleValue(add_one, x=0)
        ...     macro.b = macro.create.SingleValue(add_one, x=0)
        ...     macro.c = macro.create.SingleValue(add_one, x=0)
        >>>
        >>> m = Macro(modified_flow_macro)
        >>> m(a__x=1, b__x=2, c__x=3)
        {'a__result': 2, 'b__result': 3, 'c__result': 4}

        We can override which nodes get used to start by specifying the `starting_nodes`
        property.
        If we do this we also need to provide at least one connection among the run
        signals, but beyond that the code doesn't hold our hands.
        Let's use this and then observe how the `a` sub-node no longer gets run:
        >>> m.starting_nodes = [m.b]  # At least one starting node
        >>> _ = m.b >> m.c  # At least one run signal
        >>> # We catch and ignore output -- it's needed for chaining, but screws up
        >>> # doctests -- you don't normally need to catch it like this!
        >>> m(a__x=1000, b__x=2000, c__x=3000)
        {'a__result': 2, 'b__result': 2001, 'c__result': 3001}

        Note how the `a` node is no longer getting run, so the output is not updated!
        Manually controlling execution flow is necessary for cyclic graphs (cf. the
        while loop meta-node), but best to avoid when possible as it's easy to miss
        intended connections in complex graphs.

        We can also modify an existing macro at runtime by replacing nodes within it, as
        long as the replacement has fully compatible IO. There are three syntacic ways
        to do this. Let's explore these by going back to our `add_three_macro` and
        replacing each of its children with a node that adds 2 instead of 1.
        >>> @Macro.wrap_as.single_value_node()
        ... def add_two(x):
        ...     result = x + 2
        ...     return result
        >>>
        >>> adds_six_macro = Macro(add_three_macro)
        >>> # With the replace method
        >>> # (replacement target can be specified by label or instance,
        >>> # the replacing node can be specified by instance or class)
        >>> replaced = adds_six_macro.replace_node(adds_six_macro.one, add_two())
        >>> # With the replace_with method
        >>> adds_six_macro.two.replace_with(add_two())
        >>> # And by assignment of a compatible class to an occupied node label
        >>> adds_six_macro.three = add_two
        >>> adds_six_macro(one__x=1)
        {'three__result': 7}

        Instead of controlling the IO interface with dictionary maps, we can instead
        provide a more `Function(Node)`-like definition of the `graph_creator` by
        adding args and/or kwargs to the signature (under the hood, this dynamically
        creates new `UserInput` nodes before running the rest of the graph creation),
        and/or returning child channels (or whole children in the case of `SingleValue`
        nodes) and providing commensurate `output_labels`.
        This process switches us from the `Workflow` default of exposing all
        unconnected child IO, to a "whitelist" paradigm of _only_ showing the IO that
        we exposed by our function defintion.
        (Note: any `.inputs_map` or `.outputs_map` explicitly defined in the
        `graph_creator` still takes precedence over this whitelisting! So you always
        retain full control over what IO gets exposed.)
        E.g., these two definitions are perfectly equivalent:

        >>> @Macro.wrap_as.macro_node("lout", "n_plus_2")
        ... def LikeAFunction(macro, lin: list,  n: int = 1):
        ...     macro.plus_two = n + 2
        ...     macro.sliced_list = lin[n:macro.plus_two]
        ...     macro.double_fork = 2 * n
        ...     # ^ This is vestigial, just to show we don't need to blacklist it in a
        ...     # whitelist-paradigm
        ...     return macro.sliced_list, macro.plus_two.channel
        >>>
        >>> like_functions = LikeAFunction(lin=[1,2,3,4,5,6], n=2)
        >>> like_functions()
        {'n_plus_2': 4, 'lout': [3, 4]}

        >>> @Macro.wrap_as.macro_node()
        ... def WithIOMaps(macro):
        ...     macro.list_in = macro.create.standard.UserInput()
        ...     macro.list_in.inputs.user_input.type_hint = list
        ...     macro.forked = macro.create.standard.UserInput(2)
        ...     macro.forked.inputs.user_input.type_hint = int
        ...     macro.n_plus_2 = macro.forked + 2
        ...     macro.sliced_list = macro.list_in[macro.forked:macro.n_plus_2]
        ...     macro.double_fork = 2 * macro.forked
        ...     macro.inputs_map = {
        ...         "list_in__user_input": "lin",
        ...         macro.forked.inputs.user_input.scoped_label: "n",
        ...         "n_plus_2__other": None,
        ...         "list_in__user_input_Slice_forked__user_input_n_plus_2__add_None__step": None,
        ...         macro.double_fork.inputs.other.scoped_label: None,
        ...     }
        ...     macro.outputs_map = {
        ...         macro.sliced_list.outputs.getitem.scoped_label: "lout",
        ...         macro.n_plus_2.outputs.add.scoped_label: "n_plus_2",
        ...         "double_fork__rmul": None
        ...     }
        >>>
        >>> with_maps = WithIOMaps(lin=[1,2,3,4,5,6], n=2)
        >>> with_maps()
        {'n_plus_2': 4, 'lout': [3, 4]}

        Here we've leveraged the macro-creating decorator, but this works the same way
        using the `Macro` class directly.
    """

    def __init__(
        self,
        graph_creator: callable[[Macro], None],
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        run_after_init: bool = False,
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
        output_labels: Optional[str | list[str] | tuple[str]] = None,
        **kwargs,
    ):
        if not callable(graph_creator):
            # Children of `Function` may explicitly provide a `node_function` static
            # method so the node has fixed behaviour.
            # In this case, the `__init__` signature should be changed so that the
            # `node_function` argument is just always `None` or some other non-callable.
            # If a callable `node_function` is not received, you'd better have it as an
            # attribute already!
            if not hasattr(self, "graph_creator"):
                raise AttributeError(
                    f"If `None` is provided as a `graph_creator`, a `graph_creator` "
                    f"property must be defined instead, e.g. when making child classes"
                    f"of `Macro` with specific behaviour"
                )
        else:
            # If a callable graph creator is received, use it
            self.graph_creator = graph_creator

        self._parent = None
        super().__init__(
            label=label if label is not None else self.graph_creator.__name__,
            parent=parent,
            strict_naming=strict_naming,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )
        output_labels = self._validate_output_labels(output_labels)

        ui_nodes = self._prepopulate_ui_nodes_from_graph_creator_signature()
        returned_has_channel_objects = self.graph_creator(self, *ui_nodes)
        self._configure_graph_execution()

        # Update IO map(s) if a function-like graph creator interface was used
        if len(ui_nodes) > 0:
            self._whitelist_inputs_map(*ui_nodes)
        if returned_has_channel_objects is not None:
            self._whitelist_outputs_map(
                output_labels,
                *(
                    (returned_has_channel_objects,)
                    if not isinstance(returned_has_channel_objects, tuple)
                    else returned_has_channel_objects
                ),
            )

        self._inputs: Inputs = self._build_inputs()
        self._outputs: Outputs = self._build_outputs()

        self.set_input_values(**kwargs)

    def _validate_output_labels(self, output_labels) -> tuple[str]:
        """
        Ensure that output_labels, if provided, are commensurate with graph creator
        return values, if provided, and return them as a tuple.
        """
        graph_creator_returns = ParseOutput(self.graph_creator).output
        output_labels = (
            (output_labels,) if isinstance(output_labels, str) else output_labels
        )
        if graph_creator_returns is not None or output_labels is not None:
            error_suffix = (
                f"but {self.label} macro got return values: "
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
        return () if output_labels is None else tuple(output_labels)

    def _prepopulate_ui_nodes_from_graph_creator_signature(self):
        hints_dict = get_type_hints(self.graph_creator)
        interface_nodes = ()
        for i, (arg_name, inspected_value) in enumerate(
            inspect.signature(self.graph_creator).parameters.items()
        ):
            if i == 0:
                continue  # Skip the macro argument itself, it's like `self` here

            default = (
                NotData
                if inspected_value.default is inspect.Parameter.empty
                else inspected_value.default
            )
            node = self.create.standard.UserInput(default, label=arg_name, parent=self)
            node.inputs.user_input.default = default
            try:
                node.inputs.user_input.type_hint = hints_dict[arg_name]
            except KeyError:
                pass  # If there's no hint that's fine
            interface_nodes += (node,)

        return interface_nodes

    def _whitelist_inputs_map(self, *ui_nodes) -> None:
        """
        Updates the inputs map so each UI node's output channel is available directly
        under the node label, and updates the map to disable all other input that
        wasn't explicitly mapped already.
        """
        self.inputs_map = self._hide_non_whitelisted_io(
            self._whitelist_map(
                self.inputs_map, tuple(n.label for n in ui_nodes), ui_nodes
            ),
            "inputs",
        )

    def _whitelist_outputs_map(
        self, output_labels: tuple[str], *creator_returns: HasChannel
    ):
        """
        Updates the outputs map so objects returned by the graph creator directly
        leverage the supplied output labels, and updates the map to disable all other
        output that wasn't explicitly mapped already.
        """
        self.outputs_map = self._hide_non_whitelisted_io(
            self._whitelist_map(self.outputs_map, output_labels, creator_returns),
            "outputs",
        )

    @staticmethod
    def _whitelist_map(
        io_map: bidict, new_labels: tuple[str], has_channel_objects: tuple[HasChannel]
    ) -> bidict:
        """
        Update an IO map to give new labels to the channels of a bunch of `HasChannel`
        objects.
        """
        io_map = bidict({}) if io_map is None else io_map
        for new_label, ui_node in zip(new_labels, has_channel_objects):
            # White-list everything not already in the map
            if ui_node.channel.scoped_label not in io_map.keys():
                io_map[ui_node.channel.scoped_label] = new_label
        return io_map

    def _hide_non_whitelisted_io(
        self, io_map: bidict, i_or_o: Literal["inputs", "outputs"]
    ) -> dict:
        """
        Make a new map dictionary with `None` entries for each channel that isn't
        already in the provided map bidict. I.e. blacklist things we didn't whitelist.
        """
        io_map = dict(io_map)
        # We do it in two steps like this to leverage the bidict security on the setter
        # Since bidict can't handle getting `None` (i.e. disable) for multiple keys
        for node in self.nodes.values():
            for channel in getattr(node, i_or_o):
                if channel.scoped_label not in io_map.keys():
                    io_map[channel.scoped_label] = None
        return io_map

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
            node=self,
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

    def _update_children(self, children_from_another_process):
        super()._update_children(children_from_another_process)
        self._rebuild_data_io()

    def _configure_graph_execution(self):
        run_signals = self.disconnect_run()

        has_signals = len(run_signals) > 0
        has_starters = len(self.starting_nodes) > 0

        if has_signals and has_starters:
            # Assume the user knows what they're doing
            self._reconnect_run(run_signals)
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


def macro_node(*output_labels, **node_class_kwargs):
    """
    A decorator for dynamically creating macro classes from graph-creating functions.

    Decorates a function.
    Returns a `Macro` subclass whose name is the camel-case version of the
    graph-creating function, and whose signature is modified to exclude this function
    and provided kwargs.

    Optionally takes output labels as args in case the node function uses the
    like-a-function interface to define its IO. (The number of output labels must match
    number of channel-like objects returned by the graph creating function _exactly_.)

    Optionally takes any keyword arguments of `Macro`.
    """
    output_labels = None if len(output_labels) == 0 else output_labels

    def as_node(graph_creator: callable[[Macro, ...], Optional[tuple[HasChannel]]]):
        return type(
            graph_creator.__name__,
            (Macro,),  # Define parentage
            {
                "__init__": partialmethod(
                    Macro.__init__,
                    None,
                    output_labels=output_labels,
                    **node_class_kwargs,
                ),
                "graph_creator": staticmethod(graph_creator),
            },
        )

    return as_node
