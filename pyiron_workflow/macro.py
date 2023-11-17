"""
A base class for macro nodes, which are composite like workflows but have a static
interface and are not intended to be internally modified after instantiation.
"""

from __future__ import annotations

from functools import partialmethod
from typing import Optional, TYPE_CHECKING

from pyiron_workflow.channels import InputData, OutputData
from pyiron_workflow.composite import Composite
from pyiron_workflow.io import Outputs, Inputs

if TYPE_CHECKING:
    from bidict import bidict


class Macro(Composite):
    """
    A macro is a composite node that holds a graph with a fixed interface, like a
    pre-populated workflow that is the same every time you instantiate it.

    At instantiation, the macro uses a provided callable to build and wire the graph,
    then builds a static IO interface for this graph. (See the parent class docstring
    for more details, but by default and as with workflows, unconnected IO is
    represented by combining node and channel names, but can be controlled in more
    detail with maps.)
    This IO is _value linked_ to the child IO, so that their values stay synchronized,
    but the child nodes of a macro form an isolated sub-graph.
    As with function nodes, sub-classes may define a method for creating the graph.

    As with workflows, all DAG macros can determine their execution flow automatically,
    if you have cycles in your data flow, or otherwise want more control over the
    execution, all you need to do is specify the `node.signals.input.run` connections
    and `starting_nodes` list yourself.
    If only _one_ of these is specified, you'll get an error, but if you've provided
    both then no further checks of their validity/reasonableness are performed, so be
    careful.

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
        ...     macro.one > macro.two > macro.three
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
        ...     def build_graph(self):
        ...         add_three_macro(self)
        ...
        ...     __init__ = partialmethod(
        ...         Macro.__init__,
        ...         build_graph,
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
        >>> m.outputs.to_value_dict()
        >>> m(a__x=1, b__x=2, c__x=3)
        {'a__result': 2, 'b__result': 3, 'c__result': 4}

        We can override which nodes get used to start by specifying the `starting_nodes`
        property.
        If we do this we also need to provide at least one connection among the run
        signals, but beyond that the code doesn't hold our hands.
        Let's use this and then observe how the `a` sub-node no longer gets run:
        >>> m.starting_nodes = [m.b]  # At least one starting node
        >>> m.b > m.c  # At least one run signal
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
        >>> adds_six_macro.replace(adds_six_macro.one, add_two())
        >>> # With the replace_with method
        >>> adds_six_macro.two.replace_with(add_two())
        >>> # And by assignment of a compatible class to an occupied node label
        >>> adds_six_macro.three = add_two
        >>> adds_six_macro(one__x=1)
        {'three__result': 7}
    """

    def __init__(
        self,
        graph_creator: callable[[Macro], None],
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        strict_naming: bool = True,
        inputs_map: Optional[dict | bidict] = None,
        outputs_map: Optional[dict | bidict] = None,
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
        self.graph_creator(self)
        self._configure_graph_execution()

        self._inputs: Inputs = self._build_inputs()
        self._outputs: Outputs = self._build_outputs()

        self.set_input_values(**kwargs)

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


def macro_node(**node_class_kwargs):
    """
    A decorator for dynamically creating macro classes from graph-creating functions.

    Decorates a function.
    Returns a `Macro` subclass whose name is the camel-case version of the
    graph-creating function, and whose signature is modified to exclude this function
    and provided kwargs.

    Optionally takes any keyword arguments of `Macro`.
    """

    def as_node(graph_creator: callable[[Macro], None]):
        return type(
            graph_creator.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (Macro,),  # Define parentage
            {
                "__init__": partialmethod(Macro.__init__, None, **node_class_kwargs),
                "graph_creator": staticmethod(graph_creator),
            },
        )

    return as_node
