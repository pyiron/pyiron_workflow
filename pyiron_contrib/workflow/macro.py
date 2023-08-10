"""
A base class for macro nodes, which are composite like workflows but have a static
interface and are not intended to be internally modified after instantiation.
"""

from __future__ import annotations

from functools import partialmethod
from typing import Optional

from pyiron_contrib.workflow.composite import Composite
from pyiron_contrib.workflow.io import Outputs, Inputs


class Macro(Composite):
    """
    A macro is a composite node that holds a graph with a fixed interface, like a
    pre-populated workflow that is the same every time you instantiate it.

    At instantiation, the macro uses a provided callable to build and wire the graph,
    then builds a static IO interface for this graph. (By default, unconnected IO is
    passed using the same formalism as workflows to combine node and channel names, but
    this can be overriden to rename the channels in the IO panel and/or to expose
    channels that already have an internal connection.)

    Like function nodes, initial values for input can be set using kwargs, and the node
    will (by default) attempt to update at the end of the instantiation process.

    It is intended that subclasses override the initialization signature and provide
    the graph creation directly from their own method.

    Examples:
        Let's consider the simplest case of macros that just consecutively add 1 to
        their input:
        >>> from pyiron_contrib.workflow.function import SingleValue
        >>> from pyiron_contrib.workflow.macro import Macro
        >>>
        >>> def add_one(x):
        ...     result = x + 1
        ...     return result
        >>>
        >>> def add_three_macro(macro):
        ...     macro.one = SingleValue(add_one)
        ...     macro.two = SingleValue(add_one, macro.one)
        ...     macro.three = SingleValue(add_one, macro.two)

        We can make a macro by passing this graph-building function (that takes a macro
        as its first argument, i.e. `self` from the macro's perspective) to the `Macro`
        class. Then, we can use it like a regular node! Just like a workflow, the
        io is constructed from unconnected owned-node IO by combining node and channel
        labels.
        >>> macro = Macro(add_three_macro)
        >>> out = macro(one_x=3)
        >>> out.three_result
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
        >>> macro(one_x=0).three_result
        3

        We can also nest macros, rename their IO, and provide access to
        internally-connected IO:
        >>> def nested_macro(macro):
        ...     macro.a = SingleValue(add_one)
        ...     macro.b = Macro(add_three_macro, one_x=macro.a)
        ...     macro.c = SingleValue(add_one, x=macro.b.outputs.three_result)
        >>>
        >>> macro = Macro(
        ...     nested_macro,
        ...     inputs_map={"a_x": "inp"},
        ...     outputs_map={"c_result": "out", "b_result": "intermediate"},
        ... )
        >>> macro(inp=1)
        {'intermediate': 5, 'out': 6}

        Since the graph builder has access to the macro being instantiated, we can also
        do things like override the starting nodes to be used when invoking a run. E.g.
        consider this two-track graph, where we would normally run both nodes on a `run`
        call (since they are both head-most nodes), but we override the default behavior
        to only run _one_ of the two tracks (note that we stop the child nodes from
        running when they get their values updated, just so we can see that one of them
        is really not doing anything on the run command):
        >>> def modified_start_macro(macro):
        ...     macro.a = SingleValue(add_one, x=0, run_on_updates=False)
        ...     macro.b = SingleValue(add_one, x=0, run_on_updates=False)
        ...     macro.starting_nodes = [macro.b]
        >>>
        >>> m = Macro(modified_start_macro, update_on_instantiation=False)
        >>> m.outputs.to_value_dict()
        {'a_result': pyiron_contrib.workflow.channels.NotData,
        'b_result': pyiron_contrib.workflow.channels.NotData}

        >>> m(a_x=1, b_x=2)
        {'a_result': pyiron_contrib.workflow.channels.NotData, 'b_result': 3}
    """

    def __init__(
        self,
        graph_creator: callable[[Macro], None],
        label: Optional[str] = None,
        run_on_updates: bool = True,
        update_on_instantiation: bool = True,
        parent: Optional[Composite] = None,
        strict_naming: bool = True,
        inputs_map: Optional[dict] = None,
        outputs_map: Optional[dict] = None,
        **kwargs,
    ):
        self._parent = None
        super().__init__(
            label=label if label is not None else graph_creator.__name__,
            parent=parent,
            run_on_updates=run_on_updates,
            strict_naming=strict_naming,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )
        graph_creator(self)

        self._inputs: Inputs = self._build_inputs()
        self._outputs: Outputs = self._build_outputs()

        self._batch_update_input(**kwargs)

        if update_on_instantiation:
            self.update()

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> Outputs:
        return self._outputs

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
                "__init__": partialmethod(
                    Macro.__init__,
                    graph_creator,
                    **node_class_kwargs,
                )
            },
        )

    return as_node
