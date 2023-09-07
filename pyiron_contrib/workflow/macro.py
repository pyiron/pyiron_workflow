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
        >>> from pyiron_contrib.workflow.macro import Macro
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
        internally-connected IO:
        >>> def nested_macro(macro):
        ...     macro.a = macro.create.SingleValue(add_one)
        ...     macro.b = macro.create.Macro(add_three_macro, one__x=macro.a)
        ...     macro.c = macro.create.SingleValue(
        ...         add_one, x=macro.b.outputs.three__result
        ...     )
        ...     macro.a > macro.b > macro.c
        >>>
        >>> macro = Macro(
        ...     nested_macro,
        ...     inputs_map={"a__x": "inp"},
        ...     outputs_map={"c__result": "out", "b__three__result": "intermediate"},
        ... )
        >>> macro(inp=1)
        {'intermediate': 5, 'out': 6}

        Macros and workflows automatically look for the upstream-most data nodes and use
        those to start calculations when run.
        Let's build a simple macro with two independent tracks:
        >>> def modified_start_macro(macro):
        ...     macro.a = macro.create.SingleValue(add_one, x=0)
        ...     macro.b = macro.create.SingleValue(add_one, x=0)
        >>>
        >>> m = Macro(modified_start_macro)
        >>> m.outputs.to_value_dict()
        >>> m(a__x=1, b__x=2)
        {'a__result': 2, 'b__result': 3}

        We can override which nodes get used to start by specifying the `starting_nodes`
        property.
        Let's use this and then observe how the `a` sub-node no longer gets run:
        >>> m.starting_nodes = [m.b]
        >>> m(a__x=1000, b__x=2000)
        {'a__result': 2, 'b__result': 2001}
    """

    def __init__(
        self,
        graph_creator: callable[[Macro], None],
        label: Optional[str] = None,
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
            strict_naming=strict_naming,
            inputs_map=inputs_map,
            outputs_map=outputs_map,
        )
        graph_creator(self)

        self._inputs: Inputs = self._build_inputs()
        self._outputs: Outputs = self._build_outputs()

        self._batch_update_input(**kwargs)

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
