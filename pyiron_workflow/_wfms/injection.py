"""
This module provides a mixing class for ports and (single-output) nodes to produce
_new nodes_ when they are subjected to an operation.

Only unary operations on such an injectable object and operations between two such
objects are permitted -- i.e. operations on literals are not supported. As a side
effect

Not _all_ operations are injectable. Notably absent are comparators (e.g. `==`), which
are reserved for use by the actual port/node objects themselves (e.g. to investigate
membership in collections), and reflexive operations (e.g. `*=`), which don't make
sense in a graph paradigm (E.g., `wf.some_node.inputs.foo *= wf.inputs.bar`?!).
"""

from __future__ import annotations

import abc
from collections.abc import Callable
from typing import TYPE_CHECKING

import flowrep as fr
from flowrep.parsers import label_helpers

from pyiron_workflow._wfms import lexical, std

if TYPE_CHECKING:
    from pyiron_workflow._wfms import atomic, workflow
    from pyiron_workflow._wfms.datatypes import Graph, MutableDag, Node, Port


class InjectionContext:
    """
    Track ``OperatorInjectionMixin`` context through mutations by using
    callable references.
    """

    def __init__(
        self,
        *,
        port: Callable[[], Port],
        graph: Callable[[], Graph | Node | None],
        label: Callable[[], fr.schemas.Label],
        lexical_path: Callable[[], lexical.LexicalPath],
    ) -> None:
        self._port = port
        self._graph = graph
        self._lexical_path = lexical_path
        self._label = label

    @property
    def port(self) -> Port:
        return self._port()

    @property
    def graph(self) -> MutableDag | None:
        return self._validate_injection_context_graph(self._graph())

    @property
    def label(self) -> fr.schemas.Label:
        return self._label()

    @property
    def lexical_path(self) -> lexical.LexicalPath:
        return self._lexical_path()

    def _validate_injection_context_graph(
        self, graph: Node | Graph | None
    ) -> MutableDag | None:
        from pyiron_workflow._wfms.datatypes import MutableDag  # noqa: PLC0415

        if graph is not None and not isinstance(graph, MutableDag):
            raise TypeError(
                f"{self.lexical_path!r} cannot be used for injection, "
                f"because its injection context graph non-None and not a "
                f"{MutableDag.__name__}. {graph!r} is a {type(graph)!r}."
            )
        return graph


class OperatorInjectionMixin(abc.ABC):
    @property
    @abc.abstractmethod
    def _injection(self) -> InjectionContext:
        """The single point of interaction for mixin users."""

    def _unary_operation(
        self, operation: fr.schemas.LabeledRecipe
    ) -> atomic.Atomic | workflow.Workflow:
        context_graph = self._injection.graph
        label = f"{operation.label}_{self._injection.label}"
        return _build_injection_graph(
            operation,
            label,
            context_graph,
            self,
        )

    def _binary_operations(
        self, other: OperatorInjectionMixin, operation: fr.schemas.LabeledRecipe
    ) -> atomic.Atomic | workflow.Workflow:
        self_context = self._injection.graph
        other_context = other._injection.graph
        context_graph = self_context or other_context

        label = f"{self._injection.label}_{operation.label}_{other._injection.label}"

        if self_context and other_context and self_context is not other_context:
            raise ValueError(
                f"Can't inject across graph contexts. "
                f"{self._injection.lexical_path!r} cannot inject operation "
                f"{operation.label!r} with {other._injection.lexical_path!r} because "
                "of mis-matched owners."
            )
        return _build_injection_graph(operation, label, context_graph, self, other)

    def __abs__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(std.abs)

    def __add__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.add)

    def __mul__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.mul)

    def __neg__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(std.neg)

    def __pos__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(std.pos)

    def __invert__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(std.invert)

    def __sub__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.sub)

    def __truediv__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.truediv)

    def __floordiv__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.floordiv)

    def __mod__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.mod)

    def __pow__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.pow)

    def __lshift__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.lshift)

    def __rshift__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.rshift)

    def __and__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.and_)

    def __or__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.or_)

    def __xor__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.xor)

    def __matmul__(
        self, other: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, std.matmul)

    def __getitem__(
        self, item: OperatorInjectionMixin
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(item, std.getitem)


def _build_operation(
    operation: fr.schemas.LabeledRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag | None,
    *sources: OperatorInjectionMixin,
) -> atomic.Atomic:
    from pyiron_workflow._wfms import atomic  # noqa: PLC0415

    if not isinstance(operation.node, fr.schemas.AtomicRecipe):  # pragma: no cover
        raise TypeError(
            f"Can't inject non-atomic recipe as an operator {operation.node!r}."
            f"This should be unreachable, and is a fallback in case injection gets "
            f"extended later."
        )

    label = (
        label_helpers.unique_suffix(label, context_graph.nodes)
        if context_graph
        else label
    )
    operation_node = atomic.Atomic(label, operation.node)
    operation_node.connect_input(*[s._injection.port for s in sources])

    return operation_node


def _build_injection_graph(
    operation: fr.schemas.LabeledRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag | None,
    *sources: OperatorInjectionMixin,
) -> workflow.Workflow:
    from pyiron_workflow._wfms import workflow  # noqa: PLC0415

    graph = workflow.Workflow(label)

    # Add the operation and wire its outputs to graph outputs
    operation_node = _build_operation(operation, label, None)
    graph.add_node(operation_node)

    for port_label, oport in operation_node.outputs.items():
        graph.create_output(
            port_label,
            type_hint=oport.type_hint,
            type_metadata=oport.type_metadata,
        )
        graph.connect(oport, graph.outputs[port_label])

    negotiated_source_ports: list[Port] = []
    for source in sources:
        source_port = source._injection.port
        source_node = source_port.owner
        if context_graph is not None and (
            source_node is context_graph or source_node in context_graph.nodes.values()
        ):
            # Create a new input to accept the source, and wire graph and child inputs
            port_label = source._injection.label
            graph.create_input(
                port_label,
                type_hint=source_port.type_hint,
                type_metadata=source_port.type_metadata,
            )
            negotiated_source_ports.append(graph.inputs[port_label])
            graph.connect_input(**{port_label: source_port})
        elif source_node.owner is None:
            # Add the source node to the new graph and wire its inputs from graph inputs.
            # Capture any pending connections *before* add_node would try to realize them
            # against the wrong context, so they can be lifted onto the new graph.
            lifted = source_node.detach_pending_connections()
            source_node.label = label_helpers.unique_suffix(
                source_node.label, graph.nodes
            )
            graph.add_node(source_node)
            negotiated_source_ports.append(source_port)

            for iport_label, iport in source_node.inputs.items():
                # Without an owner we always need to scope input ports by the (now
                # forced-unique inside the new graph context) node label
                port_label = f"{source_node.label}_{iport_label}"
                graph.create_input(
                    port_label,
                    type_hint=iport.type_hint,
                    type_metadata=iport.type_metadata,
                )
                graph.connect(graph.inputs[port_label], iport)
                if iport_label in lifted:
                    # This input was fed from an outer context; re-register the edge as a
                    # pending connection on the new (still unparented) graph so it resolves
                    # when the new graph is finally attached to that context.
                    graph.connect_input(**{port_label: lifted[iport_label]})
        else:  # pragma: no cover
            raise ValueError(
                "Can't inject across graph contexts. Fallback exception; should not be "
                "reachable."
            )

    operation_node.connect_input(*negotiated_source_ports)

    return graph
