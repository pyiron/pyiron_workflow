"""
This module provides a mixing class for ports and (single-output) nodes to produce
_new nodes_ when they are subjected to an operation.

Binary operations are permitted between two such injectable objects, or between an
injectable object and a JSONable literal -- the literal is wrapped in a `Constant`
source node. This holds for both operand orders (e.g. `port - 2` and `2 - port`) via
the reflected (`__r*__`) dunders.

Not _all_ operations are injectable. Notably absent are comparators (e.g. `==`), which
are reserved for use by the actual port/node objects themselves (e.g. to investigate
membership in collections), and reflexive operations (e.g. `*=`), which don't make
sense in a graph paradigm (E.g., `wf.some_node.inputs.foo *= wf.inputs.bar`?!).
"""

from __future__ import annotations

import abc
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

import flowrep as fr
from flowrep.parsers import label_helpers

from pyiron_workflow._wfms import lexical

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
        self,
        operation: fr.schemas.AtomicRecipe,
        operation_label: fr.schemas.Label,
    ) -> atomic.Atomic | workflow.Workflow:
        context_graph = self._injection.graph
        label = f"{operation_label}_{self._injection.label}"
        return _build_injection_graph(
            operation,
            label,
            context_graph,
            self,
        )

    def _coerce_injectable(
        self,
        other: OperatorInjectionMixin | fr.schemas.JSONABLE,
        operation_label: fr.schemas.Label,
    ) -> OperatorInjectionMixin:
        """Resolve a binary operand into an injectable, wrapping JSONable
        constants in a fresh :class:`~constant.Constant` source node."""
        if fr.tools.is_jsonable(other):
            from pyiron_workflow._wfms import constant  # noqa: PLC0415

            return constant.Constant.from_value(other)
        elif isinstance(other, OperatorInjectionMixin):
            return other
        else:
            raise TypeError(
                f"Injection operation {operation_label!r} on "
                f"{self._injection.lexical_path!r} expected another injectable object "
                f"or a JSONable constant, but got {other!r}"
            )

    def _binary_operations(
        self,
        other: OperatorInjectionMixin | fr.schemas.JSONABLE,
        operation: fr.schemas.AtomicRecipe,
        operation_label: fr.schemas.Label,
    ) -> atomic.Atomic | workflow.Workflow:
        return self._dispatch_binary(other, operation, operation_label, reflected=False)

    def _reflected_binary_operations(
        self,
        other: OperatorInjectionMixin | fr.schemas.JSONABLE,
        operation: fr.schemas.AtomicRecipe,
        operation_label: fr.schemas.Label,
    ) -> atomic.Atomic | workflow.Workflow:
        """Reflected (right-hand) form: builds ``operation(other, self)`` so that
        e.g. ``2 - port`` computes ``sub(2, port)`` rather than ``sub(port, 2)``."""
        return self._dispatch_binary(other, operation, operation_label, reflected=True)

    def _dispatch_binary(
        self,
        other: OperatorInjectionMixin | fr.schemas.JSONABLE,
        operation: fr.schemas.AtomicRecipe,
        operation_label: fr.schemas.Label,
        *,
        reflected: bool,
    ) -> atomic.Atomic | workflow.Workflow:
        other_injectable = self._coerce_injectable(other, operation_label)

        self_context = self._injection.graph
        other_context = other_injectable._injection.graph
        context_graph = self_context or other_context

        if self_context and other_context and self_context is not other_context:
            raise ValueError(
                f"Can't inject across graph contexts. "
                f"{self._injection.lexical_path!r} cannot inject operation "
                f"{operation.__name__!r} with {other_injectable._injection.lexical_path!r} because "
                "of mis-matched owners."
            )

        first, second = (
            (other_injectable, self) if reflected else (self, other_injectable)
        )
        label = f"{first._injection.label}_{operation_label}_{second._injection.label}"
        return _build_injection_graph(operation, label, context_graph, first, second)

    def __abs__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(fr.std.abs, "abs")

    def __add__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.add, "add")

    def __radd__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.add, "add")

    def __mul__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.mul, "mul")

    def __rmul__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.mul, "mul")

    def __neg__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(fr.std.neg, "neg")

    def __pos__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(fr.std.pos, "pos")

    def __invert__(self) -> atomic.Atomic | workflow.Workflow:
        return self._unary_operation(fr.std.invert, "invert")

    def __sub__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.sub, "sub")

    def __rsub__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.sub, "sub")

    def __truediv__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.truediv, "truediv")

    def __rtruediv__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.truediv, "truediv")

    def __floordiv__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.floordiv, "floordiv")

    def __rfloordiv__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.floordiv, "floordiv")

    def __mod__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.mod, "mod")

    def __rmod__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.mod, "mod")

    def __pow__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.pow, "pow")

    def __rpow__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.pow, "pow")

    def __lshift__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.lshift, "lshift")

    def __rlshift__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.lshift, "lshift")

    def __rshift__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.rshift, "rshift")

    def __rrshift__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.rshift, "rshift")

    def __and__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.and_, "and")

    def __rand__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.and_, "and")

    def __or__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.or_, "or")

    def __ror__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.or_, "or")

    def __xor__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.xor, "xor")

    def __rxor__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.xor, "xor")

    def __matmul__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(other, fr.std.matmul, "matmul")

    def __rmatmul__(
        self, other: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._reflected_binary_operations(other, fr.std.matmul, "matmul")

    def __getitem__(
        self, item: OperatorInjectionMixin | fr.schemas.JSONABLE
    ) -> atomic.Atomic | workflow.Workflow:
        return self._binary_operations(item, fr.std.getitem, "getitem")


class _HasRecipe(Protocol):
    __name__: str
    flowrep_recipe: fr.schemas.AtomicRecipe


def _build_operation(
    operation: _HasRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag | None,
    *sources: OperatorInjectionMixin,
) -> atomic.Atomic:
    from pyiron_workflow._wfms import atomic  # noqa: PLC0415

    if not isinstance(
        operation.flowrep_recipe, fr.schemas.AtomicRecipe
    ):  # pragma: no cover
        raise TypeError(
            f"Can't inject non-atomic recipe as an operator {operation.__name__!r}."
            f"This should be unreachable, and is a fallback in case injection gets "
            f"extended later."
        )

    label = (
        label_helpers.unique_suffix(label, context_graph.nodes)
        if context_graph
        else label
    )
    operation_node = atomic.Atomic(operation.flowrep_recipe, label)
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
