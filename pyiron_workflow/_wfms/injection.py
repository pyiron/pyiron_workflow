from __future__ import annotations

import abc
from typing import TYPE_CHECKING, NamedTuple

import flowrep as fr
from flowrep.parsers import label_helpers

from pyiron_workflow._wfms import lexical, std

if TYPE_CHECKING:
    from pyiron_workflow._wfms import workflow
    from pyiron_workflow._wfms.datatypes import MutableDag, Node, Port


class InjectionContext(NamedTuple):
    port: Port
    node: Node
    graph: MutableDag | None
    lexical_path: lexical.LexicalPath
    label: fr.schemas.Label


class OperatorInjectionMixin(abc.ABC):

    @abc.abstractmethod
    def _injection_context(self) -> MutableDag | None: ...

    @abc.abstractmethod
    def _injection_label(self) -> fr.schemas.Label: ...

    @abc.abstractmethod
    def _injection_lexical_path(self) -> lexical.LexicalPath: ...

    @abc.abstractmethod
    def _injection_port(self) -> Port: ...

    def _unary_operation(
        self, operation: fr.schemas.LabeledRecipe
    ) -> workflow.Workflow:
        context = self._injection_context()
        return _build_injection_graph(
            operation,
            f"{operation.label}_{self._injection_label()}",
            context,
            self,
        )

    def _binary_operations(
        self, other: OperatorInjectionMixin, operation: fr.schemas.LabeledRecipe
    ) -> workflow.Workflow:
        self_context = self._injection_context()
        other_context = other._injection_context()
        context_graph = self_context or other_context

        label = (
            f"{self._injection_label()}_{operation.label}_{other._injection_label()}"
        )

        if self_context and other_context and self_context is not other_context:
            raise ValueError(
                f"Can't inject across graph contexts. "
                f"{self._injection_lexical_path()!r} cannot inject operation "
                f"{operation.label!r} with {other._injection_lexical_path()!r} because "
                "of mis-matched owners."
            )

        return _build_injection_graph(
            operation,
            label,
            context_graph,
            self,
            other,
        )

    def __abs__(self) -> workflow.Workflow:
        return self._unary_operation(std.abs)

    def __add__(self, other: OperatorInjectionMixin) -> workflow.Workflow:
        return self._binary_operations(other, std.add)

    def __mul__(self, other: OperatorInjectionMixin) -> workflow.Workflow:
        return self._binary_operations(other, std.mul)


def _build_operation(
    operation: fr.schemas.LabeledRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag,
    *sources: OperatorInjectionMixin,
) -> Node:
    from pyiron_workflow._wfms import constructors  # noqa: PLC0415

    operation_node = constructors.node(
        operation.node,
        label=label_helpers.unique_suffix(label, context_graph.nodes),
    )
    operation_node.connect_input(*[s._injection_port() for s in sources])

    return operation_node


def _build_injection_graph(
    operation: fr.schemas.LabeledRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag | None,
    *sources: OperatorInjectionMixin,
) -> workflow.Workflow:
    from pyiron_workflow._wfms import constructors, workflow  # noqa: PLC0415

    graph = workflow.Workflow(label)

    # Add the operation and wire its outputs to graph outputs
    operation_node = constructors.node(operation.node, label=operation.label)
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
        source_port = source._injection_port()
        source_node = source_port.owner
        if context_graph is not None and (
            source_node is context_graph or source_node in context_graph.nodes.values()
        ):
            # Create a new input to accept the source, and wire graph and child inputs
            port_label = source._injection_label()
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
                port_label = f"{source._injection_label()}_{iport_label}"
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
