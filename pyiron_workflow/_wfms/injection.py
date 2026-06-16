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
    def _injection_context(self) -> InjectionContext: ...

    def _unary_operation(
        self, operation: fr.schemas.LabeledRecipe
    ) -> workflow.Workflow:
        context = self._injection_context()
        return _build_injection_graph(
            operation,
            f"{operation.label}_{context.label}",
            context.graph,
            context.port,
        )

    def _binary_operations(
        self, other: OperatorInjectionMixin, operation: fr.schemas.LabeledRecipe
    ) -> workflow.Workflow:
        self_context = self._injection_context()
        other_context = other._injection_context()
        context_graph = self_context.graph or other_context.graph

        if (
            self_context.graph
            and other_context.graph
            and self_context.graph is not other_context.graph
        ):
            raise ValueError(
                f"Can't inject across graph contexts. {self_context.lexical_path!r} "
                f"cannot inject operation {operation.label!r} with "
                f"{other_context.lexical_path!r} because of mis-matched owners."
            )

        label = f"{self_context.label}_{operation.label}_{other_context.label}"

        return _build_injection_graph(
            operation,
            label,
            context_graph,
            self_context.port,
            other_context.port,
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
    *sources: Port,
) -> Node:
    from pyiron_workflow._wfms import constructors  # noqa: PLC0415

    operation_node = constructors.node(
        operation.node,
        label=label_helpers.unique_suffix(label, context_graph.nodes),
    )
    operation_node.connect_input(*sources)

    return operation_node


def _build_injection_graph(
    operation: fr.schemas.LabeledRecipe,
    label: fr.schemas.Label,
    context_graph: MutableDag | None,
    *sources: Port,
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

    negotiated_sources: list[Port] = []
    for source in sources:
        source_node = source.owner
        if context_graph is not None and (
            source_node is context_graph or source_node in context_graph.nodes.values()
        ):
            # Create a new input to accept the source, and wire graph and child inputs
            port_label = f"{source_node.label}_{source.label}"
            graph.create_input(
                port_label,
                type_hint=source.type_hint,
                type_metadata=source.type_metadata,
            )
            negotiated_sources.append(graph.inputs[port_label])
            graph.connect_input(**{port_label: source})
        elif source_node.owner is None:
            # Add the source node to the new graph and wire its inputs from graph inputs.
            # Capture any pending connections *before* add_node would try to realize them
            # against the wrong context, so they can be lifted onto the new graph.
            lifted = source_node.detach_pending_connections()
            source_node.label = label_helpers.unique_suffix(
                source_node.label, graph.nodes
            )
            graph.add_node(source_node)
            negotiated_sources.append(source)

            for iport_label, iport in source_node.inputs.items():
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

    operation_node.connect_input(*negotiated_sources)

    return graph
