from __future__ import annotations

import abc
from typing import TYPE_CHECKING, NamedTuple

import flowrep as fr

from pyiron_workflow._wfms import lexical, std

if TYPE_CHECKING:
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

    def _unary_operation(self, operation: fr.schemas.LabeledRecipe) -> Node:
        from pyiron_workflow._wfms import constructors  # noqa: PLC0415

        context = self._injection_context()
        label = f"{operation.label}_{context.label}"
        if context.graph is None:
            from pyiron_workflow._wfms.workflow import Workflow  # noqa: PLC0415

            graph = Workflow(label)
            operation = constructors.node(operation.node, label=operation.label)
            graph.add_node(operation)
            graph.add_node(context.node)
            operation.connect_input(context.port)

            for port_label, iport in context.node.inputs.items():
                graph.create_input(
                    port_label,
                    type_hint=iport.type_hint,
                    type_metadata=iport.type_metadata,
                )
                graph.connect(graph.inputs[port_label], iport)

            for port_label, oport in operation.outputs.items():
                graph.create_output(
                    port_label,
                    type_hint=oport.type_hint,
                    type_metadata=oport.type_metadata,
                )
                graph.connect(oport, graph.outputs[label])
            return graph
        else:
            operation = constructors.node(operation.node, label=label)
            context.graph.add_node(operation)
            operation.connect_input(context.port)
            return operation

    def _binary_operations(
        self, other: OperatorInjectionMixin, operation: fr.schemas.LabeledRecipe
    ) -> Node:
        raise NotImplementedError()

    def __abs__(self) -> Node:
        return self._unary_operation(std.abs)

    def __add__(self, other: OperatorInjectionMixin) -> Node:
        return self._binary_operations(other, std.add)

    def __mul__(self, other: OperatorInjectionMixin) -> Node:
        return self._binary_operations(other, std.mul)
