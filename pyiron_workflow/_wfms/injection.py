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
        if context.graph is None:
            raise ValueError(
                f"Cannot perform unary injection on {context.lexical_path!r} because it "
                f"has no  with no graph context."
            )

        operation = constructors.node(
            operation.node, label=f"{operation.label}_{context.label}"
        )
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
