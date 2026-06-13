from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import flowrep as fr

from pyiron_workflow._wfms import std

if TYPE_CHECKING:
    from pyiron_workflow._wfms.datatypes import MutableDag, Node, Port


class OperatorInjectionMixin(abc.ABC):
    label: fr.schemas.Label

    @abc.abstractmethod
    def _injectable_object(self) -> Port: ...

    @abc.abstractmethod
    def _injectable_object_owner(self) -> Node: ...

    @abc.abstractmethod
    def _injection_context(self) -> MutableDag | None: ...

    def _unary_operation(self, operation: fr.schemas.LabeledRecipe) -> Node:
        from pyiron_workflow._wfms import constructors  # noqa: PLC0415

        context = self._injection_context()
        if context is None:
            from pyiron_workflow._wfms import workflow  # noqa: PLC0415

            subgraph = workflow.Workflow(f"{operation.label}_{self.label}")
            subgraph.add_node(self._injectable_object_owner())
            operation = constructors.node(operation.node, label=operation.label)
            subgraph.connect(self._injectable_object(), operation._injectable_object())

            return subgraph
        else:
            operation = constructors.node(
                operation.node, label=f"{operation.label}_{self.label}"
            )
            context.add_node(operation)
            operation.connect_input(self._injectable_object())
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
