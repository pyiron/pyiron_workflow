from collections.abc import Callable
from typing import Any, ClassVar

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow import atomic_node


class Transform1toN:
    input_label: ClassVar[fr.schemas.Label] = "items"

    @staticmethod
    def output_label(i: int) -> fr.schemas.Label:
        return f"output_{i}"

    @staticmethod
    def iterable_to_outputs(items, /):
        return tuple(items)

    @staticmethod
    def iterable_to_output(items, /):
        return items[0]

    def __init__(self, n: int):
        if n < 1:
            raise ValueError(f"Cannot scatter into {n} outputs; need at least 1.")
        self.n = n

    @property
    def _function(self) -> Callable[[Any], Any]:
        """
        A recipe declaring exactly one output receives the whole return value,
        so a 1-wide scatter must return the element rather than a 1-tuple.
        """
        return self.iterable_to_output if self.n == 1 else self.iterable_to_outputs

    @property
    def recipe(self) -> fr.schemas.AtomicRecipe:
        return fr.schemas.AtomicRecipe(
            reference=fr.schemas.PythonReference(
                info=versions.VersionInfo.of(self._function),
                restricted_input_kinds={
                    self.input_label: fr.schemas.RestrictedParamKind.POSITIONAL_ONLY
                },
            ),
            inputs=[self.input_label],
            outputs=[self.output_label(i) for i in range(self.n)],
        )

    def node(
        self,
        label: fr.schemas.Label,
    ) -> atomic_node.Atomic:
        return atomic_node.Atomic(self.recipe, label)


class TransformNto1:
    output_label: ClassVar[fr.schemas.Label] = "output_0"

    @staticmethod
    def input_label(i: int) -> fr.schemas.Label:
        return f"item_{i}"

    @staticmethod
    def inputs_to_list(*items):
        return list(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> fr.schemas.AtomicRecipe:
        return fr.schemas.AtomicRecipe(
            reference=fr.schemas.PythonReference(
                info=versions.VersionInfo.of(self.inputs_to_list),
                restricted_input_kinds={
                    self.input_label(i): fr.schemas.RestrictedParamKind.POSITIONAL_ONLY
                    for i in range(self.n)
                },
            ),
            inputs=[self.input_label(i) for i in range(self.n)],
            outputs=[self.output_label],
        )

    def node(
        self,
        label: fr.schemas.Label,
    ) -> atomic_node.Atomic:
        return atomic_node.Atomic(self.recipe, label)
