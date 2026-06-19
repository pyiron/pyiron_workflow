from typing import ClassVar

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow._wfms import atomic


class Transform1toN:
    input_label: ClassVar[fr.schemas.Label] = "items"

    @staticmethod
    def output_label(i: int) -> fr.schemas.Label:
        return f"output_{i}"

    @staticmethod
    def iterable_to_outputs(items, /):
        return tuple(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> fr.schemas.AtomicRecipe:
        return fr.schemas.AtomicRecipe(
            reference=fr.schemas.PythonReference(
                info=versions.VersionInfo.of(self.iterable_to_outputs),
                restricted_input_kinds={
                    self.input_label: fr.schemas.RestrictedParamKind.POSITIONAL_ONLY
                },
            ),
            inputs=[self.input_label],
            outputs=[self.output_label(i) for i in range(self.n)],
            unpack_mode=fr.schemas.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: fr.schemas.Label,
    ) -> atomic.Atomic:
        return atomic.Atomic(label, self.recipe)


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
            unpack_mode=fr.schemas.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: fr.schemas.Label,
    ) -> atomic.Atomic:
        return atomic.Atomic(label, self.recipe)
