from typing import ClassVar

from flowrep.api import schemas as frs
from pyiron_snippets import versions

from pyiron_workflow._wfms import atomic


class Transform1toN:
    input_label: ClassVar[frs.Label] = "items"

    @staticmethod
    def output_label(i: int) -> frs.Label:
        return f"output_{i}"

    @staticmethod
    def iterable_to_outputs(items, /):
        return tuple(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> frs.AtomicRecipe:
        return frs.AtomicRecipe(
            reference=frs.PythonReference(
                info=versions.VersionInfo.of(self.iterable_to_outputs),
                restricted_input_kinds={
                    self.input_label: frs.RestrictedParamKind.POSITIONAL_ONLY
                },
            ),
            inputs=[self.input_label],
            outputs=[self.output_label(i) for i in range(self.n)],
            unpack_mode=frs.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: frs.Label,
    ) -> atomic.Atomic:
        return atomic.Atomic(label, self.recipe)


class TransformNto1:
    output_label: ClassVar[frs.Label] = "output_0"

    @staticmethod
    def input_label(i: int) -> frs.Label:
        return f"item_{i}"

    @staticmethod
    def inputs_to_list(*items):
        return list(items)

    def __init__(self, n: int):
        self.n = n

    @property
    def recipe(self) -> frs.AtomicRecipe:
        return frs.AtomicRecipe(
            reference=frs.PythonReference(
                info=versions.VersionInfo.of(self.inputs_to_list),
                restricted_input_kinds={
                    self.input_label(i): frs.RestrictedParamKind.POSITIONAL_ONLY
                    for i in range(self.n)
                },
            ),
            inputs=[self.input_label(i) for i in range(self.n)],
            outputs=[self.output_label],
            unpack_mode=frs.UnpackMode.TUPLE,
        )

    def node(
        self,
        label: frs.Label,
    ) -> atomic.Atomic:
        return atomic.Atomic(label, self.recipe)
