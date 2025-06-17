from __future__ import annotations

from abc import ABC

from pyiron_snippets.colors import SeabornColors

from pyiron_workflow.channels import InputData
from pyiron_workflow.io import Inputs
from pyiron_workflow.mixin.injection import (
    OutputDataWithInjection,
    OutputsWithInjection,
)
from pyiron_workflow.mixin.preview import HasIOPreview
from pyiron_workflow.node import Node


class StaticNode(Node, HasIOPreview, ABC):
    """
    A node whose IO specification is available at the class level.

    Actual IO is then constructed from the preview at instantiation.
    """

    def _setup_node(self) -> None:
        super()._setup_node()

        self._inputs = Inputs(
            *[
                InputData(
                    label=label,
                    owner=self,
                    default=default,
                    type_hint=type_hint,
                )
                for label, (type_hint, default) in self.preview_inputs().items()
            ]
        )

        self._outputs = OutputsWithInjection(
            *[
                OutputDataWithInjection(
                    label=label,
                    owner=self,
                    type_hint=hint,
                )
                for label, hint in self.preview_outputs().items()
            ]
        )

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> OutputsWithInjection:
        return self._outputs

    def display_state(self, state=None, ignore_private=True):
        state = dict(self.__getstate__()) if state is None else state
        self._make_entry_public(state, "_inputs", "inputs")
        self._make_entry_public(state, "_outputs", "outputs")
        self._make_entry_public(state, "_signals", "signals")
        return super().display_state(state=state, ignore_private=ignore_private)

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.pink
