from __future__ import annotations

from abc import ABC
from typing import Optional, Literal, TYPE_CHECKING

from pandas import DataFrame

from pyiron_workflow.channels import InputData
from pyiron_workflow.mixin.injection import (
    OutputsWithInjection,
    OutputDataWithInjection,
)
from pyiron_workflow.io import Inputs
from pyiron_workflow.mixin.preview import HasIOPreview
from pyiron_workflow.node import Node

if TYPE_CHECKING:
    from pyiron_workflow.nodes.composite import Composite


class StaticNode(Node, HasIOPreview, ABC):
    """
    A node whose IO specification is available at the class level.

    Actual IO is then constructed from the preview at instantiation.
    """

    def __init__(
        self,
        *args,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        **kwargs,
    ):
        super().__init__(
            *args,
            label=label,
            parent=parent,
            overwrite_save=overwrite_save,
            run_after_init=run_after_init,
            storage_backend=storage_backend,
            save_after_run=save_after_run,
            **kwargs,
        )

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

    def iter(
        self,
        body_node_executor=None,
        output_column_map: Optional[dict[str, str]] = None,
        **iterating_inputs,
    ) -> DataFrame:
        return self._loop(
            "iter_on",
            body_node_executor=body_node_executor,
            output_column_map=output_column_map,
            **iterating_inputs,
        )

    def zip(
        self,
        body_node_executor=None,
        output_column_map: Optional[dict[str, str]] = None,
        **iterating_inputs,
    ) -> DataFrame:
        return self._loop(
            "zip_on",
            body_node_executor=body_node_executor,
            output_column_map=output_column_map,
            **iterating_inputs,
        )

    def _loop(
        self,
        loop_style_key,
        body_node_executor=None,
        output_column_map=None,
        **looping_inputs,
    ):
        loop_on = tuple(looping_inputs.keys())
        self._guarantee_names_are_input_channels(loop_on)

        broadcast_inputs = {
            label: self.inputs[label].value
            for label in set(self.inputs.labels).difference(loop_on)
        }

        from pyiron_workflow.nodes.for_loop import for_node

        for_instance = for_node(
            self.__class__,
            **{
                loop_style_key: loop_on,
                "output_column_map": output_column_map,
                **looping_inputs,
                **broadcast_inputs,
            },
        )
        for_instance.body_node_executor = body_node_executor

        return for_instance().df

    def _guarantee_names_are_input_channels(self, presumed_input_keys: tuple[str]):
        non_input_kwargs = set(presumed_input_keys).difference(self.inputs.labels)
        if len(non_input_kwargs) > 0:
            raise ValueError(
                f"{self.full_label} cannot iterate on {non_input_kwargs} because "
                f"they are not among input channels {self.inputs.labels}"
            )
