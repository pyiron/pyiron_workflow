from __future__ import annotations

from abc import ABC

from pandas import DataFrame
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

    @classmethod
    def for_node(
        cls,
        *node_args,
        iter_on: tuple[str, ...] | str = (),
        zip_on: tuple[str, ...] | str = (),
        output_as_dataframe: bool = True,
        output_column_map: dict[str, str] | None = None,
        use_cache: bool = True,
        **node_kwargs,
    ):
        """
        A shortcut for creating for-node instances.

        Args:
            *node_args: Regular positional node arguments.
            iter_on (tuple[str, ...] | str): Input label(s) in the :param:`body_node_class`
                to nested-loop on.
            zip_on (tuple[str, ...] | str): Input label(s) in the :param:`body_node_class`
                to zip-loop on.
            output_as_dataframe (bool): Whether to package the output (and iterated input)
                as a dataframe, or leave them as individual lists. (Default is True,
                package as dataframe.)
            output_column_map (dict[str, str] | None): A map for generating dataframe
                column names (values) from body node output channel labels (keys).
                Necessary iff the body node has the same label for an output channel and
                an input channel being looped over. (Default is None, just use the output
                channel labels as columb names.)
            use_cache (bool): Whether this node should default to caching its values.
                (Default is True.)
            **node_kwargs: Regular keyword node arguments.

        Returns:
            (For): An instance of a dynamically-subclassed :class:`For` node using
                _this class_ as the `For.body_node_class`.

        Examples:
            >>> from pyiron_workflow import Workflow
            >>>
            >>> n = Workflow.create.standard.Add.for_node(
            ...     iter_on="other",
            ...     obj=1,
            ...     other=[1, 2],
            ...     output_as_dataframe=False,
            ... )
            >>>
            >>> out = n()
            >>> out.add
            [2, 3]

        """
        from pyiron_workflow.nodes.for_loop import for_node

        return for_node(
            cls,
            *node_args,
            iter_on=iter_on,
            zip_on=zip_on,
            output_as_dataframe=output_as_dataframe,
            output_column_map=output_column_map,
            use_cache=use_cache,
            **node_kwargs,
        )

    def iter(
        self,
        body_node_executor=None,
        output_column_map: dict[str, str] | None = None,
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
        output_column_map: dict[str, str] | None = None,
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
                "output_as_dataframe": True,  # These methods terminate at the user
                # So force the user-friendly dataframe output.
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
