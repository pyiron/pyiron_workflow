from __future__ import annotations

from abc import ABC
from concurrent.futures import Executor
from functools import lru_cache
import itertools
import math
from typing import Any, ClassVar, Literal, Optional

from pandas import DataFrame
from pyiron_snippets.factory import classfactory

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.nodes.static_io import StaticNode
from pyiron_workflow.nodes.transform import (
    inputs_to_dict,
    inputs_to_dataframe,
    InputsToDict,
)


def dictionary_to_index_maps(
    data: dict,
    nested_keys: Optional[list[str] | tuple[str, ...]] = None,
    zipped_keys: Optional[list[str] | tuple[str, ...]] = None,
):
    """
    Given a dictionary where some data is iterable, and list(s) of keys over
    which to make a nested and/or zipped loop, return dictionaries mapping
    these keys to all the indices of the data they hold. Zipped loops are
    nested outside the nesting loops.

    Args:
        data (dict): The dictionary of data, some of which must me iterable.
        nested_keys (tuple[str, ...] | None): The keys whose data to make a
            nested for-loop over.
        zipped_keys (tuple[str, ...] | None): The keys whose data to make a
            zipped for-loop over.

    Returns:
        (tuple[dict[..., int], ...]): A tuple of dictionaries where each item
            maps the dictionary key to an index for that key's value.

    Raises:
        (KeyError): If any of the provided keys are not keys of the provided
            dictionary.
        (TypeError): If any of the data held in a provided key does cannot be
            operated on with `len`.
        (ValueError): If neither set of keys to iterate on is provided, or if
            all values being iterated over have a length of zero.
    """

    try:
        nested_data_lengths = (
            []
            if (nested_keys is None or len(nested_keys) == 0)
            else list(len(data[key]) for key in nested_keys)
        )
    except TypeError as e:
        raise TypeError(
            f"Could not parse nested lengths -- Does one of the keys {nested_keys} "
            f"have non-iterable data?"
        ) from e
    n_nest = math.prod(nested_data_lengths) if len(nested_data_lengths) > 0 else 0

    try:
        n_zip = (
            0
            if (zipped_keys is None or len(zipped_keys) == 0)
            else min(len(data[key]) for key in zipped_keys)
        )
    except TypeError as e:
        raise TypeError(
            f"Could not parse zipped lengths -- Does one of the keys {zipped_keys} "
            f"have non-iterable data?"
        ) from e

    def nested_generator():
        return itertools.product(*[range(n) for n in nested_data_lengths])

    def nested_index_map(nested_indices):
        return {
            nested_keys[i_key]: nested_index
            for i_key, nested_index in enumerate(nested_indices)
        }

    def zipped_generator():
        return range(n_zip)

    def zipped_index_map(zipped_index):
        return {key: zipped_index for key in zipped_keys}

    def merge(d1, d2):
        d1.update(d2)
        return d1

    if n_nest > 0 and n_zip > 0:
        key_index_maps = tuple(
            merge(nested_index_map(nested_indices), zipped_index_map(zipped_index))
            for nested_indices, zipped_index in itertools.product(
                nested_generator(), zipped_generator()
            )
        )
    elif n_nest > 0:
        key_index_maps = tuple(
            nested_index_map(nested_indices) for nested_indices in nested_generator()
        )
    elif n_zip > 0:
        key_index_maps = tuple(
            zipped_index_map(zipped_index) for zipped_index in zipped_generator()
        )
    else:
        if nested_keys is None and zipped_keys is None:
            raise ValueError(
                "At least one of `nested_keys` or `zipped_keys` must be specified."
            )
        else:
            raise ValueError(
                "Received keys to iterate over, but all values had length 0."
            )

    return key_index_maps


class UnmappedConflictError(ValueError):
    """
    When a for-node gets a body whose output label conflicts with looped a input
    label and no map was provided to avoid this.
    """


class MapsToNonexistentOutputError(ValueError):
    """
    When a for-node tries to map body node output channels that don't exist.
    """


class For(Composite, StaticNode, ABC):
    """
    Specifies fixed fields of some other node class to iterate over, but allows the
    length of looped input to vary by dynamically destroying and recreating (most of)
    its subgraph at run-time.

    Collects looped output and collates them with looped input values in a dataframe.

    The :attr:`body_node_executor` gets applied to each body node instance on each
    run.
    """

    _body_node_class: ClassVar[type[StaticNode]]
    _iter_on: ClassVar[tuple[str, ...]] = ()
    _zip_on: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, output_column_map=None, **kwargs):
        super().__init_subclass__(**kwargs)

        unmapped_conflicts = (
            set(cls._body_node_class.preview_inputs().keys())
            .intersection(cls._iter_on + cls._zip_on)
            .intersection(cls._body_node_class.preview_outputs().keys())
            .difference(() if output_column_map is None else output_column_map.keys())
        )
        if len(unmapped_conflicts) > 0:
            raise UnmappedConflictError(
                f"The body node {cls._body_node_class.__name__} has channel labels "
                f"{unmapped_conflicts} that appear as both (looped) input _and_ output "
                f"for {cls.__name__}. All such channels require a map to produce new, "
                f"unique column names for the output."
            )

        maps_to_nonexistent_output = set(
            {} if output_column_map is None else output_column_map.keys()
        ).difference(cls._body_node_class.preview_outputs().keys())
        if len(maps_to_nonexistent_output) > 0:
            raise MapsToNonexistentOutputError(
                f"{cls.__name__} tried to map body node output(s) "
                f"{maps_to_nonexistent_output} to new column names, but "
                f"{cls._body_node_class.__name__} has no such outputs."
            )

        cls._output_column_map = output_column_map

    @classmethod
    @property
    @lru_cache(maxsize=1)
    def output_column_map(cls) -> dict[str, str]:
        """
        How to transform body node output labels to dataframe column names.
        """
        map_ = {k: k for k in cls._body_node_class.preview_outputs().keys()}
        overrides = {} if cls._output_column_map is None else cls._output_column_map
        for body_label, column_name in overrides.items():
            map_[body_label] = column_name
        return map_

    def __init__(
        self,
        *args,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        overwrite_save: bool = False,
        run_after_init: bool = False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        strict_naming: bool = True,
        body_node_executor: Optional[Executor] = None,
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
            strict_naming=strict_naming,
            **kwargs,
        )
        self.body_node_executor = None

    def _setup_node(self) -> None:
        super()._setup_node()
        input_nodes = []
        for channel in self.inputs:
            n = self.create.standard.UserInput(
                channel.default, label=channel.label, parent=self
            )
            n.inputs.user_input.type_hint = channel.type_hint
            channel.value_receiver = n.inputs.user_input
            input_nodes.append(n)
        self.starting_nodes = input_nodes
        self._input_node_labels = tuple(n.label for n in input_nodes)

    def on_run(self):
        self._build_body()
        return super().on_run()

    def _build_body(self):
        """
        Construct instances of the body node based on input length, and wire them to IO.
        """
        iter_maps = dictionary_to_index_maps(
            self.inputs.to_value_dict(),
            nested_keys=self._iter_on,
            zipped_keys=self._zip_on,
        )

        self._clean_existing_subgraph()

        self.dataframe = inputs_to_dataframe(len(iter_maps))
        self.dataframe.outputs.df.value_receiver = self.outputs.df

        for n, channel_map in enumerate(iter_maps):
            body_node = self._body_node_class(label=f"body_{n}", parent=self)
            body_node.executor = self.body_node_executor
            row_collector = self._build_collector_node(n)

            self._connect_broadcast_input(body_node)
            for label, i in channel_map.items():
                self._connect_looped_input(body_node, row_collector, label, i)

            self._collect_output_from_body(body_node, row_collector)

            self.dataframe.inputs[f"row_{n}"] = row_collector

        self.set_run_signals_to_dag_execution()

    def _clean_existing_subgraph(self):
        for label in self.child_labels:
            if label not in self._input_node_labels:
                self.remove_child(label)
            else:
                # Re-run the user input node so it has up-to-date output, otherwise
                # when we inject a getitem node -- which will try to run automatically
                # -- it will see data it can work with, but if that data happens to
                # have the wrong length it may successfully auto-run on the wrong thing
                # and throw an error!
                self.children[label].run(
                    run_data_tree=False,
                    run_parent_trees_too=False,
                    fetch_input=False,
                    # Data should simply be coming from the value link
                    # We just want to refresh the output
                )
        # TODO: Instead of deleting _everything_ each time, try and re-use stuff

    def _build_collector_node(self, row_number):
        # Iterated inputs
        row_specification = {
            key: (self._body_node_class.preview_inputs()[key][0], NOT_DATA)
            for key in self._iter_on + self._zip_on
        }
        # Outputs
        row_specification.update(
            {
                self.output_column_map[key]: (hint, NOT_DATA)
                for key, hint in self._body_node_class.preview_outputs().items()
            }
        )
        return inputs_to_dict(
            row_specification, parent=self, label=f"row_collector_{row_number}"
        )

    def _connect_broadcast_input(self, body_node: StaticNode) -> None:
        """Connect broadcast macro input to each body node."""
        for broadcast_label in set(self.preview_inputs().keys()).difference(
            self._iter_on + self._zip_on
        ):
            self.inputs[broadcast_label].value_receiver = body_node.inputs[
                broadcast_label
            ]

    def _connect_looped_input(
        self,
        body_node: StaticNode,
        row_collector: InputsToDict,
        looped_input_label: str,
        i: int,
    ) -> None:
        """Get item from macro input and connect it to body and collector nodes."""
        index_node = self.children[looped_input_label][i]  # Inject getitem node
        body_node.inputs[looped_input_label] = index_node
        row_collector.inputs[looped_input_label] = index_node

    def _collect_output_from_body(
        self, body_node: StaticNode, row_collector: InputsToDict
    ) -> None:
        """Pass body node output to the collector node."""
        for label, body_out in body_node.outputs.items():
            row_collector.inputs[self.output_column_map[label]] = body_out

    @classmethod
    @lru_cache(maxsize=1)
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        preview = {}
        for label, (hint, default) in cls._body_node_class.preview_inputs().items():
            # TODO: Leverage hint and default, listing if it's looped on
            if label in cls._zip_on + cls._iter_on:
                hint = list if hint is None else list[hint]
                default = NOT_DATA  # TODO: Figure out a generator pattern to get lists
            preview[label] = (hint, default)
        return preview

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return {"df": DataFrame}

    @property
    def _input_value_links(self):
        """
        Value connections between child output and macro in string representation based
        on labels.

        The string representation helps storage, and having it as a property ensures
        the name is protected.
        """
        # Duplicated value linking behaviour from Macro class
        return [
            (c.label, (c.value_receiver.owner.label, c.value_receiver.label))
            for c in self.inputs
        ]

    @property
    def _output_value_links(self):
        """
        Value connections between macro and child input in string representation based
        on labels.

        The string representation helps storage, and having it as a property ensures
        the name is protected.
        """
        # Duplicated value linking behaviour from Macro class
        return [
            ((c.owner.label, c.label), c.value_receiver.label)
            for child in self
            for c in child.outputs
            if c.value_receiver is not None
        ]

    def __getstate__(self):
        state = super().__getstate__()
        state["body_node_executor"] = None

        # Duplicated value linking behaviour from Macro class
        state["_input_value_links"] = self._input_value_links
        state["_output_value_links"] = self._output_value_links

        return state

    def _get_state_from_remote_other(self, other_self):
        state = super()._get_state_from_remote_other(other_self)
        state.pop("body_node_executor")  # Got overridden to None for __getstate__,
        # so keep local
        return state

    def __setstate__(self, state):
        # Duplicated value linking behaviour from Macro class
        # Purge value links from the state
        input_links = state.pop("_input_value_links")
        output_links = state.pop("_output_value_links")

        super().__setstate__(state)

        # Re-forge value links
        for inp, (child, child_inp) in input_links:
            self.inputs[inp].value_receiver = self.children[child].inputs[child_inp]

        for (child, child_out), out in output_links:
            self.children[child].outputs[child_out].value_receiver = self.outputs[out]


def _for_node_class_name(
    body_node_class: type[StaticNode], iter_on: tuple[str, ...], zip_on: tuple[str, ...]
):
    iter_fields = (
        "" if len(iter_on) == 0 else "Iter" + "".join(k.title() for k in iter_on)
    )
    zip_fields = "" if len(zip_on) == 0 else "Zip" + "".join(k.title() for k in zip_on)
    return f"{For.__name__}{body_node_class.__name__}{iter_fields}{zip_fields}"


@classfactory
def for_node_factory(
    body_node_class: type[StaticNode],
    iter_on: tuple[str, ...] = (),
    zip_on: tuple[str, ...] = (),
    output_column_map: dict | None = None,
    use_cache: bool = True,
    /,
):
    combined_docstring = (
        "For node docstring:\n"
        + (For.__doc__ if For.__doc__ is not None else "")
        + "\nBody node docstring:\n"
        + (body_node_class.__doc__ if body_node_class.__doc__ is not None else "")
    )

    return (
        _for_node_class_name(body_node_class, iter_on, zip_on),
        (For,),
        {
            "_body_node_class": body_node_class,
            "_iter_on": iter_on,
            "_zip_on": zip_on,
            "__doc__": combined_docstring,
            "use_cache": use_cache,
        },
        {"output_column_map": output_column_map},
    )


def for_node(
    body_node_class,
    *node_args,
    iter_on=(),
    zip_on=(),
    output_column_map: Optional[dict[str, str]] = None,
    use_cache: bool = True,
    **node_kwargs,
):
    """
    Makes a new :class:`For` node which internally creates instances of the
    :param:`body_node_class` and loops input onto them in nested and/or zipped loop(s).

    Output is a single channel, `"df"`, which holds a :class:`pandas.DataFrame` whose
    rows couple (looped) input to their respective body node outputs.

    The internal node structure gets re-created each run, so the same inputs must
    consistently be iterated over, but their lengths can change freely.

    An executor can be applied to all body node instances at run-time by assigning it
    to the :attr:`body_node_executor` attribute of the for-node.

    Args:
        body_node_class type[StaticNode]: The class of node to loop on.
        *node_args: Regular positional node arguments.
        iter_on (tuple[str, ...]): Input labels in the :param:`body_node_class` to
            nested-loop on.
        zip_on (tuple[str, ...]): Input labels in the :param:`body_node_class` to
            zip-loop on.
        output_column_map (dict[str, str] | None): A map for generating dataframe
            column names (values) from body node output channel labels (keys).
            Necessary iff the body node has the same label for an output channel and
            an input channel being looped over. (Default is None, just use the output
            channel labels as columb names.)
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        **node_kwargs: Regular keyword node arguments.

    Returns:
        (For): An instance of a dynamically-subclassed :class:`For` node.

    Examples:
        >>> from pyiron_workflow import Workflow
        >>>
        >>> @Workflow.wrap.as_function_node("together")
        ... def FiveTogether(a: int, b: int, c: int, d: int, e: str = "foobar"):
        ...     return (a, b, c, d, e),
        >>>
        >>> for_instance = Workflow.create.for_node(
        ...     FiveTogether,
        ...     iter_on=("a", "b"),
        ...     zip_on=("c", "d"),
        ...     a=[1, 2],
        ...     b=[3, 4, 5, 6],
        ...     c=[7, 8],
        ...     d=[9, 10, 11],
        ...     e="e"
        ... )
        >>>
        >>> out = for_instance()
        >>> type(out.df)
        <class 'pandas.core.frame.DataFrame'>

        Internally, the loop node has made a bunch of body nodes, as well as nodes to
        index and collect data
        >>> len(for_instance)
        48

        We get one dataframe row for each possible combination of looped input
        >>> len(out.df)
        16

        We are stuck iterating on the fields we defined, but we can change the length
        of the input and the loop node's body will get reconstructed at run-time to
        accommodate this
        >>> out = for_instance(a=[1], b=[3], d=[7])
        >>> len(for_instance), len(out)
        (12, 1)

        Note that if we had simply returned each input individually, without any output
        labels on the node, we'd need to specify a map on the for-node so that the
        (looped) input and output columns on the resulting dataframe are all unique:
        >>> @Workflow.wrap.as_function_node()
        ... def FiveApart(a: int, b: int, c: int, d: int, e: str = "foobar"):
        ...     return a, b, c, d, e,
        >>>
        >>> for_instance = Workflow.create.for_node(
        ...     FiveApart,
        ...     iter_on=("a", "b"),
        ...     zip_on=("c", "d"),
        ...     a=[1, 2],
        ...     b=[3, 4, 5, 6],
        ...     c=[7, 8],
        ...     d=[9, 10, 11],
        ...     e="e",
        ...     output_column_map={
        ...         "a": "out_a",
        ...         "b": "out_b",
        ...         "c": "out_c",
        ...         "d": "out_d"
        ...     }
        ... )
        >>>
        >>> out = for_instance()
        >>> out.df.columns
        Index(['a', 'b', 'c', 'd', 'out_a', 'out_b', 'out_c', 'out_d', 'e'], dtype='object')

    """
    for_node_factory.clear(_for_node_class_name(body_node_class, iter_on, zip_on))
    cls = for_node_factory(
        body_node_class, iter_on, zip_on, output_column_map, use_cache
    )
    cls.preview_io()
    return cls(*node_args, **node_kwargs)
