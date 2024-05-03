from __future__ import annotations

from abc import ABC
from functools import lru_cache
import itertools
import math
from typing import Any, ClassVar, Optional

from pandas import DataFrame

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.composite import Composite
from pyiron_workflow.io_preview import StaticNode
from pyiron_workflow.snippets.factory import classfactory
from pyiron_workflow.transform import inputs_to_dict, inputs_to_dataframe


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
        nested_data_lengths = [] if nested_keys is None else list(
            len(data[key]) for key in nested_keys
        )
    except TypeError as e:
        raise TypeError(
            f"Could not parse nested lengths -- Does one of the keys {nested_keys} "
            f"have non-iterable data?"
        ) from e
    n_nest = math.prod(nested_data_lengths) if len(nested_data_lengths) > 0 else 0

    try:
        n_zip = 0 if zipped_keys is None else min(
            len(data[key]) for key in zipped_keys
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
            for nested_indices, zipped_index
            in itertools.product(nested_generator(), zipped_generator())
        )
    elif n_nest > 0:
        key_index_maps = tuple(
            nested_index_map(nested_indices)
            for nested_indices
            in nested_generator()
        )
    elif n_zip > 0:
        key_index_maps = tuple(
            zipped_index_map(zipped_index)
            for zipped_index
            in zipped_generator()
        )
    else:
        if nested_keys is None and zipped_keys is None:
            raise ValueError(
                "At least one of `nested_keys` or `zipped_keys` must be specified.")
        else:
            raise ValueError(
                "Received keys to iterate over, but all values had length 0.")

    return key_index_maps


class For(Composite, StaticNode, ABC):
    _body_node_class: ClassVar[type[StaticNode]]
    _iter_on: ClassVar[tuple[str, ...]] = ()
    _zip_on: ClassVar[tuple[str, ...]] = ()

    _iter_label_prefix: ClassVar[str] = "iter_"
    _zip_label_prefix: ClassVar[str] = "zip_"

    def _setup_node(self) -> None:
        super()._setup_node()
        input_nodes = []
        for channel in self.inputs:
            n = self.create.standard.UserInput(
                channel.default,
                label=channel.label,
                parent=self
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
            zipped_keys=self._zip_on
        )

        for label in self.child_labels:
            if label not in self._input_node_labels:
                self.remove_child(label)
            else:
                # Re-run the user input node so it has up-to-date output, otherwise
                # when we inject a getitem node -- which will try to run automatically
                # -- it will see data it can work with, but if that data happens to
                # have the wrong length it may successfully auto-run on the wrong thing
                # and throw an error!
                self.children[label]()
        # TODO: Instead of deleting _everything_ each time, try and re-use stuff

        self.dataframe = inputs_to_dataframe(len(iter_maps))
        self.dataframe.outputs.df.value_receiver = self.outputs.df

        for n, channel_map in enumerate(iter_maps):
            body_node = self._body_node_class(label=f"body_{n}", parent=self)

            # Iterated inputs
            row_specification = {
                key: (self._body_node_class.preview_inputs()[key], NOT_DATA)
                for key in self._iter_on + self._zip_on
            }
            # Outputs
            row_specification.update(
                {
                    key: (hint, NOT_DATA)
                    for key, hint in self._body_node_class.preview_outputs().items()
                }
            )
            row_collector = inputs_to_dict(
                row_specification,
                parent=self,
                label=f"row_collector_{n}"
            )
            self.dataframe.inputs[f"row_{n}"] = row_collector
            for (label, body_out) in body_node.outputs.items():
                row_collector.inputs[label] = body_out

            # Wire up the looped input
            for looped_input_label, i in channel_map.items():
                index_node = self.children[looped_input_label][i]  # Inject getitem node
                body_node.inputs[looped_input_label] = index_node
                row_collector.inputs[looped_input_label] = index_node

            # Wire up the broadcast input
            for broadcast_label in set(self.preview_inputs().keys()).difference(
                self._iter_on + self._zip_on
            ):
                self.inputs[broadcast_label].value_receiver = body_node.inputs[broadcast_label]

            self.set_run_signals_to_dag_execution()

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


@classfactory
def for_node_factory(
    body_node_class: type[StaticNode],
    iter_on: tuple[str, ...] = (),
    zip_on: tuple[str, ...] = (),
    /
):
    # TODO: verify all iter and zips are in the body node input previews
    # TODO: verify iter and zip on are not intersecting
    iter_fields = (
                      "" if len(iter_on) == 0
                      else "Iter" + "".join(k.title() for k in iter_on)
    )
    zip_fields = "" if len(iter_on) == 0 else "Zip" + "".join(k.title() for k in zip_on)
    return (
        f"{For.__name__}{body_node_class.__name__}{iter_fields}{zip_fields}",
        (For,),
        {
            "_body_node_class": body_node_class,
            "_iter_on": iter_on,
            "_zip_on": zip_on,
        },
        {},
    )


def for_node(body_node_class, *node_args, iter_on=(), zip_on=(), **node_kwargs):
    """
    Makes a new :class:`For` node which internally creates instances of the
    :param:`body_node_class` and loops input onto them in nested and/or zipped loop(s).

    Output is a single channel, `"df"`, which holds a :class:`pandas.DataFrame` whose
    rows couple (looped) input to their respective body node outputs.

    The internal node structure gets re-created each run, so the same inputs must
    consistently be iterated over, but their lengths can change freely.

    Args:
        body_node_class type[StaticNode]: The class of node to loop on.
        *node_args: Regular positional node arguments.
        iter_on (tuple[str, ...]): Input labels in the :param:`body_node_class` to
            nested-loop on.
        zip_on (tuple[str, ...]): Input labels in the :param:`body_node_class` to
            zip-loop on.
        **node_kwargs: Regular keyword node arguments.

    Returns:
        (For): An instance of a dynamically-subclassed :class:`For` node.

    Examples:
        >>> from pyiron_workflow import Workflow
        >>>
        >>> @Workflow.wrap.as_function_node("together")
        ... def Three(a: int, b: int, c: int, d: int, e: str = "foobar"):
        ...     return (a, b, c, d, e),
        >>>
        >>> for_instance = Workflow.create.for_node(
        ...     Three,
        ...     iter_on=("a", "b"),
        ...     zip_on=("c", "d"),
        ...     a=[1, 2],
        ...     b=[3, 4, 5],
        ...     c=[6, 7],
        ...     d=[7, 8, 9],
        ...     e="e"
        ... )
        >>>
        >>> out = for_instance()
        >>> type(out.df)
        <class 'pandas.core.frame.DataFrame'>

    """
    cls = for_node_factory(body_node_class, iter_on, zip_on)
    cls.preview_io()
    return cls(*node_args, **node_kwargs)
