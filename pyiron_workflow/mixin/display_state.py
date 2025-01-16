"""
Simple display capabilities to make it easier for humans to see what's happening.
"""

from abc import ABC
from json import dumps
from typing import Any

from pyiron_workflow.mixin.has_interface_mixins import UsesState


class HasStateDisplay(UsesState, ABC):
    """
    A mixin that leverages :meth:`__getstate__` to automatically build a half-decent
    JSON-compatible representation dictionary.

    Child classes can over-ride :meth:`display_state` to add or remove elements from
    the display dictionary, e.g. to (optionally) expose state elements that would
    otherwise be private or to show properties that are computed and not stored in
    state, or (mandatory -- JSON demands it) remove recursion from the state.

    Provides a :meth:`_repr_json_` method leveraging this beautified state dictionary
    to give a standard JSON representation in Jupyter notebooks.
    """

    def display_state(
        self, state: dict | None = None, ignore_private: bool = True
    ) -> dict[str, Any]:
        """
        A dictionary of JSON-compatible objects based on the object state (plus
        whatever modifications to the state the class designer has chosen to make).

        Anything that fails to dump to JSON gets cast as a string and then dumped.

        Args:
            state (dict|None): The starting state. Default is None which uses
                `__getstate__`, but available in case child classes want to first
                sanitize the state values.
            ignore_private (bool): Whether to ignore or include any state element
                whose key starts with `'_'`. Default is True, only show public data.

        Returns:
            dict:
        """
        display = dict(self.__getstate__()) if state is None else state
        to_del = []
        for k, v in display.items():
            if ignore_private and k.startswith("_"):
                to_del.append(k)
                continue

            if isinstance(v, HasStateDisplay):
                display[k] = v.display_state(ignore_private=ignore_private)
            else:
                try:
                    display[k] = dumps(v)
                except TypeError:
                    display[k] = dumps(str(v))

        for k in to_del:
            del display[k]

        return display

    def _repr_json_(self):
        return self.display_state()

    @staticmethod
    def _make_entry_public(state: dict, private_key: str, public_key: str):
        if private_key not in state:
            raise ValueError(
                f"Can't make {private_key} public, it was not found among "
                f"{list(state.keys())}"
            )
        if public_key in state:
            raise ValueError(
                f"Can't make {private_key} public, {public_key} is already a key in"
                f" the dict!"
            )
        state[public_key] = state[private_key]
        del state[private_key]
        return state
