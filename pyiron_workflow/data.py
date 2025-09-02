from __future__ import annotations

import dataclasses

from pyiron_snippets.singleton import Singleton


class NotData(metaclass=Singleton):
    """
    This class exists purely to initialize data channel values where no default value
    is provided; it lets the channel know that it has _no data in it_ and thus should
    not identify as ready.
    """

    @classmethod
    def __repr__(cls):
        # We use the class directly (not instances of it) where there is not yet data
        # So give it a decent repr, even as just a class
        return "NOT_DATA"

    def __reduce__(self):
        return "NOT_DATA"

    def __bool__(self):
        return False


NOT_DATA = NotData()


@dataclasses.dataclass
class SemantikonRecipeChange:
    """
    Represents a change in a Semantikon recipe, to be digested in order to modify a
    Semantikon workflow dictionary representation.

    Attributes:
        location (list[str]): The location within the recipe where the change occurs,
            described by a lexical path of node labels -- the user will insert other
            keys like `"nodes"` etc. into this path.
        new_edge (tuple[str, str]): The new edge being introduced, represented as a
            tuple of strings according to the Semantikon formalism..
        parent_input (str | None): The optional parent input associated with the edge.
        parent_output (str | None): The optional parent output associated with the edge.
    """

    location: list[str]
    new_edge: tuple[str, str]
    parent_input: str | None = None
    parent_output: str | None = None
