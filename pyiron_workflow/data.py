from __future__ import annotations

import dataclasses

from flowrep.api.schemas import NOT_DATA as NOT_DATA
from flowrep.api.schemas import NotData as NotData


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
