"""
For interface specification.

There are cases where we want to be able to depend on the presence of a particular
attribute or method, but care very little about its details -- nothing beyond, perhaps,
type hints. These mixins allow us to guarantee the presence of such interfaces while
leaving their actual implementation up to other classes in order to have the weakest
possible coupling between different components of a composed class.
"""

from abc import ABC, abstractmethod


class HasLabel(ABC):
    """
    A mixin to guarantee the label interface exists.
    """

    @property
    @abstractmethod
    def label(self) -> str:
        """A label for the object."""
