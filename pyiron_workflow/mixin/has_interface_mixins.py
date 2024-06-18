"""
For interface specification.

There are cases where we want to be able to depend on the presence of a particular
attribute or method, but care very little about its details -- nothing beyond, perhaps,
type hints. These mixins allow us to guarantee the presence of such interfaces while
leaving their actual implementation up to other classes in order to have the weakest
possible coupling between different components of a composed class.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel


class UsesState(ABC):
    """
    A mixin for any class using :meth:`__getstate__` or :meth:`__setstate__`.

    Guarantees that `super()` can always be called in these methods to return a copy
    of the state dict or to update it, respectively.
    """

    def __getstate__(self):
        # Make a shallow(! careful!) copy of the state so any modifications don't
        # immediately impact the object we're getting the state from)
        return dict(self.__dict__)

    def __setstate__(self, state):
        self.__dict__.update(**state)


class HasLabel(ABC):
    """
    A mixin to guarantee the label interface exists.
    """

    @property
    @abstractmethod
    def label(self) -> str:
        """A label for the object."""

    @property
    def full_label(self) -> str:
        """
        A more verbose label based off the underlying label attribute (and possibly
        other data) -- in the root class, it's just the same as the label.
        """
        return self.label


class HasParent(ABC):
    """
    A mixin to guarantee the parent interface exists.
    """

    @property
    @abstractmethod
    def parent(self) -> Any:
        """A parent for the object."""


class HasChannel(ABC):
    """
    A mix-in class for use with the :class:`Channel` class.
    A :class:`Channel` is able to (attempt to) connect to any child instance of :class:`HasConnection`
    by looking at its :attr:`connection` attribute.

    This is useful for letting channels attempt to connect to non-channel objects
    directly by pointing them to some channel that object holds.
    """

    @property
    @abstractmethod
    def channel(self) -> Channel:
        pass


class HasRun(ABC):
    """
    A mixin to guarantee that the :meth:`run` method exists.
    """

    @abstractmethod
    def run(self, *args, **kwargs):
        pass
