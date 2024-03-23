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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiron_workflow.channels import Channel


class HasLabel(ABC):
    """
    A mixin to guarantee the label interface exists.
    """

    @property
    @abstractmethod
    def label(self) -> str:
        """A label for the object."""


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
