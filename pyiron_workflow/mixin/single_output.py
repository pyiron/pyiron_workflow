"""
A mixin for nodes, so that they can be extended to leverage injections directly, just
like output -- as long as they have a single, unambiguous output to use!
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyiron_workflow.mixin.has_interface_mixins import HasChannel, HasLabel
from pyiron_workflow.mixin.injection import (
    OutputDataWithInjection,
    OutputsWithInjection,
)


class AmbiguousOutputError(ValueError):
    """Raised when searching for exactly one output, but multiple are found."""


class ExploitsSingleOutput(HasLabel, HasChannel, ABC):
    @property
    @abstractmethod
    def outputs(self) -> OutputsWithInjection:
        """
        Required interface.

        Fulfilled by, e.g. :class:`pyiron_workflow.injection.HasIOWithInjection`
        """

    @property
    def channel(self) -> OutputDataWithInjection:
        """
        The single output channel. Fulfills the interface expectations for the
        :class:`HasChannel` mixin and allows this object to be used directly for
        forming connections, etc.

        Returns:
            (OutputDataWithInjection): The single output channel.

        Raises:
            AmbiguousOutputError: If there is not exactly one output channel.
        """
        if len(self.outputs) != 1:
            raise AmbiguousOutputError(
                f"Tried to access the channel value of {self.label}, but this is only "
                f"possible when there is a single output channel -- {self.label} has: "
                f"{self.outputs.labels}. Access probably occurred attempting to use "
                f"this object like an output channel, e.g. with injection or to form a "
                f"connection. Either make sure it has exactly one output channel, or "
                f"use the particular channel you want directly."
            )
        else:
            return self.outputs[self.outputs.labels[0]]

    def __getattr__(self, item):
        try:
            return super().__getattr__(item)
        except AttributeError as e1:
            try:
                return getattr(self.channel, item)
            except Exception as e2:
                raise e2 from e1

    def __getitem__(self, item):
        return self.channel.__getitem__(item)

    def __lt__(self, other):
        return self.channel.__lt__(other)

    def __le__(self, other):
        return self.channel.__le__(other)

    def eq(self, other):
        return self.channel.eq(other)

    def __ne__(self, other):
        return self.channel.__ne__(other)

    def __gt__(self, other):
        return self.channel.__gt__(other)

    def __ge__(self, other):
        return self.channel.__ge__(other)

    def bool(self):
        return self.channel.bool()

    def len(self):
        return self.channel.len()

    def contains(self, other):
        return self.channel.contains(other)

    def __add__(self, other):
        return self.channel.__add__(other)

    def __sub__(self, other):
        return self.channel.__sub__(other)

    def __mul__(self, other):
        return self.channel.__mul__(other)

    def __rmul__(self, other):
        return self.channel.__rmul__(other)

    def __matmul__(self, other):
        return self.channel.__matmul__(other)

    def __truediv__(self, other):
        return self.channel.__truediv__(other)

    def __floordiv__(self, other):
        return self.channel.__floordiv__(other)

    def __mod__(self, other):
        return self.channel.__mod__(other)

    def __pow__(self, other):
        return self.channel.__pow__(other)

    def __and__(self, other):
        return self.channel.__and__(other)

    def __xor__(self, other):
        return self.channel.__xor__(other)

    def __or__(self, other):
        return self.channel.__or__(other)

    def __neg__(self):
        return self.channel.__neg__()

    def __pos__(self):
        return self.channel.__pos__()

    def __abs__(self):
        return self.channel.__abs__()

    def __invert__(self):
        return self.channel.__invert__()

    def int(self):
        return self.channel.int()

    def float(self):
        return self.channel.float()

    def __round__(self):
        return self.channel.__round__()
