"""
A mixin for nodes, so that they can be extended to leverage injections directly, just
like output -- as long as they have a single, unambiguous output to use!
"""

from __future__ import annotations

from abc import ABC

from pyiron_workflow.mixin.has_interface_mixins import HasInjectableOutputChannel


class InjectsOnChannel(HasInjectableOutputChannel, ABC):
    def __getattr__(self, item):
        try:
            return super().__getattr__(item)
        except AttributeError:
            channel = self.channel
            return getattr(channel, item)

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
