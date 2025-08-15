from __future__ import annotations

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
