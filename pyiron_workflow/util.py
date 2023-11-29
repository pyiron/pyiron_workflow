from pyiron_base import state

logger = state.logger


class DotDict(dict):
    def __getattr__(self, item):
        return self.__getitem__(item)

    def __setattr__(self, key, value):
        self[key] = value

    def __dir__(self):
        return set(super().__dir__() + list(self.keys()))

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        for k, v in state.items():
            self.__dict__[k] = v

    def to_list(self):
        """A list of values (order not guaranteed)"""
        return list(self.values())


class SeabornColors:
    """
    Hex codes for the ten `seaborn.color_palette()` colors (plus pure white and black),
    recreated to avoid adding an entire dependency.
    """

    blue = "#1f77b4"
    orange = "#ff7f0e"
    green = "#2ca02c"
    red = "#d62728"
    purple = "#9467bd"
    brown = "#8c564b"
    pink = "#e377c2"
    gray = "#7f7f7f"
    olive = "#bcbd22"
    cyan = "#17becf"
    white = "#ffffff"
    black = "#000000"


class HasPost(type):
    """
    A metaclass for adding a `__post__` method which has a compatible signature with
    `__init__` (and indeed receives all its input), but is guaranteed to be called
    only _after_ `__init__` is totally finished.

    Based on @jsbueno's reply in [this discussion](https://discuss.python.org/t/add-a-post-method-equivalent-to-the-new-method-but-called-after-init/5449/11)
    """

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        if post := getattr(cls, "__post__", False):
            post(instance, *args, **kwargs)
        return instance
