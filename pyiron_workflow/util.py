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
