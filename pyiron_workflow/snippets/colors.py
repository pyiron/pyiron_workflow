"""
Simple stuff for colors when you don't want matplotlib/seaborn in your dependency stack.
"""


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
