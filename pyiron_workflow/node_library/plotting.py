"""
For graphical representations of data.
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from pyiron_workflow.function import single_value_node


@single_value_node("fig")
def Scatter(
    x: Optional[list | np.ndarray] = None, y: Optional[list | np.ndarray] = None
):
    from matplotlib import pyplot as plt

    return plt.scatter(x, y)


nodes = [
    Scatter,
]
