"""
For graphical representations of data.
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from pyiron_workflow.function import single_value_node


@single_value_node(output_labels="fig")
def scatter(
    x: Optional[list | np.ndarray] = None, y: Optional[list | np.ndarray] = None
):
    from matplotlib import pyplot as plt

    return plt.scatter(x, y)


nodes = [
    scatter,
]
