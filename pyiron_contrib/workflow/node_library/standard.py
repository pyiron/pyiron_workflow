from __future__ import annotations

from typing import Optional

import numpy as np
from matplotlib import pyplot as plt

from pyiron_contrib.workflow.function import single_value_node


@single_value_node("fig")
def scatter(
    x: Optional[list | np.ndarray] = None, y: Optional[list | np.ndarray] = None
):
    return plt.scatter(x, y)


nodes = [
    scatter,
]
