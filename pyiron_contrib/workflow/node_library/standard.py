from __future__ import annotations

from typing import Optional

import numpy as np
from matplotlib import pyplot as plt

from pyiron_contrib.workflow.function import single_value_node


@single_value_node(output_labels="fig")
def scatter(
    x: Optional[list | np.ndarray] = None, y: Optional[list | np.ndarray] = None
):
    return plt.scatter(x, y)


@single_value_node()
def user_input(user_input):
    return user_input


nodes = [
    scatter,
    user_input,
]
