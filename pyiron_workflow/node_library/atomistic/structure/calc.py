from pyiron_workflow.function import as_function_node
from typing import Optional, Union

# Huge savings when replacing pyiron_atomistics atoms class with ase one!! (> 5s vs 40 ms)
# from pyiron_atomistics.atomistics.structure.atoms import Atoms
from ase import Atoms


@as_function_node("structure")
def volume(structure: Optional[Atoms] = None) -> float:
    return structure.get_volume()


nodes = [volume]
