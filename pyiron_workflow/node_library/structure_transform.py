from pyiron_workflow.function import single_value_node
from typing import Optional, Union
from pyiron_atomistics.atomistics.structure.atoms import Atoms


@single_value_node("structure")
def repeat(
    structure: Optional[Atoms] = None, repeat_scalar: int = 1
) -> Optional[Atoms]:
    return structure.repeat(repeat_scalar)


@single_value_node("structure")
def apply_strain(
    structure: Optional[Atoms] = None, strain: Union[float, int] = 0
) -> Optional[Atoms]:
    # print("apply strain: ", strain)
    struct = structure.copy()
    # struct.cell *= strain
    struct.apply_strain(strain)
    return struct


@single_value_node()
def create_vacancy(structure, index: int | None = 0):
    structure = structure.copy()
    if index is not None:
        del structure[index]

    return structure


nodes = [
    repeat,
    apply_strain,
    create_vacancy,
]
