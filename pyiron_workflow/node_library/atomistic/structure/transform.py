from pyiron_workflow.function import as_function_node
from typing import Optional, Union

# Huge savings when replacing pyiron_atomistics atoms class with ase one!! (> 5s vs 40 ms)
# from pyiron_atomistics.atomistics.structure.atoms import Atoms
from ase import Atoms


@as_function_node("structure")
def repeat(structure: Atoms, repeat_scalar: int = 1) -> Atoms:
    return structure.repeat(repeat_scalar)


@as_function_node("structure")
def apply_strain(
    structure: Optional[Atoms] = None, strain: Union[float, int] = 0
) -> Optional[Atoms]:
    # print("apply strain: ", strain)
    struct = structure.copy()
    # struct.cell *= strain
    struct.apply_strain(strain)
    return struct


@as_function_node()
def create_vacancy(structure, index: int | None = 0):
    structure = structure.copy()
    if index is not None:
        del structure[index]

    return structure


@as_function_node("structure")
def rotate_axis_angle(
    structure: Atoms,
    angle: float | int = 0,
    axis: list = [0, 0, 1],
    center=(0, 0, 0),
    rotate_cell: bool = False,
):
    """
    Rotate atoms based on a vector and an angle, or two vectors.

    Parameters:

    angle = None:
    Angle that the atoms is rotated around the vector ‘v’. ‘a’ can also be a vector and then ‘a’ is rotated into ‘v’.
    v:
    Vector to rotate the atoms around. Vectors can be given as strings: ‘x’, ‘-x’, ‘y’, … .
    center = (0, 0, 0):
    The center is kept fixed under the rotation. Use ‘COM’ to fix the center of mass, ‘COP’ to fix the center of positions or ‘COU’ to fix the center of cell.
    rotate_cell = False:
    If true the cell is also rotated.
    :type rotate_cell: object
    """

    structure_rotated = structure.copy()
    structure_rotated.rotate(a=angle, v=axis, center=center, rotate_cell=rotate_cell)
    return structure_rotated


nodes = [
    repeat,
    apply_strain,
    create_vacancy,
    rotate_axis_angle,
]
