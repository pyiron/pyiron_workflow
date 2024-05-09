from dataclasses import asdict, dataclass
from typing import Optional, Union
import warnings

from phonopy import Phonopy

from pyiron_workflow.function import as_function_node
from pyiron_workflow.macro import as_macro_node
from pyiron_workflow.transform import dataclass_node


@as_function_node()
def PhonopyObject(structure):
    # from phonopy import Phonopy
    from structuretoolkit.common import atoms_to_phonopy

    return Phonopy(unitcell=atoms_to_phonopy(structure))


@dataclass
class GenerateSupercellsParameters:
    distance: float = 0.01
    is_plusminus: Union[str, bool] = "auto"
    is_diagonal: bool = True
    is_trigonal: bool = False
    number_of_snapshots: Optional[int] = None
    random_seed: Optional[int] = None
    temperature: Optional[float] = None
    cutoff_frequency: Optional[float] = None
    max_distance: Optional[float] = None


# GenerateSupercellsParameters.__doc__ = Phonopy.generate_displacements


@as_function_node()
def GenerateSupercells(
    phonopy: Phonopy,
    parameters: GenerateSupercellsParameters
) -> list:
    from structuretoolkit.common import phonopy_to_atoms

    phonopy.generate_displacements(**asdict(parameters))

    supercells = [phonopy_to_atoms(s) for s in phonopy.supercells_with_displacements]
    return supercells


@as_macro_node("phonopy", "df")
def CreatePhonopy(
    self,
    structure,
    generate_supercells_parameters: GenerateSupercellsParameters,
    engine=None,
):

    self.phonopy = PhonopyObject(structure)
    self.cells = GenerateSupercells(
        self.phonopy,
        parameters=generate_supercells_parameters
    )

    from pyiron_workflow.node_library.atomistic.calculator.ase import Static
    from pyiron_workflow.for_loop import for_node
    self.gs = for_node(
        Static,
        iter_on=("atoms",),
        atoms=self.cells,
        engine=engine,
    )

    # from pyiron_workflow.node_library.standard import GetItem
    # self.forces = for_node(
    #     GetItem,
    #     iter_on=("obj",),
    #     obj=self.gs.outputs.df["out"].to_list(),
    #     item="forces"
    # )["getitem"]
    self.forces = DictsToList(self.gs.outputs.df["out"], "forces")

    from pyiron_workflow.node_library.standard import SetAttr
    self.phonopy_with_forces = SetAttr(
        self.phonopy,
        "forces",
        self.forces
    )

    return self.phonopy_with_forces, self.gs


@as_function_node()
def DictsToList(dictionaries, key):
    """
    `atomistic.calculator.ase.Static` returns a dictionary of stuff; when we iterate
    over it, our dataframe has this dictionary in each row. We want a way to get it
    back per-column

    The old "iter" played nicely with a dictionary getting returned, where the new
    "iter" and `For` node play nicely with individual values getting returned.
    This is all OK, and it works, but it is clearly a friction point and we'll need to
    do some polish for usability.
    """
    return [d[key] for d in dictionaries]


@as_function_node()
def GetDynamicalMatrix(phonopy, q: tuple[int, int, int] = 3 * (0,)):
    import numpy as np

    if phonopy.dynamical_matrix.dynamical_matrix is None:
        phonopy.produce_force_constants()
        phonopy.dynamical_matrix.run(q=q)
    dynamical_matrix = np.real_if_close(phonopy.dynamical_matrix.dynamical_matrix)
    return dynamical_matrix


@as_function_node()
def GetEigenvalues(matrix):
    import numpy as np

    ew = np.linalg.eigvalsh(matrix)
    return ew


@as_function_node()
def HasImaginaryNodes(eigenvalues, tolerance: float = 1e-10) -> bool:
    n_imaginary_nodes = len(eigenvalues[eigenvalues < -tolerance])
    if has_imaginary_modes := n_imaginary_nodes > 0:
        warnings.warn(f"WARNING: {n_imaginary_nodes} imaginary modes exist")
    return has_imaginary_modes


@as_macro_node()
def CheckConsistency(self, phonopy, tolerance: float = 1e-10):
    self.dyn_matrix = GetDynamicalMatrix(phonopy)
    self.ew = GetEigenvalues(self.dyn_matrix)
    self.has_imaginary_modes = HasImaginaryNodes(self.ew, tolerance=tolerance)

    return self.has_imaginary_modes


@as_function_node()
def GetTotalDos(phonopy, mesh: Optional[tuple[int, int, int]] = None):
    mesh = 3 * (10,) if mesh is None else mesh

    from pandas import DataFrame

    phonopy.produce_force_constants()
    phonopy.run_mesh(mesh=mesh)
    phonopy.run_total_dos()
    total_dos = DataFrame(phonopy.get_total_dos_dict())
    return total_dos


nodes = [
    CheckConsistency,
    CreatePhonopy,
    DictsToList,
    GenerateSupercells,
    GetDynamicalMatrix,
    GetEigenvalues,
    GetTotalDos,
]
