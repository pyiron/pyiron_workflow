from typing import Optional, Union
from dataclasses import dataclass

# from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.function import single_value_node, function_node

# Ref.: https://stackoverflow.com/questions/68901049/copying-the-docstring-of-function-onto-another-function-by-name
from typing import Callable, TypeVar, Any, TypeAlias
from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")
WrappedFuncDeco: TypeAlias = Callable[[Callable[P, T]], Callable[P, T]]


def copy_doc(copy_func: Callable[..., Any]) -> WrappedFuncDeco[P, T]:
    """Copies the doc string of the given function to another.
    This function is intended to be used as a decorator.

    .. code-block:: python3

        def foo():
            '''This is a foo doc string'''
            ...

        @copy_doc(foo)
        def bar():
            ...
    """

    def wrapped(func: Callable[P, T]) -> Callable[P, T]:
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped


from phonopy.api_phonopy import Phonopy


@dataclass
@copy_doc(Phonopy.generate_displacements)
class InputPhonopyGenerateSupercells:
    distance: float = 0.01
    is_plusminus: Union[str, bool] = "auto"
    is_diagonal: bool = True
    is_trigonal: bool = False
    number_of_snapshots: Optional[int] = None
    random_seed: Optional[int] = None
    temperature: Optional[float] = None
    cutoff_frequency: Optional[float] = None
    max_distance: Optional[float] = None

    def keys(self):
        return self.__dict__.keys()

    def __getitem__(self, key):
        return self.__dict__[key]


# @single_value_node()
def generate_supercells(phonopy, parameters: InputPhonopyGenerateSupercells):
    from structuretoolkit.common import phonopy_to_atoms

    phonopy.generate_displacements(**parameters)

    supercells = [phonopy_to_atoms(s) for s in phonopy.supercells_with_displacements]
    return supercells


# The following function should be defined as a workflow macro (presently not possible)
@function_node()
def create_phonopy(
    structure,
    calculator=None,
    executor=None,
    parameters=InputPhonopyGenerateSupercells(),
):
    from phonopy import Phonopy
    from structuretoolkit.common import atoms_to_phonopy

    phonopy = Phonopy(unitcell=atoms_to_phonopy(structure))

    cells = generate_supercells(phonopy, parameters=parameters)  # .run()
    gs = calc_static()
    df = gs.iter(atoms=cells, executor=executor)
    phonopy.forces = df.forces

    # could be automatized (out = collect(gs, log_level))
    out = {}
    out["energies"] = df.energy
    out["forces"] = df.forces
    out["df"] = df

    return phonopy, out


@single_value_node()
def get_dynamical_matrix(phonopy, q=[0, 0, 0]):
    import numpy as np

    if phonopy.dynamical_matrix is None:
        phonopy.produce_force_constants()
        phonopy.dynamical_matrix.run(q=q)
    dynamical_matrix = np.real_if_close(phonopy.dynamical_matrix.dynamical_matrix)
    # print (dynamical_matrix)
    return dynamical_matrix


@single_value_node()
def get_eigenvalues(matrix):
    import numpy as np

    ew = np.linalg.eigvalsh(matrix)
    return ew


@single_value_node()
def check_consistency(phonopy, tolerance: float = 1e-10):
    dyn_matrix = get_dynamical_matrix(phonopy).run()
    ew = get_eigenvalues(dyn_matrix).run()

    ew_lt_zero = ew[ew < -tolerance]
    if len(ew_lt_zero) > 0:
        print(f"WARNING: {len(ew_lt_zero)} imaginary modes exist")
        has_imaginary_modes = True
    else:
        has_imaginary_modes = False
        print("alles ok")
    return has_imaginary_modes


@single_value_node()
def get_total_dos(phonopy, mesh=3 * [10]):
    from pandas import DataFrame

    phonopy.produce_force_constants()
    phonopy.run_mesh(mesh=mesh)
    phonopy.run_total_dos()
    total_dos = DataFrame(phonopy.get_total_dos_dict())
    return total_dos


@single_value_node()
def calc_static(atoms=None, engine=None, _internal=None):
    # move later to other package
    # print("atoms: ", atoms)
    if engine is None:
        from ase.calculators.emt import EMT

        engine = EMT()

    atoms.calc = engine

    out = {}
    # out['structure'] = atoms # not needed since identical to input
    out["forces"] = atoms.get_forces()
    out["energy"] = atoms.get_potential_energy()
    if _internal is not None:
        out["iter_index"] = _internal[
            "iter_index"
        ]  # TODO: move _internal argument to decorator class
    return out


@function_node("structure", "out")
def calc_minimize(atoms=None, engine=None, fmax=0.005, log_file="tmp.log"):
    # move later to other package
    from ase.optimize import BFGS
    import numpy as np

    if engine is None:
        from ase.calculators.emt import EMT

        engine = EMT()

    atoms.calc = engine

    if log_file is None:  # write to standard io
        log_file = "-"

    dyn = BFGS(atoms, logfile=log_file)
    dyn.run(fmax=fmax)

    # it appears that the is the structure of the second to last step (check)
    atoms_relaxed = atoms.copy()
    atoms_relaxed.calc = atoms.calc
    if dyn.r0 is not None:
        atoms_relaxed.positions = dyn.r0.reshape(-1, 3)

    out = {}
    out["relaxed_structure"] = atoms_relaxed
    # out["forces"] = dyn.f0.reshape(-1, 3)
    out["forces"] = atoms_relaxed.get_forces()
    out["energy"] = atoms_relaxed.get_potential_energy()
    out["energy_initial"] = atoms.get_potential_energy()
    print("energy: ", out["energy"], "max_force: ", np.min(np.abs(out["forces"])))

    return atoms_relaxed, out


nodes = [
    #    generate_supercells,
    create_phonopy,
    get_dynamical_matrix,
    get_eigenvalues,
    check_consistency,
    get_total_dos,
    calc_static,
    calc_minimize,
]
