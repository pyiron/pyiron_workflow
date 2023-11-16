from ase.units import Ry

from pyiron_workflow.function import single_value_node


@single_value_node("calculator")
def get_emt():
    from ase.calculators.emt import EMT

    return EMT()


@single_value_node("calculator")
def get_abinit(
    label="abinit_evcurve",
    nbands=32,
    ecut=10 * Ry,
    kpts=(3, 3, 3),
    toldfe=1.0e-2,
    v8_legacy_format=False,
):
    from ase.calculators.abinit import Abinit

    return Abinit(
        label=label,
        nbands=nbands,
        ecut=ecut,
        kpts=kpts,
        toldfe=toldfe,
        v8_legacy_format=v8_legacy_format,
    )


@single_value_node("calculator")
def get_gpaw(xc="PBE", encut=300, kpts=(3, 3, 3)):
    from gpaw import GPAW, PW

    return GPAW(xc=xc, mode=PW(encut), kpts=kpts)


@single_value_node("calculator")
def get_quantum_espresso(
    pseudopotentials={"Al": "Al.pbe-n-kjpaw_psl.1.0.0.UPF"},
    tstress=True,
    tprnfor=True,
    kpts=(3, 3, 3),
):
    from ase.calculators.espresso import Espresso

    return Espresso(
        pseudopotentials=pseudopotentials,
        tstress=tstress,
        tprnfor=tprnfor,
        kpts=kpts,
    )


@single_value_node("calculator")
def get_siesta(
    label="siesta",
    xc="PBE",
    mesh_cutoff=200 * Ry,
    energy_shift=0.01 * Ry,
    basis_set="DZ",
    kpts=(5, 5, 5),
    fdf_arguments={"DM.MixingWeight": 0.1, "MaxSCFIterations": 100},
    pseudo_path="",
    pseudo_qualifier="",
):
    from ase.calculators.siesta import Siesta

    return Siesta(
        label=label,
        xc=xc,
        mesh_cutoff=mesh_cutoff,
        energy_shift=energy_shift,
        basis_set=basis_set,
        kpts=kpts,
        fdf_arguments=fdf_arguments,
        pseudo_path=pseudo_path,
        pseudo_qualifier=pseudo_qualifier,
    )


@single_value_node("energy_dict")
def calc_with_calculator(task_dict, calculator):
    from atomistics.calculators.ase import evaluate_with_ase

    return evaluate_with_ase(task_dict=task_dict, ase_calculator=calculator)


@single_value_node("lammps_potential_dataframe")
def get_lammps_potential(potential_name, structure, resource_path):
    from atomistics.calculators.lammps import get_potential_dataframe

    df_pot = get_potential_dataframe(structure=structure, resource_path=resource_path)
    return df_pot[df_pot.Name == potential_name].iloc[0]


@single_value_node("energy_dict")
def get_lammps(task_dict, potential_dataframe):
    from atomistics.calculators.lammps import evaluate_with_lammps

    return evaluate_with_lammps(
        task_dict=task_dict,
        potential_dataframe=potential_dataframe,
    )


nodes = [
    calc_with_calculator,
    get_abinit,
    get_emt,
    get_gpaw,
    get_lammps,
    get_lammps_potential,
    get_quantum_espresso,
    get_siesta,
]
