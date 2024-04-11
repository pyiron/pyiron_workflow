from ase.units import Ry

from pyiron_workflow.function import as_function_node


@as_function_node("calculator")
def Emt():
    from ase.calculators.emt import EMT

    return EMT()


@as_function_node("calculator")
def Abinit(
    ase_label="abinit_evcurve",
    nbands=32,
    ecut=10 * Ry,
    kpts=(3, 3, 3),
    toldfe=1.0e-2,
    v8_legacy_format=False,
):
    from ase.calculators.abinit import Abinit

    return Abinit(
        label=ase_label,
        nbands=nbands,
        ecut=ecut,
        kpts=kpts,
        toldfe=toldfe,
        v8_legacy_format=v8_legacy_format,
    )


@as_function_node("calculator")
def Gpaw(xc="PBE", encut=300, kpts=(3, 3, 3)):
    from gpaw import GPAW, PW

    return GPAW(xc=xc, mode=PW(encut), kpts=kpts)


@as_function_node("calculator")
def QuantumEspresso(
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


@as_function_node("calculator")
def Siesta(
    ase_label="siesta",
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
        label=ase_label,
        xc=xc,
        mesh_cutoff=mesh_cutoff,
        energy_shift=energy_shift,
        basis_set=basis_set,
        kpts=kpts,
        fdf_arguments=fdf_arguments,
        pseudo_path=pseudo_path,
        pseudo_qualifier=pseudo_qualifier,
    )


@as_function_node("energy_dict")
def CalcWithCalculator(task_dict, calculator):
    from atomistics.calculators.ase import evaluate_with_ase

    return evaluate_with_ase(task_dict=task_dict, ase_calculator=calculator)


@as_function_node("lammps_potential_dataframe")
def LammpsPotential(potential_name, structure, resource_path):
    from atomistics.calculators.lammps import get_potential_dataframe

    df_pot = get_potential_dataframe(structure=structure, resource_path=resource_path)
    return df_pot[df_pot.Name == potential_name].iloc[0]


@as_function_node("energy_dict")
def Lammps(task_dict, potential_dataframe):
    from atomistics.calculators.lammps import evaluate_with_lammps

    return evaluate_with_lammps(
        task_dict=task_dict,
        potential_dataframe=potential_dataframe,
    )


nodes = [
    CalcWithCalculator,
    Abinit,
    Emt,
    Gpaw,
    Lammps,
    LammpsPotential,
    QuantumEspresso,
    Siesta,
]
