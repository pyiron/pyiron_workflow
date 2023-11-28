from __future__ import annotations


from typing import Optional
from pyiron_atomistics.atomistics.structure.atoms import Atoms

from pyiron_workflow.function import single_value_node, function_node
from pyiron_workflow.workflow import Workflow

from pyiron_workflow.node_library.dev_tools import Replacer, VarType, FileObject


@single_value_node(output_labels=["calculator"])
def calc_md(
    temperature: VarType(dat_type=float, store=10) = 300,
    n_ionic_steps=1000,
    n_print=100,
):
    from pyiron_atomistics.lammps.control import LammpsControl

    calculator = LammpsControl()
    # print("calc: T=", temperature)
    calculator.calc_md(
        temperature=temperature, n_ionic_steps=n_ionic_steps, n_print=n_print
    )
    return calculator


@single_value_node(output_labels=["calculator"])
def calc_static():
    from pyiron_atomistics.lammps.control import LammpsControl

    calculator = LammpsControl()
    calculator.calc_static()
    return calculator


# TODO: The following function has been only introduced to mimic input variables for a macro
@single_value_node(output_labels=["structure"])
def structure(structure):
    return structure


@single_value_node(output_labels=["path"])
def init_lammps(
    structure=None, potential=None, calculator=None, working_directory=None
):
    import os
    from pyiron_atomistics.lammps.potential import LammpsPotential, LammpsPotentialFile

    assert os.path.isdir(working_directory), "working directory missing"

    pot = LammpsPotential()
    pot.df = LammpsPotentialFile().find_by_name(potential)
    pot.write_file(file_name="potential.inp", cwd=working_directory)
    pot.copy_pot_files(working_directory)

    with open(os.path.join(working_directory, "structure.inp"), "w") as f:
        structure.write(f, format="lammps-data", specorder=pot.get_element_lst())

    # control = LammpsControl()
    # assert calc_type == "static"  # , "Cannot happen"
    # control.calc_static()
    # control.calc_md(temperature=500, n_ionic_steps=1000, n_print=100)
    calculator.write_file(file_name="control.inp", cwd=working_directory)

    return os.path.abspath(working_directory)


@single_value_node(output_labels=["log"])
def parser_log_file(log_file):
    from pymatgen.io.lammps.outputs import parse_lammps_log

    log = parse_lammps_log(log_file.path)
    if len(log) == 0:
        print(f"check {log_file.path}")
        raise ValueError("lammps_log_parser: failed")

    return log


@single_value_node(output_labels=["dump"])
def parser_dump_file(dump_file):
    from pymatgen.io.lammps.outputs import parse_lammps_dumps

    dump = list(parse_lammps_dumps(dump_file.path))
    return dump


class ShellOutput:
    stdout: str
    stderr: str
    return_code: int
    dump: FileObject  # TODO: should be done in a specific lammps object
    log: FileObject


@function_node(output_labels=["output", "dump", "log"])
def shell(
    command: str,
    environment: Optional[dict] = None,
    arguments: Optional[list] = None,
    working_directory: str = ".",
    # allowed_return_code:list=[]
):
    # -> (ShellOutput, FileObject, FileObject):  TODO: fails -> why
    import os
    import subprocess

    if environment is None:
        environment = {}
    if arguments is None:
        arguments = []

    environ = dict(os.environ)
    environ.update({k: str(v) for k, v in environment.items()})
    # print ([str(command), *map(str, arguments)], working_directory, environment)
    # print("start shell")
    proc = subprocess.run(
        [command, *map(str, arguments)],
        capture_output=True,
        cwd=working_directory,
        encoding="utf8",
        env=environ,
    )
    # print("end shell")

    output = ShellOutput()
    output.stdout = proc.stdout
    output.stderr = proc.stderr
    output.return_code = proc.returncode
    dump = FileObject("dump.out", working_directory)
    log = FileObject("log.lammps", working_directory)

    return output, dump, log


class GenericOutput:
    energy_pot: []
    energy_kin: []
    forces: []


@single_value_node(output_labels=["generic"])
def collect(out_dump, out_log):
    import numpy as np

    log = out_log[0]

    output = GenericOutput()
    output.energy_pot = log["PotEng"]
    output.energy_kin = log["TotEng"] - output.energy_pot

    forces = np.array([o.data[["fx", "fy", "fz"]] for o in out_dump])
    output.forces = forces

    return output


@single_value_node(output_labels=["potential"])
def potential(structure, name=None, index=0):
    from pyiron_atomistics.lammps.potential import list_potentials as lp

    potentials = lp(structure)
    if name is None:
        pot = potentials[index]
    else:
        if name in potentials:
            pot = name
        else:
            raise ValueError("Unknown potential")
    return pot


@single_value_node(output_labels=["potentials"])
def list_potentials(structure):
    from pyiron_atomistics.lammps.potential import list_potentials as lp

    pot = lp(structure)
    return pot

@single_value_node(output_labels=["empty"])
def list_empty():
    return []

@single_value_node(output_labels="structure")
def repeat(
    structure: Optional[Atoms] = None, repeat_scalar: int = 1
) -> Optional[Atoms]:
    return structure.repeat(repeat_scalar)


@single_value_node(output_labels="structure")
def apply_strain(
    structure: Optional[Atoms] = None, strain: float = 0.
) -> Optional[Atoms]:
    # print("apply strain: ", strain)
    struct = structure.copy()
    # struct.cell *= strain
    struct.apply_strain(strain)
    return struct


def get_calculators():
    calc_dict = dict()
    calc_dict["md"] = Workflow.create.lammps.CalcMd
    calc_dict["static"] = Workflow.create.lammps.CalcStatic
    return calc_dict

nodes = [
    structure,
    init_lammps,
    potential,
    list_potentials,
    list_empty,
    calc_md,
    calc_static,
    parser_log_file,
    parser_dump_file,
    collect,
    shell,
    repeat,
    apply_strain,
]
