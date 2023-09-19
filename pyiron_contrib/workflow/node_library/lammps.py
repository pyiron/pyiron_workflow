from __future__ import annotations

from typing import Optional
from pyiron_atomistics.atomistics.structure.atoms import Atoms

from pyiron_contrib.workflow.function import single_value_node
from pyiron_contrib.workflow.function import function_node
from pyiron_contrib.workflow.macro import Macro, macro_node
from ase.io import write

class VarType:
    def __init__(self, value=None, type=None, label: str = None, store: int = 0,
                 generic=None, doc: str=None):
        self.value = value
        self.type = type
        self.label = label
        self.store = store
        self.generic = generic
        self.doc = doc


from pathlib import Path
class FileObject:
    def __init__(self, path, directory=None):
        if directory is None:
            self._path = Path(path)
        else:
            self._path = Path(directory) / Path(path)

    def __repr__(self):
        return f'FileObject: {self._path} {self.is_file}'

    @property
    def path(self):
        # Note conversion to string (needed to satisfy glob which is used e.g. in dump parser)
        return str(self._path)

    @property
    def is_file(self):
        return self._path.is_file()

    @property
    def name(self):
        return self._path.name


@single_value_node(
    output_labels=['calculator']
)
def calc_md(temperature: VarType(type=float, store=10) = 300,
            n_ionic_steps=1000,
            n_print=100):
    from pyiron_atomistics.lammps.control import LammpsControl

    calculator = LammpsControl()
    print ('calc: T=', temperature)
    calculator.calc_md(temperature=temperature,
                       n_ionic_steps=n_ionic_steps,
                       n_print=n_print)
    return calculator

@single_value_node(
    output_labels=['calculator']
)
def calc_static():
    from pyiron_atomistics.lammps.control import LammpsControl

    calculator = LammpsControl()
    calculator.calc_static()
    return calculator

#TODO: The following function has been only introduced to mimic input variables for a macro
@single_value_node(
    output_labels=['structure']
)
def structure_node(structure):
    return structure

@single_value_node(
    output_labels=['path']
)
def init_lammps(structure=None, potential=None, calculator=None, working_directory=None):
    import os
    from pyiron_atomistics.lammps.potential import LammpsPotential, LammpsPotentialFile

    assert os.path.isdir(working_directory) == True, 'working directory missing'

    pot = LammpsPotential()
    pot.df = LammpsPotentialFile().find_by_name(potential)
    pot.write_file(
        file_name="potential.inp", cwd=working_directory
    )
    pot.copy_pot_files(working_directory)

    with open(
            os.path.join(working_directory, "structure.inp"), "w"
    ) as f:
        structure.write(f, format="lammps-data", specorder=pot.get_element_lst())

    #control = LammpsControl()
    # assert calc_type == "static"  # , "Cannot happen"
    # control.calc_static()
    #control.calc_md(temperature=500, n_ionic_steps=1000, n_print=100)
    calculator.write_file(file_name="control.inp", cwd=working_directory)

    return os.path.abspath(working_directory)


@single_value_node(
    output_labels=['log']
)
def lammps_log_parser(log_file):
    from pymatgen.io.lammps.outputs import parse_lammps_log

    print ('parse log file')
    log = parse_lammps_log(log_file.path)
    if len(log) == 0:
        print (f'check {log_file.path}')
        raise ValueError('lammps_log_parser: failed')

    return log


@single_value_node(
    output_labels=['dump']
)
def lammps_dump_parser(dump_file):
    from pymatgen.io.lammps.outputs import parse_lammps_dumps
    print ('parse dump file')
    dump = list(parse_lammps_dumps(dump_file.path))
    return dump

class ShellOutput:
    stdout:str
    stderr:str
    returncode:int
    dump: FileObject  # TODO: should be done in a specific lammps object
    log: FileObject

@function_node(
    output_labels=['output', 'dump', 'log']
)
def shell(command:str, environment: dict={}, arguments: list=[],
              working_directory: str='.', allowed_returncode:list=[]):
        # -> (ShellOutput, FileObject, FileObject):  TODO: fails -> why
    import os
    import subprocess

    environ = dict(os.environ)
    environ.update({k: str(v) for k, v in environment.items()})
    # print ([str(command), *map(str, arguments)], working_directory, environment)
    print ('start shell')
    proc = subprocess.run(
        [str(command), *map(str, arguments)],
        capture_output=True,
        cwd=working_directory,
        encoding="utf8",
        env=environ,
    )
    print ('end shell')

    output = ShellOutput()
    output.stdout = proc.stdout
    output.stderr = proc.stderr
    output.returncode = proc.returncode
    dump = FileObject('dump.out', working_directory)
    log = FileObject('log.lammps', working_directory)

    return output, dump, log


class GenericOutput:
    energy_pot: []
    energy_kin: []
    forces: []


@single_value_node(
    output_labels=['generic']
)
def collect(out_dump, out_log):
    import numpy as np
    log = out_log[0]

    output = GenericOutput()
    output.energy_pot =  log["PotEng"]
    output.energy_kin = log["TotEng"] - output.energy_pot

    forces = np.array([o.data[["fx", "fy", "fz"]] for o in out_dump])
    output.forces = forces

    return output

@single_value_node(
    output_labels=['potential']
)
def potential_node(structure, name=None, index=0):
    from pyiron_atomistics.lammps.potential import list_potentials

    potentials = list_potentials(structure)
    if name is None:
        potential = potentials[index]
    else:
        if name in potentials:
            potential = name
        else:
            raise ValueError('Unknown potential')
    return potential


@single_value_node(
    output_labels=['potentials']
)
def list_potentials_node(structure):
    from pyiron_atomistics.lammps.potential import list_potentials

    potential = list_potentials(structure)
    return potential


@macro_node()
def repeat_node(wf: Macro) -> None:
    wf.structure = wf.create.atomistics.Bulk(cubic=True, name="Al")
    wf.repeat_structure = wf.create.lammps.Repeat(structure=wf.structure,
                                                  repeat_scalar=3
                                                 )

    wf.inputs_map = {"repeat_structure__repeat_scalar": "n"}
    wf.outputs_map = {"repeat_structure__structure": "repeated_structure"}


@macro_node()
def lammps_static_node(wf: Macro) -> None:
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    wf.structure = wf.create.lammps.StructureNode()

    wf.potential = wf.create.lammps.PotentialNode(structure=wf.structure)
    # wf.list_potentials = wf.create.lammps.ListPotentialsNode(structure=wf.structure)
    wf.calc = wf.create.lammps.CalcStatic()

    wf.init = wf.create.lammps.InitLammps(
        structure=wf.structure,
        potential=wf.potential,
        calculator=wf.calc.outputs.calculator,
        working_directory='test2'
    )
    wf.engine = wf.create.lammps.Shell(command=ExecutablePathResolver(module="lammps", code="lammps"),
                                       working_directory=wf.init.outputs.path
                                       )

    wf.parser_log = wf.create.lammps.LammpsLogParser(log_file=wf.engine.outputs.log)
    wf.parser_dump = wf.create.lammps.LammpsDumpParser(dump_file=wf.engine.outputs.dump)
    wf.collect_output = wf.create.lammps.Collect(out_dump=wf.parser_dump.outputs.dump,
                                                 out_log=wf.parser_log.outputs.log)

    wf.inputs_map = {"structure__structure": "structure",
                     "potential__name": "potential"}
    wf.outputs_map = {"energy_pot__select": "energy_pot"}

@macro_node()
def lammps_md_node(wf: Macro) -> None:
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    wf.structure = wf.create.atomistics.Bulk(cubic=True, name="Al")
    wf.repeat_structure = wf.create.atomistics.Repeat(structure=wf.structure,
                                                      repeat_scalar=3)
    wf.strain = wf.create.atomistics.ApplyStrain(structure=wf.repeat_structure,
                                                       strain=1)

    wf.potential = wf.create.lammps.PotentialNode(structure=wf.strain)
    wf.calc = wf.create.lammps.CalcMd(temperature=1000, n_ionic_steps=10_000)

    wf.init = wf.create.lammps.InitLammps(structure=wf.strain,
                                          potential=wf.potential,
                                          calculator=wf.calc.outputs.calculator,
                                          working_directory='test2')
    wf.engine = wf.create.lammps.Shell(command=ExecutablePathResolver(module="lammps", code="lammps"),
                                       working_directory=wf.init.outputs.path)

    wf.parser_log = wf.create.lammps.LammpsLogParser(log_file=wf.engine.outputs.log)
    wf.parser_dump = wf.create.lammps.LammpsDumpParser(dump_file=wf.engine.outputs.dump)
    wf.collect_output = wf.create.lammps.Collect(out_dump=wf.parser_dump.outputs.dump,
                                                 out_log=wf.parser_log.outputs.log)
    wf.energy_pot = wf.create.standard.Select(data=wf.collect_output.outputs.generic, key='energy_pot')
    wf.energy_kin = wf.create.standard.Select(data=wf.collect_output.outputs.generic, key='energy_kin')

    wf.inputs_map = {# "structure__name": "element",
                     "calc__temperature": "temperature"}
    wf.outputs_map = {"energy_pot__select": "energy_pot"}


@single_value_node(output_labels="structure")
def repeat(structure: Optional[Atoms] = None, repeat_scalar: int = 1) -> Optional[Atoms]:
    return structure.repeat(repeat_scalar)

@single_value_node(output_labels="structure")
def apply_strain(structure: Optional[Atoms] = None, strain: float = 1) -> Optional[Atoms]:
    print ('apply strain: ', strain)
    struct = structure.copy()
    struct.cell *= strain
    return struct



nodes = [
    structure_node,
    init_lammps,
    potential_node,
    list_potentials_node,
    calc_md,
    calc_static,
    lammps_log_parser,
    lammps_dump_parser,
    collect,
    shell,
    repeat_node,
    lammps_md_node,
    lammps_static_node,
    repeat,
    apply_strain
]
