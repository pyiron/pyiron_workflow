from pyiron_workflow.macro import as_macro_node
from pyiron_workflow.node_library.atomistic.engine.lammps import get_calculators
from pyiron_workflow.node_library.dev_tools import set_replacer

from ase import Atoms


@as_macro_node("generic")
def Lammps(wf, structure=Atoms(), potential=None):
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    wf.potential_object = wf.create.atomistic.engine.lammps.Potential(
        structure=structure, name=potential
    )

    wf.list_potentials = wf.create.atomistic.engine.lammps.ListPotentials(
        structure=structure
    )

    wf.calc = wf.create.atomistic.engine.lammps.CalcStatic()
    wf.calc_select = set_replacer(wf.calc, get_calculators())

    wf.init_lammps = wf.create.atomistic.engine.lammps.InitLammps(
        structure=structure,
        potential=wf.potential_object,
        calculator=wf.calc.outputs.calculator,
    )
    wf.init_lammps.inputs.working_directory = (
        wf.init_lammps.working_directory.path.resolve().__str__()
    )

    wf.shell = wf.create.atomistic.engine.lammps.Shell(
        command=ExecutablePathResolver(module="lammps", code="lammps").path(),
        working_directory=wf.init_lammps.outputs.path,
    )

    wf.parse_log_file = wf.create.atomistic.engine.lammps.ParseLogFile(
        log_file=wf.shell.outputs.log
    )
    wf.parse_dump_file = wf.create.atomistic.engine.lammps.ParseDumpFile(
        dump_file=wf.shell.outputs.dump
    )
    wf.collect = wf.create.atomistic.engine.lammps.Collect(
        out_dump=wf.parse_dump_file.outputs.dump,
        out_log=wf.parse_log_file.outputs.log,
        calc_mode=wf.calc.mode,  # SVN gives output -> inject attribute getter node
    )

    return wf.collect


nodes = [Lammps]
