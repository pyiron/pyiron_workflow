from typing import Optional

from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node_library.lammps import get_calculators
from pyiron_workflow.node_library.dev_tools import set_replacer


@macro_node("generic")
def Lammps(wf: Macro, structure, potential_name: Optional[str] = None) -> None:
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    wf.potential = wf.create.lammps.Potential(structure, name=potential_name)
    wf.list_pots = wf.create.lammps.ListPotentials(structure)

    wf.calc = wf.create.lammps.CalcStatic()
    wf.calc_select = set_replacer(wf.calc, get_calculators())

    wf.init_lammps = wf.create.lammps.InitLammps(
        structure=structure,
        potential=wf.potential,
        calculator=wf.calc.outputs.calculator,
        # working_directory="test2",
    )
    wf.init_lammps.inputs.working_directory = wf.init_lammps.working_directory.path.__str__()
    wf.shell = wf.create.lammps.Shell(
        command=ExecutablePathResolver(module="lammps", code="lammps").path(),
        working_directory=wf.init_lammps.outputs.path,
    )

    wf.parser_log_file = wf.create.lammps.ParserLogFile(log_file=wf.shell.outputs.log)
    wf.parser_dump_file = wf.create.lammps.ParserDumpFile(dump_file=wf.shell.outputs.dump)
    wf.collect = wf.create.lammps.Collect(
        out_dump=wf.parser_dump_file.outputs.dump, out_log=wf.parser_log_file.outputs.log
    )
    return wf.collect


nodes = [Lammps]
