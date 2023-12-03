from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node_library.lammps import get_calculators
from pyiron_workflow.node_library.dev_tools import set_replacer


@macro_node()
def lammps(wf: Macro) -> None:
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    structure = wf.create.lammps.Structure()

    potential = wf.create.lammps.Potential(structure=structure)

    wf.list_pots = wf.create.lammps.ListPotentials(structure=structure)

    wf.calc = wf.create.lammps.CalcStatic()
    wf.calc_select = set_replacer(wf.calc, get_calculators())

    init_lammps = wf.create.lammps.InitLammps(
        structure=wf.structure,
        potential=potential,
        calculator=wf.calc.outputs.calculator,
        # working_directory="test2",
    )
    init_lammps.inputs.working_directory = init_lammps.working_directory.path.__str__()
    shell = wf.create.lammps.Shell(
        command=ExecutablePathResolver(module="lammps", code="lammps").path(),
        working_directory=init_lammps.outputs.path,
    )

    parser_log_file = wf.create.lammps.ParserLogFile(log_file=shell.outputs.log)
    parser_dump_file = wf.create.lammps.ParserDumpFile(dump_file=shell.outputs.dump)
    collect = wf.create.lammps.Collect(
        out_dump=parser_dump_file.outputs.dump, out_log=parser_log_file.outputs.log
    )

    wf.inputs_map = {
        "structure__structure": "structure",
        "potential__name": "potential",
    }
    wf.outputs_map = {"collect__generic": "generic"}


nodes = [lammps]
