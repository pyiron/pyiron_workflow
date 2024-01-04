from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node_library.lammps import get_calculators
from pyiron_workflow.node_library.dev_tools import set_replacer


@macro_node()
def Lammps(wf: Macro) -> None:
    from pyiron_contrib.tinybase.shell import ExecutablePathResolver

    wf.Structure = wf.create.lammps.Structure()

    wf.Potential = wf.create.lammps.Potential(structure=wf.Structure)

    wf.ListPotentials = wf.create.lammps.ListPotentials(structure=wf.Structure)

    wf.calc = wf.create.lammps.CalcStatic()
    wf.calc_select = set_replacer(wf.calc, get_calculators())

    wf.InitLammps = wf.create.lammps.InitLammps(
        structure=wf.Structure,
        potential=wf.Potential,
        calculator=wf.calc.outputs.calculator,
        # working_directory="test2",
    )
    wf.InitLammps.inputs.working_directory = wf.InitLammps.working_directory.path.__str__()
    wf.Shell = wf.create.lammps.Shell(
        command=ExecutablePathResolver(module="lammps", code="lammps").path(),
        working_directory=wf.InitLammps.outputs.path,
    )

    wf.ParseLogFile = wf.create.lammps.ParseLogFile(log_file=wf.Shell.outputs.log)
    wf.ParseDumpFile = wf.create.lammps.ParseDumpFile(dump_file=wf.Shell.outputs.dump)
    wf.Collect = wf.create.lammps.Collect(
        out_dump=wf.ParseDumpFile.outputs.dump, out_log=wf.ParseLogFile.outputs.log
    )

    wf.inputs_map = {
        "Structure__structure": "structure",
        "Potential__name": "potential",
    }
    wf.outputs_map = {"Collect__generic": "generic"}


nodes = [Lammps]
