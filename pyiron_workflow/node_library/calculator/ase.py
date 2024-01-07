from pyiron_workflow.function import single_value_node, function_node


@single_value_node()
def static(atoms=None, engine=None, _internal=None):
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
def minimize(atoms=None, engine=None, fmax=0.005, log_file="tmp.log"):
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

    # it appears that r0 is the structure of the second to last step (check)
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
    static,
    minimize,
]
