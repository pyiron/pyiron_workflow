from typing import Optional
from dataclasses import field

from pyiron_workflow.node_library.dev_tools import wf_data_class, wfMetaData


@wf_data_class()
class OutputCalcStatic:
    from ase import Atoms
    import numpy as np

    energy_pot: Optional[float] = field(default=None, metadata=wfMetaData(log_level=0))
    force: Optional[np.ndarray] = field(default=None, metadata=wfMetaData(log_level=0))
    stress: Optional[np.ndarray] = field(
        default=None, metadata=wfMetaData(log_level=10)
    )
    structure: Optional[Atoms] = field(default=None, metadata=wfMetaData(log_level=10))

    atomic_energies: Optional[float] = field(
        default=None,
        metadata=wfMetaData(
            log_level=0, doc="per atom energy, only if supported by calculator"
        ),
    )


@wf_data_class()
class OutputCalcMinimize:
    initial: Optional[OutputCalcStatic] = field(
        default_factory=lambda: OutputCalcStatic(), metadata=wfMetaData(log_level=0)
    )
    final: Optional[OutputCalcStatic] = field(
        default_factory=lambda: OutputCalcStatic(), metadata=wfMetaData(log_level=0)
    )


@wf_data_class()
class OutputCalcMD:
    import numpy as np

    energies_pot: Optional[np.ndarray] = field(
        default=None, metadata=wfMetaData(log_level=0)
    )
    energies_kin: Optional[np.ndarray] = field(
        default=None, metadata=wfMetaData(log_level=0)
    )
    forces: Optional[np.ndarray] = field(default=None, metadata=wfMetaData(log_level=0))
    positions: Optional[np.ndarray] = field(
        default=None, metadata=wfMetaData(log_level=0)
    )
    temperatures: Optional[np.ndarray] = field(
        default=None, metadata=wfMetaData(log_level=0)
    )


@wf_data_class()
class InputCalcMD:
    """
        Set an MD calculation within LAMMPS. Nosé Hoover is used by default.

    Parameters
    temperature (None/float/list) – Target temperature value(-s). If set to None, an NVE calculation is performed. It is required when the pressure is set or langevin is set It can be a list of temperature values, containing the initial target temperature and the final target temperature (in between the target value is varied linearly).

    pressure (None/float/numpy.ndarray/list) – Target pressure. If set to None, an NVE or an NVT calculation is performed. A list of up to length 6 can be given to specify xx, yy, zz, xy, xz, and yz components of the pressure tensor, respectively. These values can mix floats and None to allow only certain degrees of cell freedom to change. (Default is None, run isochorically.)

    n_ionic_steps (int) – Number of ionic steps

    time_step (float) – Step size in fs between two steps.

    n_print (int) – Print frequency

    temperature_damping_timescale (float) – The time associated with the thermostat adjusting the temperature. (In fs. After rescaling to appropriate time units, is equivalent to Lammps’ Tdamp.)

    pressure_damping_timescale (float) – The time associated with the barostat adjusting the temperature. (In fs. After rescaling to appropriate time units, is equivalent to Lammps’ Pdamp.)

    seed (int) – Seed for the random number generation (required for the velocity creation)

    tloop –

    initial_temperature (None/float) – Initial temperature according to which the initial velocity field is created. If None, the initial temperature will be twice the target temperature (which would go immediately down to the target temperature as described in equipartition theorem). If 0, the velocity field is not initialized (in which case the initial velocity given in structure will be used). If any other number is given, this value is going to be used for the initial temperature.

    langevin (bool) – (True or False) Activate Langevin dynamics

    delta_temp (float) – Thermostat timescale, but in your Lammps time units, whatever those are. (DEPRECATED.)

    delta_press (float) – Barostat timescale, but in your Lammps time units, whatever those are. (DEPRECATED.)

    job_name (str) – Job name of the job to generate a unique random seed.

    rotation_matrix (numpy.ndarray) – The rotation matrix from the pyiron to Lammps coordinate frame.
    """

    temperature: float = 300
    n_ionic_steps: int = 1_000
    n_print: int = 100
    pressure = None
    time_step: float = 1.0
    temperature_damping_timescale: float = 100.0
    pressure_damping_timescale: float = 1000.0
    seed = None
    tloop = None
    initial_temperature = None
    langevin = False
    delta_temp = None
    delta_press = None


@wf_data_class()
class InputCalcMinimize:
    """
        Sets parameters required for minimization.

    Parameters
    e_tol (float) – If the magnitude of difference between energies of two consecutive steps is lower than or equal to e_tol, the minimisation terminates. (Default is 0.0 eV.)

    f_tol (float) – If the magnitude of the global force vector at a step is lower than or equal to f_tol, the minimisation terminates. (Default is 1e-4 eV/angstrom.)

    max_iter (int) – Maximum number of minimisation steps to carry out. If the minimisation converges before max_iter steps, terminate at the converged step. If the minimisation does not converge up to max_iter steps, terminate at the max_iter step. (Default is 100000.)

    pressure (None/float/numpy.ndarray/list) – Target pressure. If set to None, an NVE or an NVT calculation is performed. A list of up to length 6 can be given to specify xx, yy, zz, xy, xz, and yz components of the pressure tensor, respectively. These values can mix floats and None to allow only certain degrees of cell freedom to change. (Default is None, run isochorically.)

    n_print (int) – Write (dump or print) to the output file every n steps (Default: 100)

    style ('cg'/'sd'/other values from Lammps docs) – The style of the numeric minimization, either conjugate gradient, steepest descent, or other keys permissible from the Lammps docs on ‘min_style’. (Default is ‘cg’ – conjugate gradient.)

    rotation_matrix (numpy.ndarray) – The rotation matrix from the pyiron to Lammps coordinate frame.
    """

    e_tol: float = 0.0
    f_tol: float = 1e-4
    max_iter: int = 1_000_000
    pressure: float = None
    n_print: int = 100
    style: str = "cg"


@wf_data_class()
class InputCalcStatic:
    keys_to_store: Optional[list] = field(default_factory=list)


nodes = []
