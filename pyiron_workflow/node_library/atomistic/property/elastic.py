import numpy as np

from pyiron_workflow.function import single_value_node
from pyiron_workflow.node_library.dev_tools import wf_data_class
from dataclasses import field

from atomistics.workflows.elastic.symmetry import (
    find_symmetry_group_number,
    get_C_from_A2,
    get_LAG_Strain_List,
    get_symmetry_family_from_SGN,
    Ls_Dic,
)


@wf_data_class()
class OutputElasticSymmetryAnalysis:
    SGN: int = 0
    v0: float = 0.0
    LC: int = 1
    Lag_strain_list: list = field(default_factory=lambda: [])
    epss: np.ndarray = field(default_factory=lambda: np.zeros(0))


@wf_data_class()
class InputElasticTensor:
    num_of_point: int = 5
    eps_range: float = 0.005
    sqrt_eta: bool = True
    fit_order: int = 2


@wf_data_class()
class DataStructureContainer:
    structure: list = field(default_factory=lambda: [])
    job_name: list = field(default_factory=lambda: [])
    energy: list = field(default_factory=lambda: [])
    forces: list = field(default_factory=lambda: [])
    stress: list = field(default_factory=lambda: [])


@single_value_node()
def elastic_constants(structure, calculator=None, engine=None):
    structure_table = generate_structures(structure).run()

    if engine is None:
        from pyiron_workflow.node_library.atomistic.engine.ase import M3GNet

        engine = M3GNet()

    if calculator is None:
        from pyiron_workflow.node_library.atomistic.calculator.ase import static as calculator

    gs = calculator(engine=engine)

    df_new = gs.iter(atoms=structure_table.structure, executor=None)
    structure_table["energy"] = df_new.energy

    elastic = analyse_structures(data_df=structure_table).run()

    return elastic


@single_value_node()
def symmetry_analysis(structure, parameters: InputElasticTensor = InputElasticTensor()):
    out = OutputElasticSymmetryAnalysis(structure)

    out.SGN = find_symmetry_group_number(structure)
    out.v0 = structure.get_volume()
    out.LC = get_symmetry_family_from_SGN(out.SGN)
    out.Lag_strain_list = get_LAG_Strain_List(out.LC)
    out.epss = np.linspace(
        -parameters.eps_range, parameters.eps_range, parameters.num_of_point
    )

    return out


@single_value_node("structures")
def generate_structures(
    structure, parameters: InputElasticTensor = InputElasticTensor()
):
    analysis = symmetry_analysis(structure, parameters).run()
    structure_dict = {}

    zero_strain_job_name = "s_e_0"
    if 0.0 in analysis.epss:
        structure_dict[zero_strain_job_name] = structure.copy()

    for lag_strain in analysis.Lag_strain_list:
        Ls_list = Ls_Dic[lag_strain]
        for eps in analysis.epss:
            if eps == 0.0:
                continue

            Ls = np.zeros(6)
            for ii in range(6):
                Ls[ii] = Ls_list[ii]
            Lv = eps * Ls

            eta_matrix = np.zeros((3, 3))

            eta_matrix[0, 0] = Lv[0]
            eta_matrix[0, 1] = Lv[5] / 2.0
            eta_matrix[0, 2] = Lv[4] / 2.0

            eta_matrix[1, 0] = Lv[5] / 2.0
            eta_matrix[1, 1] = Lv[1]
            eta_matrix[1, 2] = Lv[3] / 2.0

            eta_matrix[2, 0] = Lv[4] / 2.0
            eta_matrix[2, 1] = Lv[3] / 2.0
            eta_matrix[2, 2] = Lv[2]

            norm = 1.0
            eps_matrix = eta_matrix
            if np.linalg.norm(eta_matrix) > 0.7:
                raise Exception(f"Too large deformation {eps}")

            if parameters.sqrt_eta:
                while norm > 1.0e-10:
                    x = eta_matrix - np.dot(eps_matrix, eps_matrix) / 2.0
                    norm = np.linalg.norm(x - eps_matrix)
                    eps_matrix = x

            # --- Calculating the M_new matrix ---------------------------------------------------------
            i_matrix = np.eye(3)
            def_matrix = i_matrix + eps_matrix
            scell = np.dot(structure.get_cell(), def_matrix)
            struct = structure.copy()
            struct.set_cell(scell, scale_atoms=True)

            jobname = subjob_name(lag_strain, eps)

            structure_dict[jobname] = struct

        # df = pd.DataFrame(
        #     dict(structure=structure_dict.values(), job_name=structure_dict.keys())
        # )

    return DataStructureContainer(
        structure=list(structure_dict.values()), job_name=list(structure_dict.keys())
    )


@wf_data_class()
class OutputElasticAnalysis:
    BV: float = 0
    GV: float = 0
    EV: float = 0
    nuV: float = 0
    S: float = 0
    BR: float = 0
    GR: float = 0
    ER: float = 0
    nuR: float = 0
    BH: float = 0
    GH: float = 0
    EH: float = 0
    nuH: float = 0
    AVR: float = 0
    energy_0: float = 0
    strain_energy: list = field(default_factory=lambda: [])
    C: np.ndarray = field(default_factory=lambda: np.zeros(0))
    A2: list = field(default_factory=lambda: [])
    C_eigval: np.ndarray = field(default_factory=lambda: np.zeros(0))


@single_value_node("structures")
def analyse_structures(
    data_df: DataStructureContainer,
    parameters: InputElasticTensor = InputElasticTensor(),
):
    zero_strain_job_name = "s_e_0"
    structure = data_df.structure[0]  # [data_df.job_name == zero_strain_job_name]
    analysis = symmetry_analysis(structure, parameters).run()

    epss = analysis.epss
    Lag_strain_list = analysis.Lag_strain_list

    out = OutputElasticAnalysis()
    energy_dict = {k: v for k, v in zip(data_df.job_name, data_df.energy)}

    if 0.0 in epss:
        out.energy_0 = energy_dict[zero_strain_job_name]

    strain_energy = []
    for lag_strain in Lag_strain_list:
        strain_energy.append([])
        for eps in epss:
            if not eps == 0.0:
                job_name = subjob_name(lag_strain, eps)
                ene = energy_dict[job_name]
            else:
                ene = out.energy_0
            strain_energy[-1].append((eps, ene))
    out.strain_energy = strain_energy
    out = fit_elastic_matrix(out, parameters.fit_order, v0=analysis.v0, LC=analysis.LC)
    return out


def calculate_modulus(out: OutputElasticAnalysis):
    C = out.C

    BV = (C[0, 0] + C[1, 1] + C[2, 2] + 2 * (C[0, 1] + C[0, 2] + C[1, 2])) / 9
    GV = (
        (C[0, 0] + C[1, 1] + C[2, 2])
        - (C[0, 1] + C[0, 2] + C[1, 2])
        + 3 * (C[3, 3] + C[4, 4] + C[5, 5])
    ) / 15
    EV = (9 * BV * GV) / (3 * BV + GV)
    nuV = (1.5 * BV - GV) / (3 * BV + GV)
    out.BV = BV
    out.GV = GV
    out.EV = EV
    out.nuV = nuV

    try:
        S = np.linalg.inv(C)

        BR = 1 / (S[0, 0] + S[1, 1] + S[2, 2] + 2 * (S[0, 1] + S[0, 2] + S[1, 2]))
        GR = 15 / (
            4 * (S[0, 0] + S[1, 1] + S[2, 2])
            - 4 * (S[0, 1] + S[0, 2] + S[1, 2])
            + 3 * (S[3, 3] + S[4, 4] + S[5, 5])
        )
        ER = (9 * BR * GR) / (3 * BR + GR)
        nuR = (1.5 * BR - GR) / (3 * BR + GR)

        BH = 0.50 * (BV + BR)
        GH = 0.50 * (GV + GR)
        EH = (9.0 * BH * GH) / (3.0 * BH + GH)
        nuH = (1.5 * BH - GH) / (3.0 * BH + GH)

        AVR = 100.0 * (GV - GR) / (GV + GR)
        out.S = S

        out.BR = BR
        out.GR = GR
        out.ER = ER
        out.nuR = nuR

        out.BH = BH
        out.GH = GH
        out.EH = EH
        out.nuH = nuH

        out.AVR = AVR
    except np.linalg.LinAlgError as e:
        print("LinAlgError:", e)

    eigval = np.linalg.eig(C)
    out.C_eigval = eigval

    return out


def fit_elastic_matrix(out: OutputElasticAnalysis, fit_order, v0, LC):
    import scipy

    A2 = []
    fit_order = int(fit_order)
    for s_e in out.strain_energy:
        ss = np.transpose(s_e)
        coeffs = np.polyfit(ss[0], ss[1] / v0, fit_order)
        A2.append(coeffs[fit_order - 2])

    A2 = np.array(A2)
    C = get_C_from_A2(A2, LC)

    for i in range(5):
        for j in range(i + 1, 6):
            C[j, i] = C[i, j]

    CONV = (
        1e21 / scipy.constants.physical_constants["joule-electron volt relationship"][0]
    )  # From eV/Ang^3 to GPa

    C *= CONV
    out.C = C
    out.A2 = A2
    calculate_modulus(out)

    return out


def subjob_name(i, eps):
    return f"s_{i}_e{eps:.5f}".replace(".", "_").replace("-", "m")


nodes = [
    symmetry_analysis,
    generate_structures,
    analyse_structures,
    elastic_constants,
]
