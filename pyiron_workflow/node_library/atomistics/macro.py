from phonopy.units import VaspToTHz

from pyiron_workflow.macro import Macro, as_macro_node
from pyiron_workflow.node_library.atomistics.calculator import CalcWithCalculator
from pyiron_workflow.node_library.atomistics.task import (
    ElasticMatrixTaskGenerator,
    EvcurveTaskGenerator,
    PhononsTaskGenerator,
    AnalyseStructures,
    GenerateStructures,
)


def atomistics_meta_creator(task_generator_node_class) -> type[Macro]:
    def generic_creator(wf: Macro, calculator, **task_kwargs) -> None:
        wf.tasks = task_generator_node_class(**task_kwargs)
        wf.structures = GenerateStructures(instance=wf.tasks)
        wf.calc = CalcWithCalculator(task_dict=wf.structures, calculator=calculator)
        wf.fit = AnalyseStructures(instance=wf.tasks, output_dict=wf.calc)
        return wf.fit

    return generic_creator


@as_macro_node("result_dict")
def ElasticMatrix(
    wf,
    calculator,
    structure,
    num_of_point=5,
    eps_range=0.05,
    sqrt_eta=True,
    fit_order=2,
):
    return atomistics_meta_creator(ElasticMatrixTaskGenerator)(
        wf,
        calculator,
        structure=structure,
        num_of_point=num_of_point,
        eps_range=eps_range,
        sqrt_eta=sqrt_eta,
        fit_order=fit_order,
    )


@as_macro_node("result_dict")
def EnergyVolumeCurve(
    wf,
    calculator,
    structure,
    num_points=11,
    fit_type="polynomial",
    fit_order=3,
    vol_range=0.05,
    axes=("x", "y", "z"),
    strains=None,
):
    return atomistics_meta_creator(EvcurveTaskGenerator)(
        wf,
        calculator,
        structure=structure,
        num_points=num_points,
        fit_type=fit_type,
        fit_order=fit_order,
        vol_range=vol_range,
        axes=axes,
        strains=strains,
    )


@as_macro_node("result_dict")
def Phonons(
    wf,
    calculator,
    structure,
    interaction_range=10,
    factor=VaspToTHz,
    displacement=0.01,
    dos_mesh=20,
    primitive_matrix=None,
    number_of_snapshots=None,
):
    return atomistics_meta_creator(PhononsTaskGenerator)(
        wf,
        calculator,
        structure=structure,
        interaction_range=interaction_range,
        factor=factor,
        displacement=displacement,
        dos_mesh=dos_mesh,
        primitive_matrix=primitive_matrix,
        number_of_snapshots=number_of_snapshots,
    )


nodes = [
    ElasticMatrix,
    EnergyVolumeCurve,
    Phonons,
]
