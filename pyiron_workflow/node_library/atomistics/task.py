from phonopy.units import VaspToTHz
from pyiron_workflow.function import as_function_node


@as_function_node("task_generator")
def ElasticMatrixTaskGenerator(
    structure, num_of_point=5, eps_range=0.05, sqrt_eta=True, fit_order=2
):
    from atomistics.workflows.elastic.workflow import ElasticMatrixWorkflow

    return ElasticMatrixWorkflow(
        structure=structure,
        num_of_point=num_of_point,
        eps_range=eps_range,
        sqrt_eta=sqrt_eta,
        fit_order=fit_order,
    )


@as_function_node("task_generator")
def EvcurveTaskGenerator(
    structure,
    num_points=11,
    fit_type="polynomial",
    fit_order=3,
    vol_range=0.05,
    axes=["x", "y", "z"],
    strains=None,
):
    from atomistics.workflows.evcurve.workflow import EnergyVolumeCurveWorkflow

    return EnergyVolumeCurveWorkflow(
        structure=structure,
        num_points=num_points,
        fit_type=fit_type,
        fit_order=fit_order,
        vol_range=vol_range,
        axes=axes,
        strains=strains,
    )


@as_function_node("task_generator")
def PhononsTaskGenerator(
    structure,
    interaction_range=10,
    factor=VaspToTHz,
    displacement=0.01,
    dos_mesh=20,
    primitive_matrix=None,
    number_of_snapshots=None,
):
    from atomistics.workflows.phonons.workflow import PhonopyWorkflow

    return PhonopyWorkflow(
        structure=structure,
        interaction_range=interaction_range,
        factor=factor,
        displacement=displacement,
        dos_mesh=dos_mesh,
        primitive_matrix=primitive_matrix,
        number_of_snapshots=number_of_snapshots,
    )


@as_function_node("result_dict")
def AnalyseStructures(instance, output_dict):
    return instance.analyse_structures(output_dict=output_dict)


@as_function_node("task_dict")
def GenerateStructures(instance):
    return instance.generate_structures()


@as_function_node("structure")
def Bulk(element):
    from ase.build import bulk

    return bulk(element)


nodes = [
    AnalyseStructures,
    GenerateStructures,
    Bulk,
    ElasticMatrixTaskGenerator,
    EvcurveTaskGenerator,
    PhononsTaskGenerator,
]
