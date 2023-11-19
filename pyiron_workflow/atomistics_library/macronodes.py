from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.function import single_value_node
from pyiron_workflow.atomistics_library.calculatornodes import calc_with_calculator
from pyiron_workflow.atomistics_library.tasknodes import (
    get_elastic_matrix_task_generator,
    get_evcurve_task_generator,
    get_phonons_task_generator,
    analyse_structures,
    generate_structures,
)


def atomistics_meta_macro(task_generator_node_class, macro_name) -> type[Macro]:

    def generic_macro(wf: Macro) -> None:
        wf.tasks = task_generator_node_class()
        wf.structures = generate_structures(instance=wf.tasks)
        wf.calc = calc_with_calculator(task_dict=wf.structures)
        wf.fit = analyse_structures(instance=wf.tasks, output_dict=wf.calc)
        inputs_map = {
            # Dynamically expose _all_ task generator input directly on the macro
            "tasks__" + s: s for s in wf.tasks.inputs.labels
        }
        inputs_map["calc__calculator"] = "calculator"
        wf.inputs_map = inputs_map
        wf.outputs_map = {"fit__result_dict": "result_dict"}
    generic_macro.__name__ = macro_name

    return macro_node()(generic_macro)


elastic_matrix = atomistics_meta_macro(
    get_elastic_matrix_task_generator, "elastic_matrix"
)


energy_volume_curve = atomistics_meta_macro(
    get_evcurve_task_generator, "energy_volume_curve",
)

phonons = atomistics_meta_macro(get_phonons_task_generator, "phonons")





@single_value_node("instance")
def get_instance(instance):
    return instance


@macro_node()
def internal_macro(wf: Macro) -> None:
    wf.get_instance = get_instance()
    wf.generate_structures = generate_structures(instance=wf.get_instance)
    wf.calc_with_calculator = calc_with_calculator(task_dict=wf.generate_structures)
    wf.fit = analyse_structures(
        instance=wf.get_instance, output_dict=wf.calc_with_calculator
    )
    wf.inputs_map = {
        "get_instance__instance": "instance",
        "calc_with_calculator__calculator": "calculator",
    }
    wf.outputs_map = {"fit__fit_dict": "fit_dict"}


@macro_node()
def get_energy_volume_curve(wf: Macro) -> None:
    wf.get_task_generator = get_evcurve_task_generator()
    wf.internal = internal_macro(instance=wf.get_task_generator)
    wf.inputs_map = {
        "get_task_generator__structure": "structure",
        "get_task_generator__num_points": "num_points",
        "get_task_generator__fit_type": "fit_type",
        "get_task_generator__fit_order": "fit_order",
        "get_task_generator__vol_range": "vol_range",
        "get_task_generator__axes": "axes",
        "get_task_generator__strains": "strains",
        "internal__calculator": "calculator",
    }
    wf.outputs_map = {"internal__fit_dict": "fit_dict"}


@macro_node()
def get_elastic_matrix(wf: Macro) -> None:
    wf.get_task_generator = get_elastic_matrix_task_generator()
    wf.internal = internal_macro(instance=wf.get_task_generator)
    wf.inputs_map = {
        "get_task_generator__structure": "structure",
        "get_task_generator__num_of_point": "num_of_point",
        "get_task_generator__eps_range": "eps_range",
        "get_task_generator__sqrt_eta": "sqrt_eta",
        "get_task_generator__fit_order": "fit_order",
        "internal__calculator": "calculator",
    }
    wf.outputs_map = {"internal__fit_dict": "fit_dict"}


@macro_node()
def get_phonons(wf: Macro) -> None:
    wf.get_task_generator = get_phonons_task_generator()
    wf.internal = internal_macro(instance=wf.get_task_generator)
    wf.inputs_map = {
        "get_task_generator__structure": "structure",
        "get_task_generator__interaction_range": "interaction_range",
        "get_task_generator__factor": "factor",
        "get_task_generator__displacement": "displacement",
        "get_task_generator__dos_mesh": "dos_mesh",
        "get_task_generator__primitive_matrix": "primitive_matrix",
        "get_task_generator__number_of_snapshots": "number_of_snapshots",
        "internal__calculator": "calculator",
    }
    wf.outputs_map = {"internal__fit_dict": "fit_dict"}


nodes = [
    elastic_matrix,
    energy_volume_curve,
    phonons,
    get_energy_volume_curve,
    get_elastic_matrix,
    get_phonons,

]
