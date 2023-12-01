from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node_library.atomistics.calculator import calc_with_calculator
from pyiron_workflow.node_library.atomistics.task import (
    elastic_matrix_task_generator,
    evcurve_task_generator,
    phonons_task_generator,
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
            "tasks__" + s: s
            for s in wf.tasks.inputs.labels
        }
        inputs_map["calc__calculator"] = "calculator"
        wf.inputs_map = inputs_map
        wf.outputs_map = {"fit__result_dict": "result_dict"}

    generic_macro.__name__ = macro_name

    return macro_node()(generic_macro)


elastic_matrix = atomistics_meta_macro(elastic_matrix_task_generator, "elastic_matrix")


energy_volume_curve = atomistics_meta_macro(
    evcurve_task_generator,
    "energy_volume_curve",
)

phonons = atomistics_meta_macro(phonons_task_generator, "phonons")


nodes = [
    elastic_matrix,
    energy_volume_curve,
    phonons,
]
