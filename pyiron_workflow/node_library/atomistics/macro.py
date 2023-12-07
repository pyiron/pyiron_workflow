from pyiron_workflow.macro import Macro, macro_node
from pyiron_workflow.node_library.atomistics.calculator import CalcWithCalculator
from pyiron_workflow.node_library.atomistics.task import (
    ElasticMatrixTaskGenerator,
    EvcurveTaskGenerator,
    PhononsTaskGenerator,
    AnalyseStructures,
    GenerateStructures,
)


def atomistics_meta_macro(task_generator_node_class, macro_name) -> type[Macro]:
    def generic_macro(wf: Macro) -> None:
        wf.tasks = task_generator_node_class()
        wf.structures = GenerateStructures(instance=wf.tasks)
        wf.calc = CalcWithCalculator(task_dict=wf.structures)
        wf.fit = AnalyseStructures(instance=wf.tasks, output_dict=wf.calc)
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


ElasticMatrix = atomistics_meta_macro(ElasticMatrixTaskGenerator, "ElasticMatrix")


EnergyVolumeCurve = atomistics_meta_macro(
    EvcurveTaskGenerator,
    "EnergyVolumeCurve",
)

Phonons = atomistics_meta_macro(PhononsTaskGenerator, "Phonons")


nodes = [
    ElasticMatrix,
    EnergyVolumeCurve,
    Phonons,
]
