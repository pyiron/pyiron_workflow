from pyiron_workflow.function import as_function_node

from pyiron_workflow.node_library.atomistic.calculator.data import (
    InputCalcMinimize,
    InputCalcMD,
    InputCalcStatic,
)


@as_function_node("generic")
def Static(structure=None, engine=None):  # , keys_to_store=None):
    output = engine(
        structure=structure,
        calculator=InputCalcStatic(),  # keys_to_store=keys_to_store)
    )
    return output.generic


nodes = [Static]
