from pyiron_workflow.function import single_value_node

from pyiron_workflow.node_library.atomistic.calculator.data import (
    InputCalcMinimize,
    InputCalcMD,
    InputCalcStatic,
)


@single_value_node("generic")
def static(structure=None, engine=None):  #, keys_to_store=None):
    output = engine(
        structure=structure, calculator=InputCalcStatic()  #keys_to_store=keys_to_store)
    )
    return output.generic


nodes = [static]
