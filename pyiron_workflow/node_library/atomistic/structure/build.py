from pyiron_workflow.function import single_value_node
from pyiron_workflow.workflow import Workflow


@single_value_node("structure")
def bulk(
    name,
    crystalstructure=None,
    a=None,
    c=None,
    covera=None,
    u=None,
    orthorhombic=False,
    cubic=False,
):
    from pyiron_atomistics import _StructureFactory

    return _StructureFactory().bulk(
        name,
        crystalstructure,
        a,
        c,
        covera,
        u,
        orthorhombic,
        cubic,
    )


@Workflow.wrap_as.macro_node("structure")
def cubic_bulk_cell(
    wf, element: str, cell_size: int = 1, vacancy_index: int | None = None
):
    from pyiron_workflow.node_library.atomistic.structure.transform import (
        create_vacancy,
        repeat,
    )

    wf.structure = bulk(name=element, cubic=True)
    wf.cell = repeat(structure=wf.structure, repeat_scalar=cell_size)

    wf.cell_with_vacancies = create_vacancy(structure=wf.cell, index=vacancy_index)
    return wf.cell_with_vacancies  # .outputs.structure


nodes = [bulk, cubic_bulk_cell]
