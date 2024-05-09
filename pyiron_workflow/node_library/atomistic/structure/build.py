from pyiron_workflow.workflow import Workflow


@Workflow.wrap.as_function_node("structure")
def Bulk(
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


@Workflow.wrap.as_macro_node("structure")
def CubicBulkCell(
    wf, element: str, cell_size: int = 1, vacancy_index: int | None = None
):
    from pyiron_workflow.node_library.atomistic.structure.transform import (
        CreateVacancy,
        Repeat,
    )

    wf.bulk = Bulk(name=element, cubic=True)
    wf.cell = Repeat(structure=wf.bulk, repeat_scalar=cell_size)

    wf.structure = CreateVacancy(structure=wf.cell, index=vacancy_index)
    return wf.structure


nodes = [
    Bulk,
    CubicBulkCell,
]
