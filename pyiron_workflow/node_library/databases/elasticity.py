from pyiron_workflow.function import as_function_node
from typing import Optional


@as_function_node("dataframe")
def DeJong(max_index: int | None = None, filename="ec.json"):
    """
    Expects the file to be the "ec.json" database referenced by:
    Ref. de Jong et al. https://www.nature.com/articles/sdata20159#MOESM77

    :return:
    """
    import pandas as pd
    import io
    from ase.io import read

    df = pd.read_json(filename)

    structures = []
    # count = 0
    if max_index is None:
        max_index = len(df.structure)

    df = df.drop(index=range(max_index, len(df)))
    for structure in df.structure:
        f = io.StringIO(structure)
        atoms = read(f, format="cif")
        structures.append(atoms)

        # count += 1
        # if count == max_index:
        #     break

    df["atoms"] = structures

    return df


nodes = [
    DeJong,
]
