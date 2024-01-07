from pyiron_workflow.function import single_value_node


@single_value_node("engine")
def EMT():
    from ase.calculators.emt import EMT

    return EMT()


@single_value_node("engine")
def M3GNet():
    import matgl
    from matgl.ext.ase import M3GNetCalculator

    return M3GNetCalculator(matgl.load_model("M3GNet-MP-2021.2.8-PES"))


nodes = [
    EMT,
    M3GNet,
]
