from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow import NOT_DATA
from pyiron_workflow.channels import Channel

def extract_data(item: Channel) -> dict:
    data = {}
    for key in ["default", "value"]:
        if getattr(item, key) is not NOT_DATA:
            data[key] = getattr(item, key)
    if getattr(item, "type_hint") is not None:
        data["type_hint"] = getattr(item, "type_hint")
    return data


def is_internal_connection(
    channel: Channel, workflow: Composite, io_: str
) -> bool:
    if not channel.connected:
        return False
    return any([channel.connections[0] in getattr(n, io_) for n in workflow])


def get_scoped_label(channel: Channel, io_: str) -> str:
    return channel.scoped_label.replace("__", f".{io_}.")


def get_universal_dict(wf: Composite, with_values: bool = True) -> dict:
    data = {"inputs": {}, "outputs": {}}
    if isinstance(wf, Composite):
        data["nodes"] = {}
        data["edges"] = []
        for inp in wf.inputs:
            if inp.value_receiver is not None:
                data["edges"].append((f"inputs.{inp.scoped_label}", get_scoped_label(inp.value_receiver, "inputs")))
        for node in wf:
            label = node.label
            data["nodes"][label] = get_universal_dict(node, with_values=with_values)
            for out in node.outputs:
                if is_internal_connection(out, wf, "inputs"):
                    data["edges"].append((get_scoped_label(out, "outputs"), get_scoped_label(out.connections[0], "inputs")))
                elif out.value_receiver is not None:
                    data["edges"].append((get_scoped_label(out, "outputs"), f"outputs.{out.value_receiver.scoped_label}"))
        for io_ in ["inputs", "outputs"]:
            for inp in getattr(wf, io_):
                data[io_][inp.scoped_label] = extract_data(inp)
    else:
        for io_ in ["inputs", "outputs"]:
            for inp in getattr(wf, io_):
                data[io_][inp.label] = extract_data(inp)
        data["function"] = wf.node_function
    return data
