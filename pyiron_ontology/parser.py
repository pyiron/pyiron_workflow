from semantikon.converter import parse_input_args, parse_output_args


def get_inputs_and_outputs(node):
    """
    Read input and output arguments with their type hints and return a
    dictionary containing all input output information

    Args:
        node (pyiron_workflow.nodes.Node): node to be parsed

    Returns:
        (dict): dictionary containing input output args, type hints, values
            and variable names
    """
    inputs = parse_input_args(node.node_function)
    outputs = parse_output_args(node.node_function)
    if isinstance(outputs, dict):
        outputs = (outputs,)
    outputs = {key: out for key, out in zip(node.outputs.labels, outputs)}
    for key, value in node.inputs.to_value_dict().items():
        inputs[key]["value"] = value
        inputs[key]["var_name"] = key
    for key, value in node.outputs.to_value_dict().items():
        outputs[key]["value"] = value
        outputs[key]["var_name"] = key
    return {
        "inputs": inputs,
        "outputs": outputs,
        "function": node.node_function.__name__,
        "label": node.label,
    }
