from flowrep.api import schemas as frs
from semantikon import converter, flowrep_dict
from semantikon import datastructure as sds

from pyiron_workflow._wfms.datatypes import InputPort, Node, OutputPort, PortMap


def _annotation_to_type_hint(annotation) -> type | None:
    return (
        flowrep_dict._unwrap_annotated(annotation) if annotation is not None else None
    )


def _annotation_to_type_metadata(annotation) -> sds.TypeMetadata | None:
    return converter.parse_metadata(annotation) if annotation is not None else None


def build_inputs(owner: Node, live: frs.LiveNode) -> PortMap[InputPort, Node]:
    return PortMap[InputPort, Node](
        owner,
        *(
            InputPort(
                label=label,
                owner=owner,
                type_hint=_annotation_to_type_hint(flowrep_port.annotation),
                type_metadata=_annotation_to_type_metadata(flowrep_port.annotation),
                has_default=label in owner.recipe.inputs_with_defaults,
            )
            for label, flowrep_port in live.input_ports.items()
        ),
    )


def build_outputs(owner: Node, live: frs.LiveNode) -> PortMap[OutputPort, Node]:
    return PortMap[OutputPort, Node](
        owner,
        *(
            OutputPort(
                label=label,
                owner=owner,
                type_hint=_annotation_to_type_hint(flowrep_port.annotation),
                type_metadata=_annotation_to_type_metadata(flowrep_port.annotation),
            )
            for label, flowrep_port in live.output_ports.items()
        ),
    )
