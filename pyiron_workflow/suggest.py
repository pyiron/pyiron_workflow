import bidict
from semantikon import metadata as meta

from pyiron_workflow import channels, io, type_hinting
from pyiron_workflow.channels import ChannelConnectionError
from pyiron_workflow.nodes import static_io


def _parse_levers(channel: channels.DataChannel):
    hint = channel.type_hint
    if hint is None:
        raise ValueError("Cannot suggest a value for a channel with no type hint.")

    proximate_graph = channel.owner.parent
    if proximate_graph is None:
        raise ValueError("Cannot suggest a value for a channel outside a graph.")

    suggest_for_input = isinstance(channel, channels.InputChannel)

    return hint, proximate_graph, suggest_for_input


def suggest_connections(channel: channels.DataChannel):
    hint, proximate_graph, suggest_for_input = _parse_levers(channel)

    counterpart = "outputs" if suggest_for_input else "inputs"

    siblings = bidict.bidict(proximate_graph.children)
    siblings.pop(channel.owner.label)

    candidates: list[tuple[static_io.StaticNode, channels.DataChannel]] = []
    for sibling in siblings.values():
        candidate_panel: io.DataIO = getattr(sibling, counterpart)
        for candidate in candidate_panel:
            if candidate.type_hint is None or candidate in channel.connections:
                continue

            upstream, downstream = (
                (candidate, channel) if suggest_for_input else (channel, candidate)
            )
            if not type_hinting.type_hint_is_as_or_more_specific_than(
                upstream.type_hint, downstream.type_hint
            ):
                continue

            if meta._is_annotated(upstream.type_hint) and meta._is_annotated(
                downstream.type_hint
            ):
                if downstream._validate_ontology(upstream):
                    candidates.append((sibling, candidate))
                else:
                    continue
            else:
                candidates.append((sibling, candidate))

    return candidates


def suggest_nodes(
    channel: channels.DataChannel, *corpus: type[static_io.StaticNode]
) -> list[type[static_io.StaticNode]]:
    target_hint, proximate_graph, suggest_for_input = _parse_levers(channel)

    candidate_classes = []
    for node_class in corpus:
        preview = (
            node_class.preview_outputs()
            if suggest_for_input
            else node_class.preview_inputs()
        )

        for label, preview_value in preview.items():
            candidate_hint = preview_value if suggest_for_input else preview_value[0]

            if candidate_hint is None:
                continue

            upstream_hint, downstream_hint = (
                (candidate_hint, target_hint)
                if suggest_for_input
                else (target_hint, candidate_hint)
            )

            if not type_hinting.type_hint_is_as_or_more_specific_than(
                upstream_hint, downstream_hint
            ):
                continue

            trial_label = f"__ontological_candidate_{node_class.__name__}"
            trial_child = proximate_graph.add_child(node_class(label=trial_label))
            try:
                channel.connect(
                    trial_child.outputs[label]
                    if suggest_for_input
                    else trial_child.inputs[label]
                )
                candidate_classes.append(node_class)
            except ChannelConnectionError:
                continue
            finally:
                proximate_graph.remove_child(trial_label)

    return candidate_classes
