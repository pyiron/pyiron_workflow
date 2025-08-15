import bidict
from semantikon import metadata as meta

from pyiron_workflow import channels, io, type_hinting
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
