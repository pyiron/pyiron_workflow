import bidict
import toposort
from semantikon import metadata as meta

from pyiron_workflow import channels, io, topology, type_hinting
from pyiron_workflow.nodes import static_io


class SuggestionError(BaseException): ...


class UnhintedError(SuggestionError, ValueError): ...


class NonSiblingError(SuggestionError, ValueError): ...


class ConnectedInputError(SuggestionError, ValueError): ...


def _parse_levers(channel: channels.DataChannel):
    hint = channel.type_hint
    if hint is None:
        raise UnhintedError("Cannot suggest a value for a channel with no type hint.")

    proximate_graph = channel.owner.parent
    if proximate_graph is None:
        raise NonSiblingError("Cannot suggest a value for a channel outside a graph.")

    suggest_for_input = isinstance(channel, channels.InputChannel)

    return hint, proximate_graph, suggest_for_input


def suggest_connections(
    channel: channels.DataChannel,
    *corpus: static_io.StaticNode,
    stop_early: bool = False,
):
    hint, proximate_graph, suggest_for_input = _parse_levers(channel)
    if suggest_for_input and channel.connected:
        raise ConnectedInputError(
            f"Cannot suggest a connection for the input {channel.full_label} because "
            f"it is connected. Please disconnect it and ask for suggestions again."
        )

    counterpart = "outputs" if suggest_for_input else "inputs"

    if len(corpus) == 0:
        siblings = bidict.bidict(proximate_graph.children)
        siblings.pop(channel.owner.label)
        corpus = tuple(siblings.values())

    candidates: list[tuple[static_io.StaticNode, channels.DataChannel]] = []
    for sibling in corpus:
        candidate_panel: io.DataIO = getattr(sibling, counterpart)
        for candidate in candidate_panel:
            if candidate.type_hint is None or candidate in channel.connections:
                continue

            upstream, downstream = (
                (candidate, channel) if suggest_for_input else (channel, candidate)
            )
            if downstream.connected:
                continue
            if not type_hinting.type_hint_is_as_or_more_specific_than(
                upstream.type_hint, downstream.type_hint
            ):
                continue

            if (
                meta._is_annotated(upstream.type_hint)
                and meta._is_annotated(downstream.type_hint)
                and upstream.owner.graph_root._validate_ontologies
                and upstream.owner.graph_root is downstream.owner.graph_root
                and not upstream.has_ontologically_valid_connection(downstream)
            ):
                continue

            # Disallow circular connections
            data_digraph = topology.nodes_to_data_digraph(proximate_graph.children)
            data_digraph[downstream.owner.label].add(upstream.owner.label)
            try:
                toposort.toposort_flatten(data_digraph)
            except toposort.CircularDependencyError:
                continue

            candidates.append((sibling, candidate))
            if stop_early:
                return candidates

    return candidates


def suggest_nodes(
    channel: channels.DataChannel, *corpus: type[static_io.StaticNode]
) -> list[type[static_io.StaticNode]]:
    _, proximate_graph, suggest_for_input = _parse_levers(channel)
    if suggest_for_input and channel.connected:
        raise ConnectedInputError(
            f"Cannot suggest a connection for the input {channel.full_label} because "
            f"it is connected. Please disconnect it and ask for suggestions again."
        )

    candidate_classes = []
    for node_class in corpus:
        suggestions = []
        trial_label = "ONTOLOGICALTRIALNODE"
        try:
            trial_child = proximate_graph.add_child(node_class(label=trial_label))
            suggestions = suggest_connections(channel, trial_child, stop_early=True)
        finally:
            proximate_graph.remove_child(trial_label)
        if len(suggestions) > 0:
            candidate_classes.append(node_class)
    return candidate_classes
