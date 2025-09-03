import bidict
import toposort
from semantikon import metadata as meta

from pyiron_workflow import channels, io, topology, type_hinting
from pyiron_workflow.data import SemantikonRecipeChange
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
                and not _ontologically_valid_pair(upstream, downstream)
            ):
                continue

            # Disallow circular connections
            try:
                downstream.connect(upstream)
                toposort.toposort_flatten(
                    topology.nodes_to_data_digraph(proximate_graph.children)
                )
            except toposort.CircularDependencyError:
                continue
            finally:
                downstream.disconnect(upstream)

            candidates.append((sibling, candidate))

    return candidates


def _ontologically_valid_pair(upstream, downstream):
    # Importing semantikon.ontology is expensive, so we delay importing
    # the knowledge submodule until the last minute
    from pyiron_workflow import knowledge  # noqa: PLC0415

    root = upstream.owner.graph_root
    trial_edge = SemantikonRecipeChange(
        location=str(downstream.owner.lexical_path).split(
            downstream.owner.lexical_delimiter
        )[2:-1],
        new_edge=(
            f"{upstream.owner.label}.outputs.{upstream.label}",
            f"{downstream.owner.label}.inputs.{downstream.label}",
        ),
        parent_input=downstream.scoped_label,
        parent_output=upstream.scoped_label,
    )
    validation = knowledge.validate_workflow(root, trial_edge)
    return knowledge.is_valid(validation)


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

        for _, preview_value in preview.items():
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
            existing_source_connection: list[
                tuple[channels.DataChannel, channels.DataChannel]
            ] = []
            try:
                trial_child = proximate_graph.add_child(node_class(label=trial_label))
                upstream, downstream = (
                    (trial_child, channel)
                    if suggest_for_input
                    else (channel, trial_child)
                )
                existing_source_connection = downstream.disconnect_all()
                if (
                    meta._is_annotated(upstream.type_hint)
                    and meta._is_annotated(downstream.type_hint)
                    and upstream.owner.graph_root._validate_ontologies
                    and upstream.owner.graph_root is downstream.owner.graph_root
                    and not _ontologically_valid_pair(upstream, downstream)
                ):
                    continue
            finally:
                proximate_graph.remove_child(trial_label)
                for _, partner in existing_source_connection:
                    # Should only be 0 or 1 items; iterate to accommodate the data type
                    downstream.connect(partner)
            candidate_classes.append(node_class)

    return candidate_classes
