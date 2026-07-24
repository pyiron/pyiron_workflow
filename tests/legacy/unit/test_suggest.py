import unittest
from typing import Literal

import pyiron_workflow._legacy as pwf
from pyiron_workflow._legacy.nodes import static_io
from pyiron_workflow._legacy.suggest import (
    ConnectedInputError,
    NonSiblingError,
    UnhintedError,
    suggest_connections,
    suggest_nodes,
)


class Storage:
    def __init__(self, contents=None):
        self.contents = contents


@pwf.as_function_node
def AddNuts(gets_nuts: Storage) -> Storage:
    has_nuts = gets_nuts
    return has_nuts


@pwf.as_function_node
def AddWashers(gets_washer: Storage) -> Storage:
    # Like a washer, the purpose here is to go between the nuts and bolts
    # In our case, to make sure we catch and avoid circular connection suggestions
    has_washer = gets_washer
    return has_washer


@pwf.as_function_node
def AddBolts(gets_bolts: Storage) -> Storage:
    has_bolts = gets_bolts
    return has_bolts


@pwf.as_function_node
def AssembleShelf(to_assemble: Storage) -> Storage:
    assembled = to_assemble
    return assembled


@pwf.as_function_node
def PlaceBooks(bookshelf: Storage, *books: str) -> Storage:
    bookshelf.contents = books
    return bookshelf


@pwf.as_function_node
def OnlyType(storage: Storage) -> Storage:
    return storage


@pwf.as_function_node
def NoHints(shelf):
    return shelf


@pwf.as_function_node
def MultipleChannels(
    data_typed: Storage,
    wrong_data_type: str,
) -> tuple[Storage, str]:
    return data_typed, wrong_data_type


NODE_CORPUS = (
    AddNuts,
    AddWashers,
    AddBolts,
    AssembleShelf,
    PlaceBooks,
    OnlyType,
    NoHints,
)


class TestSuggest(unittest.TestCase):

    def setUp(self):
        self.wf = pwf.Workflow("storage")
        self.wf.nuts = AddNuts()
        self.wf.washers = AddWashers(self.wf.nuts)
        self.wf.bolts = AddBolts(self.wf.washers)
        self.wf.assembled = AssembleShelf()
        self.wf.bookshelf = PlaceBooks()
        self.wf.only_type = OnlyType()
        self.wf.no_hints = NoHints()

    def test_exceptions(self):
        with self.subTest("Connected input"):
            for channel in [
                self.wf.washers.inputs.gets_washer,
                self.wf.bolts.inputs.gets_bolts,
            ]:
                for suggester in (suggest_connections, suggest_nodes):
                    with self.assertRaises(
                        ConnectedInputError,
                        msg="These two already have input connections, they must be "
                        "disconnected to get a suggestion",
                    ):
                        suggester(channel)

        with self.subTest("Non-sibling nodes"):
            not_in_graph = AddNuts()
            with self.assertRaises(
                NonSiblingError,
                msg="The default corpus looks for siblings, but if an invalid corpus "
                "is explicitly provided, we want to fail cleanly",
            ):
                suggest_connections(not_in_graph.inputs.gets_nuts, not_in_graph)

        with self.subTest("Unhinted"):
            for suggester in (suggest_connections, suggest_nodes):
                with self.assertRaises(
                    UnhintedError,
                    msg="The node has no hinting, so it cannot be suggested",
                ):
                    suggester(self.wf.no_hints.inputs.shelf)

    def _build_connection_suggestions(
        self, io: Literal["inputs", "outputs"]
    ) -> dict[static_io.StaticNode, list[static_io.StaticNode] | None]:
        """
        We're just playing with single-input/single-output nodes here, so let's keep
        things simple by mapping the suggestions from node-to-node directly.
        We can add one or two little edge cases later for channel-specific stuff.
        """
        suggestions = {}
        for node in self.wf.children.values():
            try:
                channel = next(iter(getattr(node, io).channel_dict.values()))
                suggestions[node] = [
                    node for (node, channel) in suggest_connections(channel)
                ]
            except ValueError:
                suggestions[node] = None
        return suggestions

    def test_input_connection_suggestions(self):
        suggestions = self._build_connection_suggestions("inputs")

        with self.subTest("No input suggestions for connected input"):
            self.assertIsNone(suggestions[self.wf.washers])
            self.assertIsNone(suggestions[self.wf.bolts])
        suggestions.pop(self.wf.washers)
        suggestions.pop(self.wf.bolts)

        with self.subTest("Without any hinting, no suggestions should happen"):
            for target, suggested in suggestions.items():
                if target is self.wf.no_hints:
                    self.assertIsNone(suggested)
                else:
                    self.assertNotIn(self.wf.no_hints, suggested)
        suggestions.pop(self.wf.no_hints)

        with self.subTest("Type-compatible siblings are suggested"):
            for target, suggested in suggestions.items():
                if target is self.wf.only_type:
                    all_siblings = list(self.wf.children.values())
                    all_siblings.remove(target)
                    all_siblings.remove(self.wf.no_hints)
                    self.assertListEqual(suggested, all_siblings)
                else:
                    self.assertIn(self.wf.only_type, suggested)

        with self.subTest("No cyclic suggestions"):
            self.assertNotIn(self.wf.washers, suggestions[self.wf.nuts])
            self.assertNotIn(self.wf.bolts, suggestions[self.wf.nuts])

        with self.subTest("Type-compatible suggestions present"):
            self.assertIn(self.wf.bolts, suggestions[self.wf.assembled])
            self.assertIn(self.wf.assembled, suggestions[self.wf.bookshelf])

        # Now make modifications and re-test
        self.wf.assembled.inputs.to_assemble = self.wf.bolts
        with self.subTest("Suggestions survive an upstream connection"):
            self.assertIn(
                (self.wf.assembled, self.wf.assembled.outputs.assembled),
                suggest_connections(self.wf.bookshelf.inputs.bookshelf),
            )

        self.wf.bolts.disconnect()
        with self.subTest("Cylcic constraint removed"):
            self.assertIn(
                (self.wf.bolts, self.wf.bolts.outputs.has_bolts),
                suggest_connections(self.wf.nuts.inputs.gets_nuts),
            )

    def test_output_connection_suggestions(self):
        suggestions = self._build_connection_suggestions("outputs")

        with self.subTest("No input suggestions for connected input"):
            for suggested in list(filter(None, suggestions.values())):
                self.assertNotIn(self.wf.washers, suggested)
                self.assertNotIn(self.wf.bolts, suggested)

        with self.subTest("Without any hinting, no suggestions should happen"):
            for target, suggested in suggestions.items():
                if target is self.wf.no_hints:
                    self.assertIsNone(suggested)
                else:
                    self.assertNotIn(self.wf.no_hints, suggested)

        with self.subTest("Type-compatible siblings are suggested"):
            for target, suggested in suggestions.items():
                if target is self.wf.only_type:
                    siblings = list(suggestions.keys())
                    siblings.remove(target)
                    siblings.remove(self.wf.no_hints)
                    siblings.remove(self.wf.washers)  # Already connected
                    siblings.remove(self.wf.bolts)  # Already connected
                    self.assertListEqual(suggested, siblings)
                elif suggested is not None:
                    self.assertIn(self.wf.only_type, suggested)

        with self.subTest("No cyclic suggestions"):
            self.assertNotIn(self.wf.nuts, suggestions[self.wf.bolts])

        with self.subTest("Type-compatible suggestions present"):
            self.assertIn(self.wf.assembled, suggestions[self.wf.bolts])
            self.assertIn(self.wf.bookshelf, suggestions[self.wf.assembled])

        # Now make modifications and re-test
        self.wf.assembled.inputs.to_assemble = self.wf.bolts
        with self.subTest("Suggestions survive an upstream connection"):
            self.assertIn(
                (self.wf.bookshelf, self.wf.bookshelf.inputs.bookshelf),
                suggest_connections(self.wf.assembled.outputs.assembled),
            )

        self.wf.bolts.disconnect()
        with self.subTest("Cylcic constraint removed"):
            self.assertIn(
                (self.wf.nuts, self.wf.nuts.inputs.gets_nuts),
                suggest_connections(self.wf.bolts.outputs.has_bolts),
            )

    def _build_node_suggestions(
        self, io: Literal["inputs", "outputs"]
    ) -> dict[static_io.StaticNode, list[static_io.StaticNode] | None]:
        """
        We're just playing with single-input/single-output nodes here, so let's keep
        things simple by mapping the suggestions from node-to-node directly.
        We can add one or two little edge cases later for channel-specific stuff.
        """
        suggestions: dict[static_io.StaticNode, list[static_io.StaticNode] | None] = {}
        for node in self.wf.children.values():
            try:
                channel = next(iter(getattr(node, io).channel_dict.values()))
                suggestions[node] = suggest_nodes(channel, *NODE_CORPUS)
            except ValueError:
                suggestions[node] = None
        return suggestions

    def test_input_node_suggestions(self):
        suggestions = self._build_node_suggestions("inputs")

        with self.subTest("No input suggestions for connected input"):
            self.assertIsNone(suggestions[self.wf.washers])
            self.assertIsNone(suggestions[self.wf.bolts])

        with self.subTest("Without any hinting, no suggestions should happen"):
            for target, suggested in suggestions.items():
                if target is self.wf.no_hints:
                    self.assertIsNone(suggested)

        with self.subTest("Type-compatible node classes are suggested"):
            for _, suggested in suggestions.items():
                if suggested is not None:
                    self.assertIn(OnlyType, suggested)

            hinted_corpus = list(NODE_CORPUS)
            hinted_corpus.remove(NoHints)
            self.assertListEqual(hinted_corpus, suggestions[self.wf.only_type])

        with self.subTest("Type-compatible suggestion present"):
            self.assertIn(
                AssembleShelf,
                suggestions[self.wf.bookshelf],
            )

    def test_output_node_suggestions(self):
        suggestions = self._build_node_suggestions("outputs")

        with self.subTest("Without any hinting, no suggestions should happen"):
            for target, suggested in suggestions.items():
                if target is self.wf.no_hints:
                    self.assertIsNone(suggested)

        with self.subTest("Type-compatible node classes are suggested"):
            for suggested in suggestions.values():
                if suggested is not None:
                    self.assertIn(OnlyType, suggested)

            hinted_corpus = list(NODE_CORPUS)
            hinted_corpus.remove(NoHints)
            self.assertListEqual(hinted_corpus, suggestions[self.wf.only_type])

        with self.subTest("Type-compatible suggestion present"):
            self.assertIn(PlaceBooks, suggestions[self.wf.assembled])

    def test_limited_corpus(self):
        with self.subTest("Connections"):
            self.assertEqual(
                suggest_connections(
                    self.wf.bolts.outputs.has_bolts,
                    self.wf.only_type,
                    self.wf.no_hints,
                ),
                [(self.wf.only_type, self.wf.only_type.inputs.storage)],
                msg="Only one element of the corpus is valid",
            )

        with self.subTest("Nodes"):
            self.assertEqual(
                suggest_nodes(
                    self.wf.bolts.outputs.has_bolts,
                    OnlyType,
                    NoHints,
                ),
                [OnlyType],
                msg="Only one element of the corpus is valid",
            )

    def test_multiple_channels(self):
        wf = pwf.Workflow("multiple_channels")
        wf.single = AddNuts()
        wf.multiple = MultipleChannels()

        self.assertListEqual(
            [(wf.multiple, wf.multiple.outputs.data_typed)],
            suggest_connections(wf.single.inputs.gets_nuts),
            msg="All correctly typed channels should be suggested.",
        )

        self.assertListEqual(
            [(wf.multiple, wf.multiple.inputs.data_typed)],
            suggest_connections(wf.single.outputs.has_nuts),
            msg="All correctly typed channels should be suggested.",
        )

        for channel in [
            wf.single.inputs.gets_nuts,
            wf.single.outputs.has_nuts,
        ]:
            self.assertListEqual(
                [MultipleChannels],
                suggest_nodes(channel, MultipleChannels, NoHints),
                msg="Each node with at least one viable channel should be suggested.",
            )
