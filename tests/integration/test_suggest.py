"""
These are in integration tests because they are too slow (10-20s).
"""

import unittest

import rdflib
from semantikon.metadata import u

import pyiron_workflow as pwf
from pyiron_workflow.channels import ChannelConnectionError
from pyiron_workflow.suggest import suggest_connections, suggest_nodes

EX = rdflib.Namespace("http://example.org/")


class Storage:
    def __init__(self, contents=None):
        self.contents = contents


@pwf.as_function_node
def AddNuts(
    gets_nuts: u(Storage, uri=EX.Shelf),
) -> u(Storage, derived_from="inputs.gets_nuts", triples=(EX.hasComponents, EX.nut)):
    has_nuts = gets_nuts
    return has_nuts


@pwf.as_function_node
def AddWashers(
    gets_washer: u(Storage, uri=EX.Shelf),
) -> u(
    Storage, derived_from="inputs.gets_washer", triples=(EX.hasComponents, EX.washer)
):
    # Like a washer, the purpose here is to go between the nuts and bolts
    # In our case, to make sure we catch and avoid circular connection suggestions
    has_washer = gets_washer
    return has_washer


@pwf.as_function_node
def AddBolts(
    gets_bolts: u(Storage, uri=EX.Shelf),
) -> u(Storage, derived_from="inputs.gets_bolts", triples=(EX.hasComponents, EX.bolt)):
    has_bolts = gets_bolts
    return has_bolts


@pwf.as_function_node
def AssembleShelf(
    to_assemble: u(
        Storage,
        uri=EX.Shelf,
        restrictions=(
            (
                (rdflib.OWL.onProperty, EX.hasComponents),
                (rdflib.OWL.someValuesFrom, EX.bolt),
            ),
            (
                (rdflib.OWL.onProperty, EX.hasComponents),
                (rdflib.OWL.someValuesFrom, EX.washer),
            ),
            (
                (rdflib.OWL.onProperty, EX.hasComponents),
                (rdflib.OWL.someValuesFrom, EX.nut),
            ),
        ),
    ),
) -> u(Storage, derived_from="inputs.to_assemble", triples=(EX.hasState, EX.assembled)):
    assembled = to_assemble
    return assembled


@pwf.as_function_node
def PlaceBooks(
    bookshelf: u(
        Storage,
        uri=EX.Shelf,
        restrictions=(
            (rdflib.OWL.onProperty, EX.hasState),
            (rdflib.OWL.someValuesFrom, EX.assembled),
        ),
    ),
    *books: str,
) -> u(Storage, derived_from="inputs.bookshelf", triples=(EX.hasContents, EX.book)):
    bookshelf.contents = books
    return bookshelf


@pwf.as_function_node
def OnlyType(shelf: Storage) -> Storage:
    return shelf


@pwf.as_function_node
def NoHints(shelf):
    return shelf


@pwf.as_function_node
def WrongHint(
    fridge: u(Storage, uri=EX.Fridge),
) -> u(Storage, derived_from="inputs.fridge"):
    return fridge


NODE_CORPUS = (
    AddNuts,
    AddWashers,
    AddBolts,
    AssembleShelf,
    PlaceBooks,
    OnlyType,
    NoHints,
    WrongHint,
)


class TestSuggest(unittest.TestCase):

    def setUp(self):
        self.wf = pwf.Workflow("storage")
        self.wf.nuts = AddNuts()
        self.wf.washers = AddWashers(self.wf.nuts)
        self.wf.bolts = AddBolts(self.wf.washers)
        self.wf.shelf = AssembleShelf()
        self.wf.bookshelf = PlaceBooks()
        self.wf.only_type = OnlyType()
        self.wf.no_hints = NoHints()
        self.wf.wrong_hint = WrongHint()

    def test_unfulfilled_rstrictions(self):
        with self.assertRaises(
            ChannelConnectionError,
            msg="In principle, I _do_ want this to be OK -- shelf is promising to "
            "provide a constructed shelf. Semantikon disallows this because the "
            "restrictions from the assembly stage get inherited along with the other "
            "properties. Cf. "
            "https://github.com/pyiron/semantikon/issues/262#issue-3381086039",
        ):
            self.wf.bookshelf.inputs.bookshelf = self.wf.shelf.outputs.assembled

    def test_suggest_connections(self):
        print("CONNECTION SUGGESTIONS")
        print("\nINPUT SUGGESTIONS")
        for c in self.wf.children.values():
            try:
                print(
                    c.full_label,
                    [
                        f"{n.label}.{cc.label}"
                        for (n, cc) in suggest_connections(
                            next(iter(c.inputs.channel_dict.values()))
                        )
                    ],
                )
            except ValueError:
                print(c.label, "No suggestions")
        print("\nOUTPUT SUGGESTIONS")
        for c in self.wf.children.values():
            try:
                print(
                    c.full_label,
                    [
                        f"{n.label}.{cc.label}"
                        for (n, cc) in suggest_connections(
                            next(iter(c.outputs.channel_dict.values()))
                        )
                    ],
                )
            except ValueError:
                print(c.label, "No suggestions")
        pass

    def test_suggest_nodes(self):
        print("\n\nNODE SUGGESTIONS")
        print("\nINPUT SUGGESTIONS")
        for c in self.wf.children.values():
            try:
                print(
                    c.label,
                    [
                        cl.__name__
                        for cl in suggest_nodes(
                            next(iter(c.inputs.channel_dict.values())), *NODE_CORPUS
                        )
                    ],
                )
            except ValueError:
                print(c.label, "No suggestions")
        print("\nOUTPUT SUGGESTIONS")
        for c in self.wf.children.values():
            try:
                print(
                    c.label,
                    [
                        cl.__name__
                        for cl in suggest_nodes(
                            next(iter(c.outputs.channel_dict.values())), *NODE_CORPUS
                        )
                    ],
                )
            except ValueError:
                print(c.label, "No suggestions")
