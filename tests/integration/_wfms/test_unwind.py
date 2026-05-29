import unittest

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import api as wfms


def add(n: int, y: int | float) -> float:
    return n + y


class TestUnwind(unittest.TestCase):
    def test_reverting_edges(self):
        """
        New edges get type-checked by default; here, we demonstrate that collective
        "@_undoable" actions get rolled back to the last acceptable state on an
        action failure by adding multiple new edges, the last of which is invalid.
        """
        wf = wfms.Workflow("test")
        wf.create_input("n", type_hint=int)
        wf.create_input("m", type_hint=int)
        wf.create_input("x", type_hint=float)
        wf.create_input("y", type_hint=float)

        # Syntactic sugar recognizes attribute assignment of a plain function as a
        # new atomic node
        wf.matching = add
        wf.as_or_more_specific = add
        wf.wrong = add

        # Set up half the input edges in alignment with type hints
        first_half = [
            wfms.EdgeTuple(
                frs.InputSource(port="n"),
                frs.TargetHandle(node="matching", port="n"),
            ),
            wfms.EdgeTuple(
                frs.InputSource(port="n"),
                frs.TargetHandle(node="as_or_more_specific", port="n"),
            ),
            wfms.EdgeTuple(
                frs.InputSource(port="y"),
                frs.TargetHandle(node="wrong", port="y"),
            ),
        ]
        wf.add_edge(*first_half)

        edges_before = list(wf.edges)
        self.assertListEqual(
            edges_before, first_half, msg="Sanity check that they all got added"
        )

        second_half = [
            wfms.EdgeTuple(  # Perfect match
                frs.InputSource(port="y"),
                frs.TargetHandle(node="matching", port="y"),
            ),
            wfms.EdgeTuple(  # More specific: passing int to float
                frs.InputSource(port="m"),
                frs.TargetHandle(node="as_or_more_specific", port="y"),
            ),
            wfms.EdgeTuple(  # WRONG: passing float to int
                frs.InputSource(port="x"),
                frs.TargetHandle(node="wrong", port="n"),
            ),
        ]

        with self.assertRaisesRegex(
            TypeError, "Processing edge 'x->wrong.n' on 'test'"
        ):
            wf.add_edge(*second_half)

        self.assertListEqual(
            wf.edges,
            edges_before,
            msg="The entire edge additions should be rolled back at the end when the "
            "problem is hit",
        )

        wf.add_edge(*second_half[:-1])
        self.assertListEqual(
            wf.edges,
            first_half + second_half[:-1],
            msg="Sanity check that it was just the terminal edge causing trouble",
        )
