"""
The tests use the concrete `Atomic` / `Macro` instances exposed by
`_fixtures` (rather than rolling custom subclasses) wherever possible so we
exercise the abstract `Node` / `StaticNode` surface as it is used in
practice.
"""

from __future__ import annotations

import typing
import unittest
from concurrent import futures

from flowrep.api import schemas as frs

from pyiron_workflow._wfms import datatypes, execution
from tests.unit._wfms import _fixtures


class TestPort(unittest.TestCase):
    def test_input_port_lexical_path_one_level(self):
        m = _fixtures.macro_node()
        port = m.nodes["add_0"].inputs["x"]
        self.assertEqual(port.lexical_path, "my_macro.add_0.inputs.x")

    def test_input_port_lexical_path_on_macro_itself(self):
        m = _fixtures.macro_node()
        self.assertEqual(m.inputs["x"].lexical_path, "my_macro.inputs.x")

    def test_input_port_lexical_path_nested(self):
        nm = _fixtures.nested_macro_node()
        port = nm.nodes["macro_0"].nodes["add_0"].inputs["x"]
        self.assertEqual(port.lexical_path, "my_nested_macro.macro_0.add_0.inputs.x")

    def test_output_port_lexical_path(self):
        m = _fixtures.macro_node()
        out_label = next(iter(m.nodes["add_0"].outputs.keys()))
        port = m.nodes["add_0"].outputs[out_label]
        self.assertEqual(port.lexical_path, f"my_macro.add_0.outputs.{out_label}")


class TestInputPort(unittest.TestCase):
    def test_io_indicator(self):
        self.assertEqual(datatypes.InputPort._io_indicator, "inputs")

    def test_has_default_false_when_recipe_has_no_defaults(self):
        n = _fixtures.atomic_add_node()
        # `add(x, y)` has no default parameters, so the fixture's recipe
        # exposes `inputs_with_defaults == []`.
        self.assertEqual(n.recipe.inputs_with_defaults, [])
        for label, port in n.inputs.items():
            self.assertFalse(port.has_default, msg=label)

    def test_has_default_true_when_constructed_with_default(self):
        # Directly exercise the dataclass field — the StaticNode builder sets
        # this based on `recipe.inputs_with_defaults`; we verify the
        # InputPort *carries* the bit faithfully.
        owner = _fixtures.atomic_add_node()
        port = datatypes.InputPort(
            label="x",
            owner=owner,
            type_hint=None,
            type_metadata=None,
            has_default=True,
        )
        self.assertTrue(port.has_default)


class TestOutputPort(unittest.TestCase):
    def test_io_indicator(self):
        self.assertEqual(datatypes.OutputPort._io_indicator, "outputs")


class TestNodeLabel(unittest.TestCase):
    def test_label_round_trip(self):
        with self.subTest("No owner"):
            n = _fixtures.atomic_add_node(label="initial")
            self.assertEqual(n.label, "initial")
            n.label = "renamed"
            self.assertEqual(n.label, "renamed")

        with self.subTest("With owner"):
            n = _fixtures.passthrough_node(label="owner")
            with self.assertRaises(ValueError):
                n.nodes["add_0"].label = "child"


class TestNodeOwner(unittest.TestCase):
    def test_initial_parenting_is_silent(self):
        n = _fixtures.atomic_add_node()
        owner = _fixtures.macro_node("owner_b")
        self.assertIsNone(n.owner)
        n.owner = owner
        self.assertIs(n.owner, owner)

    def test_reparenting_raises_value_error(self):
        n = _fixtures.atomic_add_node()
        owner_a = _fixtures.macro_node("owner_a")
        owner_b = _fixtures.macro_node("owner_b")
        n.owner = owner_a
        with self.assertRaises(ValueError) as ctx:
            n.owner = owner_b
        message = str(ctx.exception)
        self.assertIn("owner_a", message)
        self.assertIn("owner_b", message)
        # And the owner was not mutated.
        self.assertIs(n.owner, owner_a)

    def test_idempotent_reassignment_is_silent(self):
        n = _fixtures.atomic_add_node()
        owner = _fixtures.macro_node("owner_a")
        n.owner = owner
        n.owner = owner  # same owner — must not raise
        self.assertIs(n.owner, owner)

    def test_detach_is_silent(self):
        n = _fixtures.atomic_add_node()
        owner = _fixtures.macro_node("owner_a")
        n.owner = owner
        n.owner = None
        self.assertIsNone(n.owner)


class TestNodeLexicalPath(unittest.TestCase):
    def test_unowned_returns_label_only(self):
        n = _fixtures.atomic_add_node(label="solo")
        self.assertEqual(n.lexical_path, "solo")

    def test_one_level(self):
        m = _fixtures.macro_node()
        child = m.nodes["add_0"]
        self.assertEqual(child.lexical_path, "my_macro.add_0")

    def test_two_levels(self):
        nm = _fixtures.nested_macro_node()
        grandchild = nm.nodes["macro_0"].nodes["add_0"]
        self.assertEqual(grandchild.lexical_path, "my_nested_macro.macro_0.add_0")


class TestNodeRun(unittest.TestCase):
    def test_run_integration(self):
        n = _fixtures.atomic_add_node()
        run = n.run(x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        # The fixture's `add` function returns a single value; flowrep's
        # parser auto-labels it `output_0`.
        out_label = next(iter(run.outputs.keys()))
        self.assertEqual(run.outputs[out_label].value, 3)


class TestNodeGetState(unittest.TestCase):
    def test_owner_present_records_last_detached_path(self):
        m = _fixtures.macro_node()
        child = m.nodes["add_0"]
        state = child.__getstate__()
        self.assertNotIn("_owner", state)
        self.assertEqual(state["_last_detached_path"], m.lexical_path)

    def test_owner_absent_records_neither_key(self):
        n = _fixtures.atomic_add_node()
        state = n.__getstate__()
        self.assertNotIn("_owner", state)
        self.assertNotIn("_last_detached_path", state)

    def test_live_executor_stripped_to_none(self):
        n = _fixtures.atomic_add_node()
        with futures.ThreadPoolExecutor(max_workers=1) as exe:
            n.executor = exe
            state = n.__getstate__()
        self.assertIsNone(state["executor"])

    def test_executor_instructions_tuple_preserved(self):
        n = _fixtures.atomic_add_node()
        instructions: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 2},
        )
        n.executor = instructions
        state = n.__getstate__()
        self.assertEqual(state["executor"], instructions)

    def test_none_executor_preserved(self):
        n = _fixtures.atomic_add_node()
        n.executor = None
        state = n.__getstate__()
        self.assertIsNone(state["executor"])


class _RecordingFC(datatypes.FlowControl[frs.ForEachNode, frs.LiveForEach]):
    """
    Minimal concrete `FlowControl` whose `_build_retrospective_*`
    methods record the run they were called with and return sentinel values.

    Reused by `TestFlowControlRetrospectiveFallbacks`.
    """

    def __init__(self, label: frs.Label, recipe: frs.ForEachNode):
        super().__init__(label, recipe)
        self.calls: list[tuple[str, object]] = []

    @classmethod
    def _result_type(cls) -> type[frs.LiveForEach]:
        return frs.LiveForEach

    # Prospective stubs — not exercised here, but required to satisfy the ABC.
    @property
    def prospective_input_edges(self) -> frs.InputEdges:
        return {}

    @property
    def prospective_edges(self) -> frs.Edges:
        return {}

    @property
    def prospective_output_edges(self) -> frs.ProspectiveOutputEdges:
        return {}

    @property
    def prospective_nodes(self) -> datatypes.NodeMap:
        return datatypes.NodeMap(self)

    def evaluate(self, run, config) -> None:  # pragma: no cover - unused
        raise NotImplementedError

    # Sentinels — typed as `Any` for test purposes.
    def _build_retrospective_nodes(self, run):  # type: ignore[override]
        self.calls.append(("nodes", run))
        return "sentinel_nodes"


class _SentinelResult:
    input_edges = "sentinel_input_edges"
    edges = "sentinel_edges"
    output_edges = "sentinel_output_edges"


class _SentinelRun:
    result = _SentinelResult()


class TestFlowControlRetrospectiveFallbacks(unittest.TestCase):
    """
    The four retrospective `Graph` properties short-circuit to empty
    forms when `current_run is None` and otherwise delegate to their
    matching `_build_retrospective_*` method.
    """

    def setUp(self):
        # Borrow a real `ForEachNode` recipe from the for_wf fixture so
        # `StaticNode.__init__` can build live input/output ports.
        wrapper = _fixtures.for_wf_node()
        self._foreach_recipe = wrapper.nodes["for_each_0"].recipe
        self.fc = _RecordingFC("fc", self._foreach_recipe)

    def test_no_run_returns_empty_forms_and_does_not_call_builder(self):
        self.fc.current_run = None

        self.assertEqual(self.fc.input_edges, {})
        self.assertEqual(self.fc.edges, {})
        self.assertEqual(self.fc.output_edges, {})

        nodes = self.fc.nodes
        self.assertIsInstance(nodes, datatypes.NodeMap)
        self.assertEqual(len(nodes), 0)

        self.assertEqual(self.fc.calls, [])

    def test_with_run_delegates_to_builders(self):
        sentinel_run = typing.cast(execution.Run[frs.LiveForEach], _SentinelRun())
        self.fc.current_run = sentinel_run

        self.assertEqual(self.fc.input_edges, _SentinelResult.input_edges)
        self.assertEqual(self.fc.edges, _SentinelResult.edges)
        self.assertEqual(self.fc.output_edges, _SentinelResult.output_edges)
        self.assertEqual(self.fc.nodes, "sentinel_nodes")

        self.assertEqual(self.fc.calls, [("nodes", sentinel_run)])


if __name__ == "__main__":
    unittest.main()
