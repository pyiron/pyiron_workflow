"""
The tests use the concrete `Atomic` / `Macro` instances exposed by
`_fixtures` (rather than rolling custom subclasses) wherever possible so we
exercise the abstract `Node` / `StaticNode` surface as it is used in
practice.
"""

from __future__ import annotations

import unittest
from concurrent import futures

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
        self.assertIsNotNone(child.owner)
        state = child.__getstate__()
        self.assertIsNone(state["_owner"])
        self.assertEqual(state["_detached_root"], m.lexical_path)

    def test_owner_absent_records_neither_key(self):
        n = _fixtures.atomic_add_node()
        state = n.__getstate__()
        self.assertIsNone(state["_owner"])
        self.assertIsNone(state["_detached_root"])

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

    def test_getstate_does_not_mutate_live_node(self):
        m = _fixtures.macro_node()
        child = m.nodes["add_0"]
        owner = child.owner
        with futures.ThreadPoolExecutor(max_workers=1) as exe:
            child.executor = exe
            child.__getstate__()
            self.assertIs(
                child.executor,
                exe,
                msg="__getstate__ must strip the executor from the returned state "
                "only, not from the live node",
            )
            self.assertIs(
                child.owner,
                owner,
                msg="__getstate__ must not detach the live node from its owner",
            )


if __name__ == "__main__":
    unittest.main()
