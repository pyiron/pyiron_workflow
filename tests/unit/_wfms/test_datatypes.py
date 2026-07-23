"""
The tests use the concrete `Atomic` / `Macro` instances exposed by
`_fixtures` (rather than rolling custom subclasses) wherever possible so we
exercise the abstract `Node` / `StaticNode` surface as it is used in
practice.
"""

from __future__ import annotations

import datetime
import pathlib
import unittest
from concurrent import futures

from pyiron_workflow._wfms import dag, datatypes, execution
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
    """
    Node ownership is read-only, and managed privately by node maps.

    To accommodate forflow building a runtime DAG, we don't guarantee that all owned
    nodes appear in a graph's `.nodes` attribute, _but_ we do guarantee that all nodes
    belonging to a `NodeMap` share that node map's owner.
    """

    def test_map_assignment_parents_nodes(self):
        n = _fixtures.atomic_add_node()
        owner = _fixtures.macro_node("owner_b")
        self.assertIsNone(n.owner)
        datatypes.NodeMap(owner, {"some_label": n})
        self.assertIs(n.owner, owner)

    def test_reparenting_raises_value_error(self):
        n = _fixtures.atomic_add_node()
        owner_a = _fixtures.macro_node("owner_a")
        owner_b = _fixtures.macro_node("owner_b")
        datatypes.NodeMap(owner_a, {"some_label": n})
        with self.assertRaises(ValueError) as ctx:
            datatypes.NodeMap(owner_b, {"some_label": n})
        message = str(ctx.exception)
        self.assertIn("owner_a", message)
        self.assertIn("owner_b", message)
        # And the owner was not mutated.
        self.assertIs(n.owner, owner_a)

    def test_idempotent_reassignment_is_silent(self):
        n = _fixtures.atomic_add_node()
        owner = _fixtures.macro_node("owner_a")
        datatypes.NodeMap(owner, {"some_label": n})
        self.assertIs(n.owner, owner)
        datatypes.NodeMap(owner, {"some_other_label": n})
        self.assertIs(n.owner, owner)


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
        self.assertEqual(run.outputs[out_label], 3)

    def test_run_forwards_positional_config(self):
        """A config passed positionally reaches `execution.run`."""
        n = _fixtures.atomic_add_node()
        calls: list[execution.RunStatus] = []

        def hook(
            run_dir: pathlib.Path,
            time: datetime.datetime,
            lexical_path: str,
            status: execution.RunStatus,
        ) -> None:
            calls.append(status)

        config = execution.RunConfig(
            progress_hooks=[execution.ProgressHook(hook, True)]
        )
        run = n.run(config, x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        # The default config carries no hooks, so a populated `calls` proves the
        # custom config was forwarded through to `execution.run`.
        self.assertIn(execution.RunStatus.RUNNING, calls)
        self.assertIn(execution.RunStatus.FINISHED, calls)

    def test_run_positional_config_does_not_pollute_inputs(self):
        """A positional config is consumed by `run`, not the input data."""
        n = _fixtures.atomic_add_node()
        seen_paths: list[str] = []

        def hook(
            run_dir: pathlib.Path,
            time: datetime.datetime,
            lexical_path: str,
            status: execution.RunStatus,
        ) -> None:
            seen_paths.append(lexical_path)

        config = execution.RunConfig(
            progress_hooks=[execution.ProgressHook(hook, True)]
        )
        run = n.run(config, x=1, y=2)
        # The config is forwarded (hook fired) *and* the keyword inputs still
        # reach the node and compute correctly -- the config did not leak into
        # `**input_data`.
        self.assertTrue(seen_paths)
        out_label = next(iter(run.outputs.keys()))
        self.assertEqual(run.outputs[out_label], 3)


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


class TestNodeCopy(unittest.TestCase):
    def test_atomic_copy_carries_executor_drops_parentage(self):
        n = _fixtures.atomic_add_node()
        exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 1},
        )
        n.executor = exe
        n.last_run = "fake history"

        copy = n.copy()

        self.assertIsNot(copy, n, "copy must be a fresh object")
        self.assertIs(copy.executor, exe, "executor must ride along")
        self.assertIsNone(copy.owner, "parentage must not be carried")
        self.assertIsNone(copy.last_run, "history must not be carried")

    def test_nested_macro_copy_carries_executors_and_reparents(self):
        nm = _fixtures.nested_macro_node()
        child = nm.nodes["macro_0"]
        grandchild = child.nodes["add_0"]

        top_exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 1},
        )
        child_exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 2},
        )
        grandchild_exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 3},
        )
        nm.executor = top_exe
        child.executor = child_exe
        grandchild.executor = grandchild_exe

        copy = nm.copy()
        copied_child = copy.nodes["macro_0"]
        copied_grandchild = copied_child.nodes["add_0"]

        # Executors ride at every depth.
        self.assertIs(copy.executor, top_exe)
        self.assertIs(copied_child.executor, child_exe)
        self.assertIs(copied_grandchild.executor, grandchild_exe)

        # Copies are fresh objects re-parented into the new tree.
        self.assertIsNot(copied_child, child)
        self.assertIsNone(copy.owner)
        self.assertIs(copied_child.owner, copy)

        # The original tree is untouched.
        self.assertIs(nm.nodes["macro_0"].owner, nm)


class TestConnectAtInit(unittest.TestCase):
    """`StaticNode` / `StaticGraph` connection sugar at construction.

    Exercised through `Macro`, a concrete `StaticGraph` that reaches the
    `StaticNode.connect` call only via `super().__init__` -- so the one class
    covers both the connect logic and the `StaticGraph` super-call chain. With
    no owner at construction every connection takes the 'pending' route.
    """

    @staticmethod
    def _macro(**connections):
        return dag.Macro("m", _fixtures.macro.flowrep_recipe, **connections)

    def test_connect_port_is_pending(self):
        src = _fixtures.atomic_add_node("src")
        port = src.outputs["output_0"]
        m = self._macro(x=port)
        self.assertIs(m._pending_connections["x"], port)

    def test_connect_single_output_node_coerces_to_its_port(self):
        src = _fixtures.atomic_add_node("src")
        m = self._macro(y=src)
        self.assertIs(m._pending_connections["y"], src.outputs["output_0"])

    def test_connect_multi_output_node_raises(self):
        multi = _fixtures.macro_node("multi")  # outputs `a` and `s`
        with self.assertRaises(ValueError):
            self._macro(x=multi)

    def test_connect_wrong_type_raises(self):
        with self.assertRaises(TypeError):
            self._macro(x=42)

    def test_connections_stay_pending_without_owner(self):
        src = _fixtures.atomic_add_node("src")
        m = self._macro(x=src.outputs["output_0"], y=src)
        self.assertIsNone(m.owner)
        self.assertEqual(set(m._pending_connections), {"x", "y"})

    def test_positional_connection_zips_to_first_input(self):
        # Positional connections at construction map onto input ports in order;
        # the fixture's inputs are (x, y, z), so a lone positional lands on `x`.
        src = _fixtures.atomic_add_node("src")
        port = src.outputs["output_0"]
        m = dag.Macro("m", _fixtures.macro.flowrep_recipe, port)
        self.assertIs(m._pending_connections["x"], port)

    def test_positional_and_keyword_connections_combine(self):
        src = _fixtures.atomic_add_node("src")
        port = src.outputs["output_0"]
        m = dag.Macro("m", _fixtures.macro.flowrep_recipe, port, z=src)
        self.assertIs(m._pending_connections["x"], port)
        self.assertIs(m._pending_connections["z"], src.outputs["output_0"])


if __name__ == "__main__":
    unittest.main()
