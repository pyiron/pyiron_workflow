from __future__ import annotations

import collections
import contextlib
import dataclasses
import pickle
import unittest

import semantikon
from _wfms import datatypes, workflow
from flowrep.api import schemas as frs
from unit._wfms import _fixtures

from pyiron_workflow._wfms import atomic, dag


def plain_increment(x):
    return x + 1


class TestMutablePortMap(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.other = workflow.Workflow("other")
        self.port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )

    def test_setitem_matching_owner_succeeds(self) -> None:
        self.wf.inputs["x"] = self.port
        self.assertIs(self.wf.inputs["x"], self.port)

    def test_setitem_mismatched_owner_raises(self) -> None:
        foreign = datatypes.InputPort(
            label="y", owner=self.other, type_hint=None, type_metadata=None
        )
        with self.assertRaisesRegex(
            ValueError,
            "Port 'y' already has owner 'other' and cannot be assigned to a port map with owner 'wf'",
            msg="Readable references to both owners should appear",
        ):
            self.wf.inputs["y"] = foreign

    def test_delitem_removes_entry(self) -> None:
        self.wf.inputs["x"] = self.port
        self.assertIn("x", self.wf.inputs)
        del self.wf.inputs["x"]
        self.assertNotIn("x", self.wf.inputs)


class TestWorkflowInit(unittest.TestCase):
    def test_empty_workflow_has_empty_maps(self) -> None:
        wf = workflow.Workflow("wf")
        self.assertEqual(len(wf.inputs), 0)
        self.assertEqual(len(wf.outputs), 0)
        self.assertEqual(len(wf.nodes), 0)

    def test_custom_undo_limit(self) -> None:
        explicit_limit = 3

        with self.subTest("Baseline"):
            wf = workflow.Workflow("wf")
            self.assertIsInstance(wf.undo_stack, collections.deque)
            self.assertIsInstance(wf.redo_stack, collections.deque)
            self.assertNotEqual(wf.undo_stack.maxlen, explicit_limit)
            self.assertNotEqual(wf.redo_stack.maxlen, explicit_limit)

        with self.subTest("Explicit limit set"):
            wf = workflow.Workflow("wf", undo_limit=explicit_limit)
            self.assertEqual(wf.undo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.redo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.undo_limit, explicit_limit)

    def test_diff_accumulator_starts_none(self) -> None:
        wf = workflow.Workflow("wf")
        self.assertIsNone(wf._diff_accumulator)


class TestWorkflowUndoLimit(unittest.TestCase):
    def test_setter_updates_both_stacks(self) -> None:
        wf = workflow.Workflow("wf", undo_limit=5)
        wf.undo_limit = 12
        self.assertEqual(wf.undo_limit, 12)
        self.assertEqual(wf.undo_stack.maxlen, 12)
        self.assertEqual(wf.redo_stack.maxlen, 12)


class TestMutableNodeMap(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.other = workflow.Workflow("other")

    def test_setitem_unowned_node_reparents(self) -> None:
        node = _fixtures.atomic_add_node()
        self.assertIsNone(node.owner)
        self.wf.nodes["add"] = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["add"], node)

    def test_setitem_foreign_owned_raises(self) -> None:
        node = _fixtures.atomic_add_node()
        self.other.nodes["add"] = node
        self.assertIs(node.owner, self.other)
        with self.assertRaisesRegex(ValueError, "ode 'add' already has owner 'other'"):
            self.wf.nodes["add"] = node

    def test_setattr_delegates_to_setitem(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes.adder = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_delitem_detaches_node(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes["add"] = node
        del self.wf.nodes["add"]
        self.assertIsNone(node.owner)
        self.assertNotIn("add", self.wf.nodes)

    def test_setitem_duplicate_label_raises(self) -> None:
        self.wf.nodes["add"] = _fixtures.atomic_add_node("add")
        with self.assertRaisesRegex(ValueError, "'wf' already has a node 'add'"):
            self.wf.nodes["add"] = _fixtures.atomic_add_node("add")

    def test_setattr_duplicate_label_raises(self) -> None:
        self.wf.nodes.adder = _fixtures.atomic_add_node()
        second = _fixtures.atomic_add_node()
        with self.assertRaisesRegex(ValueError, "already has a node 'adder'"):
            self.wf.nodes.adder = second
        self.assertEqual(
            second.label,
            "add",
            msg="A doomed assignment must not relabel the node",
        )

    def test_setattr_non_node_raises(self) -> None:
        with self.assertRaisesRegex(
            TypeError, "expected a Node, flowrep recipe, or function"
        ):
            self.wf.nodes.adder = 42

    def test_setattr_accepts_recipe(self) -> None:
        self.wf.nodes.m = _fixtures.macro.flowrep_recipe
        self.assertIsInstance(self.wf.nodes["m"], dag.Macro)


class TestNodeAssignmentHelpers(unittest.TestCase):
    """Module-level `_is_node_like` / `_coerce_to_node` helpers."""

    def test_is_node_like_true_for_node(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.atomic_add_node()))

    def test_is_node_like_false_for_non_node(self) -> None:
        self.assertFalse(workflow.is_nodelike(42))
        self.assertFalse(workflow.is_nodelike("a string"))
        self.assertFalse(workflow.is_nodelike(None))

    def test_coerce_to_node_relabels_node(self) -> None:
        node = _fixtures.atomic_add_node("original")
        result = workflow.coerce_to_node(node, "renamed")
        self.assertIs(result, node)
        self.assertEqual(result.label, "renamed")

    def test_coerce_to_node_rejects_non_node(self) -> None:
        with self.assertRaisesRegex(TypeError, "expected a Node"):
            workflow.coerce_to_node(42, "x")

    def test_is_node_like_true_for_recipe(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.add.flowrep_recipe))

    def test_is_node_like_true_for_function(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.add))
        self.assertTrue(workflow.is_nodelike(plain_increment))

    def test_coerce_atomic_recipe(self) -> None:
        result = workflow.coerce_to_node(_fixtures.add.flowrep_recipe, "added")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "added")

    def test_coerce_workflow_recipe(self) -> None:
        result = workflow.coerce_to_node(_fixtures.macro.flowrep_recipe, "m")
        self.assertIsInstance(result, dag.Macro)
        self.assertEqual(result.label, "m")

    def test_coerce_decorated_function(self) -> None:
        result = workflow.coerce_to_node(_fixtures.add, "added")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "added")

    def test_coerce_undecorated_function(self) -> None:
        result = workflow.coerce_to_node(plain_increment, "inc")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "inc")


class TestGraphActions(unittest.TestCase):
    """Each action's inverse() is symmetric and _dispatch applies it correctly."""

    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def _fresh_node(self, label: str = "n") -> datatypes.Node:
        return _fixtures.atomic_add_node(label)

    def _fresh_input(self, label: str = "x") -> datatypes.InputPort:
        return datatypes.InputPort(
            label=label, owner=self.wf, type_hint=None, type_metadata=None
        )

    def _fresh_output(self, label: str = "y") -> datatypes.OutputPort:
        return datatypes.OutputPort(
            label=label, owner=self.wf, type_hint=None, type_metadata=None
        )

    # symmetry

    def test_add_node_inverse_symmetric(self) -> None:
        node = self._fresh_node()
        a = workflow.AddNode(node)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_node_inverse_symmetric(self) -> None:
        node = self._fresh_node()
        a = workflow.RemoveNode(node)
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_edge_inverse_symmetric(self) -> None:
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="n", port="p")
        )
        a = workflow.AddEdge(edge)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_edge_inverse_symmetric(self) -> None:
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="n", port="p")
        )
        a = workflow.RemoveEdge(edge)
        self.assertEqual(a.inverse().inverse(), a)

    def test_rename_node_inverse_symmetric(self) -> None:
        node = self._fresh_node()
        a = workflow.RenameNode(node, "old", "new")
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_input_inverse_symmetric(self) -> None:
        port = self._fresh_input()
        a = workflow.AddInput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_input_inverse_symmetric(self) -> None:
        port = self._fresh_input()
        a = workflow.RemoveInput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_output_inverse_symmetric(self) -> None:
        port = self._fresh_output()
        a = workflow.AddOutput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_output_inverse_symmetric(self) -> None:
        port = self._fresh_output()
        a = workflow.RemoveOutput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_replace_port_inverse_symmetric(self) -> None:
        p1 = self._fresh_input("x")
        p2 = self._fresh_input("y")
        a = workflow.ReplacePort(p1, p2)
        self.assertEqual(a.inverse().inverse(), a)

    # _dispatch correctness

    def test_dispatch_add_remove_node(self) -> None:
        node = self._fresh_node()
        self.wf._dispatch(workflow.AddNode(node))
        self.assertIn("n", self.wf.nodes)
        self.wf._dispatch(workflow.RemoveNode(node))
        self.assertNotIn("n", self.wf.nodes)

    def test_dispatch_add_remove_edge(self) -> None:
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.OutputTarget(port="y")
        )
        self.wf._dispatch(workflow.AddEdge(edge))
        self.assertIn(edge, self.wf.edges)
        self.wf._dispatch(workflow.RemoveEdge(edge))
        self.assertNotIn(edge, self.wf.edges)

    def test_dispatch_add_remove_input(self) -> None:
        port = self._fresh_input()
        self.wf._dispatch(workflow.AddInput(port))
        self.assertIn("x", self.wf.inputs)
        self.wf._dispatch(workflow.RemoveInput(port))
        self.assertNotIn("x", self.wf.inputs)

    def test_dispatch_add_remove_output(self) -> None:
        port = self._fresh_output()
        self.wf._dispatch(workflow.AddOutput(port))
        self.assertIn("y", self.wf.outputs)
        self.wf._dispatch(workflow.RemoveOutput(port))
        self.assertNotIn("y", self.wf.outputs)

    def test_dispatch_replace_port(self) -> None:
        # Use create_input so the port is created with the right module's InputPort.
        self.wf.create_input("x")
        p1 = self.wf.inputs["x"]
        # Build replacement by copying the owned port (preserves the right class).
        p2 = dataclasses.replace(p1, label="z", type_hint=int)
        self.wf._dispatch(workflow.ReplacePort(p1, p2))
        self.assertNotIn("x", self.wf.inputs)
        self.assertIn("z", self.wf.inputs)
        self.assertIs(self.wf.inputs["z"], p2)

    def test_dispatch_rename_node(self) -> None:
        node = self._fresh_node("old")
        self.wf._dispatch(workflow.AddNode(node))
        self.wf._dispatch(workflow.RenameNode(node, "old", "new"))
        self.assertNotIn("old", self.wf.nodes)
        self.assertIn("new", self.wf.nodes)
        self.assertEqual(node.label, "new")

    def test_dispatch_unknown_raises(self) -> None:
        class Bogus:
            def inverse(self) -> Bogus:
                return self

        with self.assertRaises(TypeError):
            self.wf._dispatch(Bogus())  # type: ignore[arg-type]

    def test_replace_port_unknown_raises(self) -> None:
        """_replace_port raises KeyError if the port is not in inputs or outputs."""
        self.wf.create_output("y")
        port = self.wf.outputs["y"]
        # Create a port with a label not in either map
        unknown = dataclasses.replace(port, label="nonexistent")
        with self.assertRaises(KeyError):
            self.wf._replace_port(unknown, port)


class TestRecordsDecorator(unittest.TestCase):
    """_records appends to active accumulator, no-ops when None, returns action."""

    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_appends_to_active_accumulator(self) -> None:
        acc: workflow.GraphDiff = []
        self.wf._diff_accumulator = acc
        port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )
        action = self.wf._add_input(port)
        self.assertIn(action, acc)
        self.assertEqual(len(acc), 1)
        self.wf._diff_accumulator = None  # clean up

    def test_noop_when_accumulator_is_none(self) -> None:
        self.assertIsNone(self.wf._diff_accumulator)
        port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )
        action = self.wf._add_input(port)
        # State was mutated
        self.assertIn("x", self.wf.inputs)
        # But no accumulator was touched (it's still None)
        self.assertIsNone(self.wf._diff_accumulator)
        # Action is still returned
        self.assertIsInstance(action, workflow.AddInput)

    def test_returns_action(self) -> None:
        port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )
        action = self.wf._add_input(port)
        self.assertIsInstance(action, workflow.AddInput)
        self.assertIs(action.port, port)


class TestUndoableDecorator(unittest.TestCase):
    """_undoable manages the accumulator, pushes diff, clears redo, rolls back on failure."""

    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_pushes_diff_on_success(self) -> None:
        initial_len = len(self.wf.undo_stack)
        diff = self.wf.create_input("x")
        self.assertEqual(len(self.wf.undo_stack), initial_len + 1)
        self.assertIs(self.wf.undo_stack[-1], diff)

    def test_clears_redo_stack_on_success(self) -> None:
        self.wf.create_input("x")
        self.wf.undo()
        self.assertEqual(len(self.wf.redo_stack), 1)
        self.wf.create_input("y")
        self.assertEqual(len(self.wf.redo_stack), 0)

    def test_returns_accumulated_diff(self) -> None:
        diff = self.wf.create_input("x")
        self.assertIsInstance(diff, list)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.AddInput)

    def test_accumulator_none_after_call(self) -> None:
        self.wf.create_input("x")
        self.assertIsNone(self.wf._diff_accumulator)

    def test_rollback_on_exception_restores_state(self) -> None:
        other = workflow.Workflow("other")
        node1 = _fixtures.atomic_add_node("n1")
        node2 = _fixtures.atomic_add_node("n2")
        other.add_node(node2)  # node2 is foreign-owned

        initial_undo_len = len(self.wf.undo_stack)
        with self.assertRaises(ValueError):
            self.wf.add_node(node1, node2)

        self.assertNotIn("n1", self.wf.nodes)
        self.assertNotIn("n2", self.wf.nodes)
        self.assertEqual(len(self.wf.undo_stack), initial_undo_len)

    def test_reraises_original_exception(self) -> None:
        other = workflow.Workflow("other")
        node = _fixtures.atomic_add_node()
        other.add_node(node)

        with self.assertRaises(ValueError):
            self.wf.add_node(node)

    def test_accumulator_none_after_exception(self) -> None:
        other = workflow.Workflow("other")
        node = _fixtures.atomic_add_node()
        other.add_node(node)

        with contextlib.suppress(ValueError):
            self.wf.add_node(node)
        self.assertIsNone(self.wf._diff_accumulator)

    def test_nested_call_does_not_double_commit(self) -> None:
        """remove_port_hint calls add_port_hint; only one diff should be pushed."""
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        initial_undo_len = len(self.wf.undo_stack)
        self.wf.remove_port_hint(port)
        self.assertEqual(len(self.wf.undo_stack), initial_undo_len + 1)

    def test_undoable_nested_skips_stack_management(self) -> None:
        """When accumulator is already active, _undoable delegates without pushing stack."""
        outer_acc: workflow.GraphDiff = []
        self.wf._diff_accumulator = outer_acc
        try:
            result = self.wf.create_input("x")
            # Returns raw method result (None), not the accumulator
            self.assertIsNone(result)
            # Accumulator still active, not cleared
            self.assertIs(self.wf._diff_accumulator, outer_acc)
            # Undo stack not pushed
            self.assertEqual(len(self.wf.undo_stack), 0)
            # State was mutated and action appended to outer accumulator
            self.assertIn("x", self.wf.inputs)
            self.assertIsInstance(outer_acc[0], workflow.AddInput)
        finally:
            self.wf._diff_accumulator = None


class TestGetAccessors(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.wf.create_input("x")
        self.wf.create_output("y")

    def test_get_input_by_label(self) -> None:
        port = self.wf.get_input("x")
        self.assertEqual(port.label, "x")
        self.assertIs(port, self.wf.inputs["x"])

    def test_get_input_by_port(self) -> None:
        owned = self.wf.inputs["x"]
        result = self.wf.get_input(owned)
        self.assertIs(result, owned)

    def test_get_input_unknown_label_raises(self) -> None:
        with self.assertRaises(KeyError):
            self.wf.get_input("missing")

    def test_get_input_foreign_port_raises(self) -> None:
        other = workflow.Workflow("other")
        other.create_input("x")
        foreign = other.inputs["x"]
        with self.assertRaises(KeyError):
            self.wf.get_input(foreign)

    def test_get_output_by_label(self) -> None:
        port = self.wf.get_output("y")
        self.assertEqual(port.label, "y")
        self.assertIs(port, self.wf.outputs["y"])

    def test_get_output_by_port(self) -> None:
        owned = self.wf.outputs["y"]
        result = self.wf.get_output(owned)
        self.assertIs(result, owned)

    def test_get_output_unknown_label_raises(self) -> None:
        with self.assertRaises(KeyError):
            self.wf.get_output("missing")

    def test_get_output_foreign_port_raises(self) -> None:
        other = workflow.Workflow("other")
        other.create_output("y")
        foreign = other.outputs["y"]
        with self.assertRaises(KeyError):
            self.wf.get_output(foreign)


class TestNodeMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_add_node_state(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.assertIn("adder", self.wf.nodes)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_add_node_diff(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        diff = self.wf.add_node(node)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.AddNode)
        self.assertIs(diff[0].node, node)

    def test_add_multiple_nodes_diff(self) -> None:
        n1 = _fixtures.atomic_add_node("n1")
        n2 = _fixtures.atomic_sub_node("n2")
        diff = self.wf.add_node(n1, n2)
        self.assertEqual(len(diff), 2)

    def test_remove_node_state(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.remove_node("adder")
        self.assertNotIn("adder", self.wf.nodes)

    def test_remove_node_cascades_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.assertEqual(len(self.wf.edges), 1)
        self.wf.remove_node("adder")
        self.assertEqual(len(self.wf.edges), 0)

    def test_remove_node_diff_includes_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_node("adder")
        action_types = [type(a) for a in diff]
        self.assertIn(workflow.RemoveEdge, action_types)
        self.assertIn(workflow.RemoveNode, action_types)

    def test_remove_node_by_label(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.remove_node("adder")
        self.assertNotIn("adder", self.wf.nodes)

    def test_add_node_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.undo()
        self.assertNotIn("adder", self.wf.nodes)
        self.assertIsNone(node.owner)

    def test_add_node_undo_redo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.undo()
        self.wf.redo()
        self.assertIn("adder", self.wf.nodes)

    def test_rename_node_state(self) -> None:
        node = _fixtures.atomic_add_node("old")
        self.wf.add_node(node)
        self.wf.rename_node("old", "new")
        self.assertNotIn("old", self.wf.nodes)
        self.assertIn("new", self.wf.nodes)
        self.assertEqual(node.label, "new")

    def test_rename_node_rewrites_edges(self) -> None:
        node = _fixtures.atomic_add_node("old")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="old", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_node("old", "new")
        self.assertEqual(len(self.wf.edges), 1)
        src = self.wf.edges[0].source
        self.assertIsInstance(src, frs.SourceHandle)
        self.assertEqual(src.node, "new")  # type: ignore[union-attr]

    def test_rename_node_undo(self) -> None:
        node = _fixtures.atomic_add_node("old")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="old", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_node("old", "new")
        self.wf.undo()
        self.assertIn("old", self.wf.nodes)
        self.assertNotIn("new", self.wf.nodes)
        self.assertEqual(node.label, "old")
        self.assertEqual(self.wf.edges[0].source.node, "old")  # type: ignore[union-attr]

    def test_rename_node_by_node_object(self) -> None:
        node = _fixtures.atomic_add_node("old")
        self.wf.add_node(node)
        self.wf.rename_node(node, "new")
        self.assertIn("new", self.wf.nodes)

    def test_rename_node_rewrites_target_handle(self) -> None:
        """Peer edge whose target is the renamed node gets its target handle rewritten."""
        n1 = _fixtures.atomic_add_node("n1")
        n2 = _fixtures.atomic_add_node("old")
        self.wf.add_node(n1)
        self.wf.add_node(n2)
        peer_edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="n1", port="output_0"),
            frs.TargetHandle(node="old", port="x"),
        )
        self.wf.add_edge(peer_edge)
        self.wf.rename_node("old", "new")
        self.assertEqual(len(self.wf.edges), 1)
        tgt = self.wf.edges[0].target
        self.assertIsInstance(tgt, frs.TargetHandle)
        self.assertEqual(tgt.node, "new")  # type: ignore[union-attr]


class TestEdgeMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.OutputTarget(port="y")
        )

    def test_add_edge_state(self) -> None:
        self.wf.add_edge(self.edge)
        self.assertIn(self.edge, self.wf.edges)

    def test_add_edge_diff(self) -> None:
        diff = self.wf.add_edge(self.edge)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.AddEdge)

    def test_remove_edge_state(self) -> None:
        self.wf.add_edge(self.edge)
        self.wf.remove_edge(self.edge)
        self.assertNotIn(self.edge, self.wf.edges)

    def test_remove_edge_diff(self) -> None:
        self.wf.add_edge(self.edge)
        diff = self.wf.remove_edge(self.edge)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.RemoveEdge)

    def test_remove_edge_undo_regression(self) -> None:
        self.wf.add_edge(self.edge)
        self.wf.remove_edge(self.edge)
        self.assertNotIn(self.edge, self.wf.edges)
        self.wf.undo()
        self.assertIn(self.edge, self.wf.edges)

    def test_disconnect_removes_node_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        e1 = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        e2 = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        unrelated = datatypes.EdgeTuple(
            frs.InputSource(port="z"), frs.OutputTarget(port="w")
        )
        self.wf.add_edge(e1)
        self.wf.add_edge(e2)
        self.wf.add_edge(unrelated)
        self.wf.disconnect("adder")
        self.assertNotIn(e1, self.wf.edges)
        self.assertNotIn(e2, self.wf.edges)
        self.assertIn(unrelated, self.wf.edges)

    def test_disconnect_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.disconnect("adder")
        self.wf.undo()
        self.assertIn(edge, self.wf.edges)


class TestInputPortMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_create_input_state(self) -> None:
        self.wf.create_input("x")
        self.assertIn("x", self.wf.inputs)

    def test_create_input_with_hint(self) -> None:
        self.wf.create_input("x", type_hint=int)
        self.assertEqual(self.wf.inputs["x"].type_hint, int)

    def test_create_input_diff(self) -> None:
        diff = self.wf.create_input("x")
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.AddInput)

    def test_create_input_undo(self) -> None:
        self.wf.create_input("x")
        self.wf.undo()
        self.assertNotIn("x", self.wf.inputs)

    def test_remove_input_by_label(self) -> None:
        self.wf.create_input("x")
        self.wf.remove_input("x")
        self.assertNotIn("x", self.wf.inputs)

    def test_remove_input_by_port(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        self.wf.remove_input(port)
        self.assertNotIn("x", self.wf.inputs)

    def test_remove_input_cascades_edges(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        self.wf.add_edge(edge)
        self.wf.remove_input("x")
        self.assertNotIn(edge, self.wf.edges)

    def test_remove_input_diff_includes_edges(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_input("x")
        types = [type(a) for a in diff]
        self.assertIn(workflow.RemoveEdge, types)
        self.assertIn(workflow.RemoveInput, types)

    def test_remove_input_undo(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        self.wf.add_edge(edge)
        self.wf.remove_input("x")
        self.wf.undo()
        self.assertIn("x", self.wf.inputs)
        self.assertIn(edge, self.wf.edges)

    def test_rename_input_state(self) -> None:
        self.wf.create_input("x")
        self.wf.rename_input("x", "z")
        self.assertNotIn("x", self.wf.inputs)
        self.assertIn("z", self.wf.inputs)

    def test_rename_input_by_port(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        self.wf.rename_input(port, "z")
        self.assertIn("z", self.wf.inputs)

    def test_rename_input_rewrites_edges(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        self.wf.add_edge(edge)
        self.wf.rename_input("x", "z")
        self.assertEqual(len(self.wf.edges), 1)
        src = self.wf.edges[0].source
        self.assertIsInstance(src, frs.InputSource)
        self.assertEqual(src.port, "z")  # type: ignore[union-attr]

    def test_rename_input_undo(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            frs.InputSource(port="x"), frs.TargetHandle(node="adder", port="x")
        )
        self.wf.add_edge(edge)
        self.wf.rename_input("x", "z")
        self.wf.undo()
        self.assertIn("x", self.wf.inputs)
        self.assertNotIn("z", self.wf.inputs)
        self.assertEqual(self.wf.edges[0].source.port, "x")  # type: ignore[union-attr]

    def test_add_port_hint(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        self.assertIsNone(port.type_hint)
        self.wf.add_port_hint(port, int)
        self.assertEqual(self.wf.inputs["x"].type_hint, int)

    def test_add_port_hint_undo(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        self.wf.add_port_hint(port, int)
        self.wf.undo()
        self.assertIsNone(self.wf.inputs["x"].type_hint)

    def test_remove_port_hint(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        self.wf.add_port_hint(port, int)
        updated_port = self.wf.inputs["x"]
        self.wf.remove_port_hint(updated_port)
        self.assertIsNone(self.wf.inputs["x"].type_hint)

    def test_remove_port_hint_returns_diff(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        diff = self.wf.remove_port_hint(port)
        self.assertIsInstance(diff, list)

    def test_add_port_metadata(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        meta = semantikon.TypeMetadata(uri="test://uri")
        self.wf.add_port_metadata(port, meta)
        self.assertEqual(self.wf.inputs["x"].type_metadata, meta)

    def test_remove_port_metadata(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        meta = semantikon.TypeMetadata(uri="test://uri")
        self.wf.add_port_metadata(port, meta)
        updated_port = self.wf.inputs["x"]
        self.wf.remove_port_metadata(updated_port)
        self.assertIsNone(self.wf.inputs["x"].type_metadata)

    def test_remove_port_metadata_returns_diff(self) -> None:
        self.wf.create_input("x")
        port = self.wf.inputs["x"]
        diff = self.wf.remove_port_metadata(port)
        self.assertIsInstance(diff, list)


class TestOutputPortMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_create_output_state(self) -> None:
        self.wf.create_output("y")
        self.assertIn("y", self.wf.outputs)

    def test_create_output_with_hint(self) -> None:
        self.wf.create_output("y", type_hint=str)
        self.assertEqual(self.wf.outputs["y"].type_hint, str)

    def test_create_output_diff(self) -> None:
        diff = self.wf.create_output("y")
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], workflow.AddOutput)

    def test_create_output_undo(self) -> None:
        self.wf.create_output("y")
        self.wf.undo()
        self.assertNotIn("y", self.wf.outputs)

    def test_remove_output_by_label(self) -> None:
        self.wf.create_output("y")
        self.wf.remove_output("y")
        self.assertNotIn("y", self.wf.outputs)

    def test_remove_output_by_port(self) -> None:
        self.wf.create_output("y")
        port = self.wf.outputs["y"]
        self.wf.remove_output(port)
        self.assertNotIn("y", self.wf.outputs)

    def test_remove_output_cascades_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.remove_output("out")
        self.assertNotIn(edge, self.wf.edges)

    def test_remove_output_diff_includes_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_output("out")
        types = [type(a) for a in diff]
        self.assertIn(workflow.RemoveEdge, types)
        self.assertIn(workflow.RemoveOutput, types)

    def test_remove_output_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.remove_output("out")
        self.wf.undo()
        self.assertIn("out", self.wf.outputs)
        self.assertIn(edge, self.wf.edges)

    def test_rename_output_state(self) -> None:
        self.wf.create_output("y")
        self.wf.rename_output("y", "z")
        self.assertNotIn("y", self.wf.outputs)
        self.assertIn("z", self.wf.outputs)

    def test_rename_output_by_port(self) -> None:
        self.wf.create_output("y")
        port = self.wf.outputs["y"]
        self.wf.rename_output(port, "z")
        self.assertIn("z", self.wf.outputs)

    def test_rename_output_rewrites_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_output("out", "result")
        self.assertEqual(len(self.wf.edges), 1)
        tgt = self.wf.edges[0].target
        self.assertIsInstance(tgt, frs.OutputTarget)
        self.assertEqual(tgt.port, "result")  # type: ignore[union-attr]

    def test_rename_output_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="adder", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_output("out", "result")
        self.wf.undo()
        self.assertIn("out", self.wf.outputs)
        self.assertNotIn("result", self.wf.outputs)
        self.assertEqual(self.wf.edges[0].target.port, "out")  # type: ignore[union-attr]

    def test_add_port_hint_output(self) -> None:
        self.wf.create_output("y")
        port = self.wf.outputs["y"]
        self.wf.add_port_hint(port, float)
        self.assertEqual(self.wf.outputs["y"].type_hint, float)

    def test_remove_port_hint_output(self) -> None:
        self.wf.create_output("y")
        port = self.wf.outputs["y"]
        self.wf.add_port_hint(port, float)
        updated = self.wf.outputs["y"]
        self.wf.remove_port_hint(updated)
        self.assertIsNone(self.wf.outputs["y"].type_hint)


class TestUndoRedo(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_undo_empty_stack_is_noop(self) -> None:
        result = self.wf.undo()
        self.assertEqual(result, [])

    def test_redo_empty_stack_is_noop(self) -> None:
        result = self.wf.redo()
        self.assertEqual(result, [])

    def test_undo_over_request_stops_at_zero(self) -> None:
        self.wf.create_input("x")
        result = self.wf.undo(steps=5)
        self.assertEqual(len(result), 1)
        self.assertNotIn("x", self.wf.inputs)

    def test_redo_over_request_stops_at_zero(self) -> None:
        self.wf.create_input("x")
        self.wf.undo()
        result = self.wf.redo(steps=5)
        self.assertEqual(len(result), 1)
        self.assertIn("x", self.wf.inputs)

    def test_branching_clears_redo_stack(self) -> None:
        self.wf.create_input("x")
        self.wf.undo()
        self.assertEqual(len(self.wf.redo_stack), 1)
        self.wf.create_input("y")
        self.assertEqual(len(self.wf.redo_stack), 0)

    def test_undo_limit_eviction(self) -> None:
        wf = workflow.Workflow("wf", undo_limit=3)
        for label in ("a", "b", "c", "d"):
            wf.create_input(label)
        self.assertEqual(len(wf.undo_stack), 3)
        # Oldest diff (create_input("a")) should have been evicted
        wf.undo(steps=3)
        # After undoing 3 steps, "a" should still be present (was never undoable)
        self.assertIn("a", wf.inputs)
        self.assertNotIn("b", wf.inputs)
        self.assertNotIn("c", wf.inputs)
        self.assertNotIn("d", wf.inputs)

    def test_long_form_add_undo_redo(self) -> None:
        labels = [f"port{i}" for i in range(5)]
        for lbl in labels:
            self.wf.create_input(lbl)
        self.assertEqual(len(self.wf.inputs), 5)

        self.wf.undo(steps=5)
        self.assertEqual(len(self.wf.inputs), 0)
        self.assertEqual(len(self.wf.undo_stack), 0)

        self.wf.redo(steps=5)
        self.assertEqual(len(self.wf.inputs), 5)
        for lbl in labels:
            self.assertIn(lbl, self.wf.inputs)

    def test_undo_returns_inverse_diffs(self) -> None:
        self.wf.create_input("x")
        undone = self.wf.undo()
        self.assertEqual(len(undone), 1)
        self.assertIsInstance(undone[0], list)
        self.assertIsInstance(undone[0][0], workflow.RemoveInput)

    def test_redo_returns_original_diffs(self) -> None:
        diff = self.wf.create_input("x")
        self.wf.undo()
        redone = self.wf.redo()
        self.assertEqual(len(redone), 1)
        self.assertEqual(redone[0], diff)


class TestAtomicRollback(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_rollback_on_foreign_node_in_batch(self) -> None:
        """If the second node in add_node fails, the first must be rolled back."""
        other = workflow.Workflow("other")
        n1 = _fixtures.atomic_add_node("n1")
        n2 = _fixtures.atomic_add_node("n2")
        other.add_node(n2)

        initial_undo_len = len(self.wf.undo_stack)
        initial_redo_len = len(self.wf.redo_stack)

        with self.assertRaises(ValueError):
            self.wf.add_node(n1, n2)

        # State unchanged
        self.assertNotIn("n1", self.wf.nodes)
        self.assertNotIn("n2", self.wf.nodes)
        # Stacks unchanged
        self.assertEqual(len(self.wf.undo_stack), initial_undo_len)
        self.assertEqual(len(self.wf.redo_stack), initial_redo_len)

    def test_rollback_cascade_with_edges(self) -> None:
        """
        Partially-applied edge removals during a failed cascade must be rolled back.

        remove_node("n1", "nonexistent") removes n1's edge and n1 itself in the
        first iteration, then raises KeyError on "nonexistent".  The rollback must
        restore both the node and the edge.
        """
        n1 = _fixtures.atomic_add_node("n1")
        self.wf.add_node(n1)
        edge = datatypes.EdgeTuple(
            frs.SourceHandle(node="n1", port="output_0"),
            frs.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)

        initial_nodes = set(self.wf.nodes)
        initial_edges = list(self.wf.edges)
        initial_undo_len = len(self.wf.undo_stack)

        with self.assertRaises(KeyError):
            self.wf.remove_node("n1", "nonexistent")

        self.assertEqual(set(self.wf.nodes), initial_nodes)
        self.assertEqual(self.wf.edges, initial_edges)
        self.assertEqual(len(self.wf.undo_stack), initial_undo_len)

    def test_rollback_leaves_redo_stack_unchanged(self) -> None:
        self.wf.create_input("x")
        self.wf.undo()
        redo_len_before = len(self.wf.redo_stack)

        other = workflow.Workflow("other")
        node = _fixtures.atomic_add_node()
        other.add_node(node)

        with self.assertRaises(ValueError):
            self.wf.add_node(node)

        self.assertEqual(len(self.wf.redo_stack), redo_len_before)

    def test_rollback_during_rollback_propagates(self) -> None:
        """If _dispatch raises during rollback, that exception propagates."""
        original_dispatch = self.wf._dispatch

        call_count = [0]

        def patched_dispatch(action: workflow.GraphAction) -> None:
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("dispatch failed during rollback")
            original_dispatch(action)

        # Trigger a scenario where two actions accumulate, then the method fails,
        # and during rollback _dispatch is patched to fail on the second call.
        # We'll manually trigger this by patching after setup.
        n1 = _fixtures.atomic_add_node("n1")
        n2 = _fixtures.atomic_add_node("n2")

        # Accumulate two forward actions manually
        acc: workflow.GraphDiff = []
        self.wf._diff_accumulator = acc
        self.wf._add_node(n1)
        self.wf._add_node(n2)
        self.wf._diff_accumulator = None

        # Now simulate a failed undoable that recorded those two actions,
        # then during rollback patched dispatch raises on second call.
        # We achieve this by re-running rollback directly.
        self.wf._dispatch = patched_dispatch  # type: ignore[method-assign]

        with self.assertRaises(RuntimeError):
            for action in reversed(acc):
                self.wf._dispatch(action.inverse())
        self.assertEqual(
            call_count[0],
            len(acc),
            msg="Should have been called at each action rolled back",
        )


class TestWorkflowSetattrSugar(unittest.TestCase):
    """`Workflow.__setattr__` node-assignment sugar."""

    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")

    def test_assigning_node_adds_it(self) -> None:
        node = _fixtures.atomic_add_node("original")
        self.wf.adder = node
        self.assertIs(self.wf.nodes["adder"], node)
        self.assertEqual(node.label, "adder")
        self.assertIs(node.owner, self.wf)

    def test_assignment_is_undoable(self) -> None:
        self.wf.adder = _fixtures.atomic_add_node()
        self.assertIn("adder", self.wf.nodes)
        self.wf.undo()
        self.assertNotIn("adder", self.wf.nodes)

    def test_collision_with_method_raises(self) -> None:
        with self.assertRaisesRegex(AttributeError, "collides with an existing"):
            self.wf.run = _fixtures.atomic_add_node()

    def test_collision_with_property_raises(self) -> None:
        with self.assertRaisesRegex(AttributeError, "collides with an existing"):
            self.wf.recipe = _fixtures.atomic_add_node()

    def test_collision_with_instance_attribute_raises(self) -> None:
        with self.assertRaisesRegex(AttributeError, "collides with an existing"):
            self.wf.executor = _fixtures.atomic_add_node()

    def test_duplicate_label_raises(self) -> None:
        self.wf.adder = _fixtures.atomic_add_node()
        with self.assertRaisesRegex(ValueError, "already has a node 'adder'"):
            self.wf.adder = _fixtures.atomic_add_node()

    def test_assigning_owned_node_raises(self) -> None:
        other = workflow.Workflow("other")
        node = _fixtures.atomic_add_node()
        other.adder = node
        with self.assertRaisesRegex(ValueError, "already has an owner"):
            self.wf.adder = node

    def test_non_node_value_assigned_normally(self) -> None:
        self.wf.executor = None
        self.assertIsNone(self.wf.executor)

    def test_non_node_public_attribute_assigned_normally(self) -> None:
        self.wf.some_flag = 42
        self.assertEqual(self.wf.some_flag, 42)

    def test_private_node_assigned_normally(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf._stash = node
        self.assertIs(self.wf._stash, node)
        self.assertNotIn("_stash", self.wf.nodes)

    def test_init_still_works(self) -> None:
        wf = workflow.Workflow("fresh", undo_limit=4)
        self.assertEqual(len(wf.nodes), 0)
        self.assertIsNone(wf.executor)
        self.assertIsNone(wf.current_run)
        self.assertEqual(wf.undo_limit, 4)

    def test_assigning_recipe_adds_macro(self) -> None:
        self.wf.m = _fixtures.macro.flowrep_recipe
        self.assertIsInstance(self.wf.nodes["m"], dag.Macro)

    def test_assigning_function_adds_atomic(self) -> None:
        self.wf.inc = plain_increment
        self.assertIsInstance(self.wf.nodes["inc"], atomic.Atomic)


class TestWorkflowAttributeSugar(unittest.TestCase):
    """`__getattr__` node-map fallback on `Workflow`."""

    @staticmethod
    def _populated_workflow() -> workflow.Workflow:
        wf = workflow.Workflow("wf")
        wf.add_node(
            _fixtures.atomic_add_node("executor"),
            _fixtures.atomic_add_node("nodes"),
            _fixtures.atomic_add_node("plain"),
        )
        return wf

    def test_sugar_returns_node(self) -> None:
        wf = self._populated_workflow()
        self.assertIs(wf.plain, wf.nodes["plain"])

    def test_real_attribute_shadows_node(self) -> None:
        wf = self._populated_workflow()
        # `executor` is a real instance attribute (set to None in __init__).
        self.assertIsNone(wf.executor)
        self.assertIsNot(wf.executor, wf.nodes["executor"])
        # `nodes` is a real property returning the node map itself.
        self.assertIsInstance(wf.nodes, workflow.MutableNodeMap)

    def test_underscore_label_excluded(self) -> None:
        wf = workflow.Workflow("wf")
        hidden = _fixtures.atomic_add_node("_hidden")
        wf.add_node(hidden)
        with self.assertRaises(AttributeError):
            _ = wf._hidden
        # The node is still reachable through the nodes map.
        self.assertIs(wf.nodes["_hidden"], hidden)

    def test_unknown_attribute_raises(self) -> None:
        wf = workflow.Workflow("wf")
        with self.assertRaises(AttributeError):
            _ = wf.does_not_exist


class TestWorkflowPickle(unittest.TestCase):
    """A Workflow must survive a pickle round-trip with children parented."""

    def test_round_trip_reparents_children(self) -> None:
        wf = workflow.Workflow("wf")
        wf.add_node(_fixtures.atomic_add_node("a"), _fixtures.atomic_sub_node("b"))
        restored = pickle.loads(pickle.dumps(wf))
        self.assertEqual(sorted(restored.nodes), ["a", "b"])
        for label, child in restored.nodes.items():
            self.assertIs(child.owner, restored, msg=f"{label} lost its owner")
            self.assertIsNone(
                child._detached_root, msg=f"{label} kept a stale detached root"
            )


if __name__ == "__main__":
    unittest.main()
