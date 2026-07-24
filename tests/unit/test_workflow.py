from __future__ import annotations

import collections
import contextlib
import dataclasses
import pickle
import unittest
from concurrent import futures

import flowrep as fr
import semantikon
from unit import _fixtures

from pyiron_workflow._wfms import (
    actions,
    atomic,
    constructors,
    dag,
    datatypes,
    execution,
    workflow,
)


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
            wf = workflow.Workflow("wf", explicit_limit)
            self.assertEqual(wf.undo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.redo_stack.maxlen, explicit_limit)
            self.assertEqual(wf.undo_limit, explicit_limit)

    def test_diff_accumulator_starts_none(self) -> None:
        wf = workflow.Workflow("wf")
        self.assertIsNone(wf._diff_accumulator)


class TestWorkflowUndoLimit(unittest.TestCase):
    def test_setter_updates_both_stacks(self) -> None:
        wf = workflow.Workflow("wf", 5)
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
            # Tuples are not JSONable, so this cannot be coerced to a Constant node
            self.wf.nodes.adder = (42,)

    def test_setattr_accepts_recipe(self) -> None:
        self.wf.nodes.m = _fixtures.macro.flowrep_recipe
        self.assertIsInstance(self.wf.nodes["m"], dag.Macro)


class TestIsNode(unittest.TestCase):
    def test_is_node_like_true_for_node(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.atomic_add_node()))

    def test_is_node_like_false_for_non_node(self) -> None:
        self.assertFalse(workflow.is_nodelike(42))
        self.assertFalse(workflow.is_nodelike("a string"))
        self.assertFalse(workflow.is_nodelike(None))

    def test_is_node_like_true_for_recipe(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.add.flowrep_recipe))

    def test_is_node_like_true_for_function(self) -> None:
        self.assertTrue(workflow.is_nodelike(_fixtures.add))
        self.assertTrue(workflow.is_nodelike(_fixtures.plain_increment))


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
        a = actions.AddNode(node)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_node_inverse_symmetric(self) -> None:
        node = self._fresh_node()
        a = actions.RemoveNode(node)
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_edge_inverse_symmetric(self) -> None:
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="n", port="p"),
        )
        a = actions.AddEdge(edge)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_edge_inverse_symmetric(self) -> None:
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="n", port="p"),
        )
        a = actions.RemoveEdge(edge)
        self.assertEqual(a.inverse().inverse(), a)

    def test_rename_node_inverse_symmetric(self) -> None:
        node = self._fresh_node()
        a = actions.RenameNode(node, "old", "new")
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_input_inverse_symmetric(self) -> None:
        port = self._fresh_input()
        a = actions.AddInput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_input_inverse_symmetric(self) -> None:
        port = self._fresh_input()
        a = actions.RemoveInput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_add_output_inverse_symmetric(self) -> None:
        port = self._fresh_output()
        a = actions.AddOutput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_remove_output_inverse_symmetric(self) -> None:
        port = self._fresh_output()
        a = actions.RemoveOutput(port)
        self.assertEqual(a.inverse().inverse(), a)

    def test_replace_port_inverse_symmetric(self) -> None:
        p1 = self._fresh_input("x")
        p2 = self._fresh_input("y")
        a = actions.ReplacePort(p1, p2)
        self.assertEqual(a.inverse().inverse(), a)

    def test_adding_circular_node_raises(self):
        sub_wf = workflow.Workflow("sub_wf")
        sub_wf.my_own_grandpa = self.wf
        with self.assertRaisesRegex(ValueError, "contains a cycle"):
            self.wf.add_node(sub_wf)

    # _dispatch correctness

    def test_dispatch_add_remove_node(self) -> None:
        node = self._fresh_node()
        self.wf._dispatch(actions.AddNode(node))
        self.assertIn("n", self.wf.nodes)
        self.wf._dispatch(actions.RemoveNode(node))
        self.assertNotIn("n", self.wf.nodes)

    def test_dispatch_add_remove_edge(self) -> None:
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"), fr.schemas.OutputTarget(port="y")
        )
        self.wf._dispatch(actions.AddEdge(edge))
        self.assertIn(edge, self.wf.edges)
        self.wf._dispatch(actions.RemoveEdge(edge))
        self.assertNotIn(edge, self.wf.edges)

    def test_dispatch_add_remove_input(self) -> None:
        port = self._fresh_input()
        self.wf._dispatch(actions.AddInput(port))
        self.assertIn("x", self.wf.inputs)
        self.wf._dispatch(actions.RemoveInput(port))
        self.assertNotIn("x", self.wf.inputs)

    def test_dispatch_add_remove_output(self) -> None:
        port = self._fresh_output()
        self.wf._dispatch(actions.AddOutput(port))
        self.assertIn("y", self.wf.outputs)
        self.wf._dispatch(actions.RemoveOutput(port))
        self.assertNotIn("y", self.wf.outputs)

    def test_dispatch_replace_port(self) -> None:
        # Use create_input so the port is created with the right module's InputPort.
        self.wf.create_input("x")
        p1 = self.wf.inputs["x"]
        # Build replacement by copying the owned port (preserves the right class).
        p2 = dataclasses.replace(p1, label="z", type_hint=int)
        self.wf._dispatch(actions.ReplacePort(p1, p2))
        self.assertNotIn("x", self.wf.inputs)
        self.assertIn("z", self.wf.inputs)
        self.assertIs(self.wf.inputs["z"], p2)

    def test_dispatch_rename_node(self) -> None:
        node = self._fresh_node("old")
        self.wf._dispatch(actions.AddNode(node))
        self.wf._dispatch(actions.RenameNode(node, "old", "new"))
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
        acc: actions.GraphDiff = []
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
        self.assertIsInstance(action, actions.AddInput)

    def test_returns_action(self) -> None:
        port = datatypes.InputPort(
            label="x", owner=self.wf, type_hint=None, type_metadata=None
        )
        action = self.wf._add_input(port)
        self.assertIsInstance(action, actions.AddInput)
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
        self.assertIsInstance(diff[0], actions.AddInput)

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
        outer_acc: actions.GraphDiff = []
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
            self.assertIsInstance(outer_acc[0], actions.AddInput)
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
        self.assertIsInstance(diff[0], actions.AddNode)
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
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.assertEqual(len(self.wf.edges), 1)
        self.wf.remove_node("adder")
        self.assertEqual(len(self.wf.edges), 0)

    def test_remove_node_diff_includes_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_node("adder")
        action_types = [type(a) for a in diff]
        self.assertIn(actions.RemoveEdge, action_types)
        self.assertIn(actions.RemoveNode, action_types)

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
            fr.schemas.SourceHandle(node="old", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_node("old", "new")
        self.assertEqual(len(self.wf.edges), 1)
        src = self.wf.edges[0].source
        self.assertIsInstance(src, fr.schemas.SourceHandle)
        self.assertEqual(src.node, "new")  # type: ignore[union-attr]

    def test_rename_node_undo(self) -> None:
        node = _fixtures.atomic_add_node("old")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="old", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
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
            fr.schemas.SourceHandle(node="n1", port="output_0"),
            fr.schemas.TargetHandle(node="old", port="x"),
        )
        self.wf.add_edge(peer_edge)
        self.wf.rename_node("old", "new")
        self.assertEqual(len(self.wf.edges), 1)
        tgt = self.wf.edges[0].target
        self.assertIsInstance(tgt, fr.schemas.TargetHandle)
        self.assertEqual(tgt.node, "new")  # type: ignore[union-attr]


class TestEdgeMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.wf.create_input("x")
        self.wf.create_output("y")
        self.edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"), fr.schemas.OutputTarget(port="y")
        )

    def test_add_edge_state(self) -> None:
        self.wf.add_edge(self.edge)
        self.assertIn(self.edge, self.wf.edges)

    def test_add_edge_diff(self) -> None:
        diff = self.wf.add_edge(self.edge)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], actions.AddEdge)

    def test_remove_edge_state(self) -> None:
        self.wf.add_edge(self.edge)
        self.wf.remove_edge(self.edge)
        self.assertNotIn(self.edge, self.wf.edges)

    def test_remove_edge_diff(self) -> None:
        self.wf.add_edge(self.edge)
        diff = self.wf.remove_edge(self.edge)
        self.assertEqual(len(diff), 1)
        self.assertIsInstance(diff[0], actions.RemoveEdge)

    def test_remove_edge_undo_regression(self) -> None:
        self.wf.add_edge(self.edge)
        self.wf.remove_edge(self.edge)
        self.assertNotIn(self.edge, self.wf.edges)
        self.wf.undo()
        self.assertIn(self.edge, self.wf.edges)

    def test_disconnect_removes_node_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_input("z")
        self.wf.create_output("out")
        self.wf.create_output("w")
        e1 = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        e2 = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        unrelated = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="z"), fr.schemas.OutputTarget(port="w")
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
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.disconnect("adder")
        self.wf.undo()
        self.assertIn(edge, self.wf.edges)

    def test_connect(self):
        self.wf.create_input("x")
        self.wf.create_input("a")
        self.wf.create_output("y")
        self.wf.first = _fixtures.atomic_add_node("first")
        self.wf.second = _fixtures.atomic_add_node("first")
        self.wf.connect(self.wf.inputs.x, self.wf.first.inputs.x)
        self.wf.connect(self.wf.inputs.a, self.wf.first.inputs.y)
        self.wf.connect(self.wf.inputs.a, self.wf.second.inputs.y)
        self.wf.connect(self.wf.first.outputs.output_0, self.wf.second.inputs.x)
        self.wf.connect(self.wf.second, self.wf.outputs.y)
        self.assertEqual(42 + 2, self.wf.run(x=42, a=1).outputs.y)
        self.wf.undo(5)  # n connect calls
        self.assertEqual([], self.wf.edges)


class TestAddEdgeValidation(unittest.TestCase):
    """`Workflow.add_edge` wraps `validation.validate_edge` by default and
    accepts a `type_validate=False` kwarg to skip the check."""

    def _sibling_wf(self, src_factory, tgt_factory):
        return _fixtures.build_workflow(
            node_specs={"src": src_factory, "tgt": tgt_factory},
            label="wf",
        )

    def _sibling_edge(self) -> datatypes.EdgeTuple:
        return datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="src", port="output_0"),
            fr.schemas.TargetHandle(node="tgt", port="x"),
        )

    def test_validates_by_default_passes_compatible(self) -> None:
        wf = self._sibling_wf(_fixtures.typed_int_node, _fixtures.typed_int_node)
        edge = self._sibling_edge()
        wf.add_edge(edge)
        self.assertIn(edge, wf.edges)

    def test_validates_by_default_rejects_type_hint_mismatch(self) -> None:
        wf = self._sibling_wf(_fixtures.typed_float_node, _fixtures.typed_int_node)
        edge = self._sibling_edge()
        with self.assertRaises(TypeError):
            wf.add_edge(edge)
        self.assertNotIn(edge, wf.edges)

    def test_validates_by_default_rejects_unknown_port(self) -> None:
        wf = workflow.Workflow("wf")
        bad = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="nope"), fr.schemas.OutputTarget(port="nada")
        )
        with self.assertRaises(KeyError):
            wf.add_edge(bad)
        self.assertEqual(wf.edges, [])

    def test_skip_validation_allows_type_hint_mismatch(self) -> None:
        wf = self._sibling_wf(_fixtures.typed_float_node, _fixtures.typed_int_node)
        edge = self._sibling_edge()
        wf.add_edge(edge, type_validate=False)
        self.assertIn(edge, wf.edges)

    def test_skip_validation_allows_unknown_port(self) -> None:
        wf = workflow.Workflow("wf")
        bad = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="nope"), fr.schemas.OutputTarget(port="nada")
        )
        wf.add_edge(bad, type_validate=False)
        self.assertIn(bad, wf.edges)

    def test_failed_validation_does_not_grow_undo_stack(self) -> None:
        wf = self._sibling_wf(_fixtures.typed_float_node, _fixtures.typed_int_node)
        edge = self._sibling_edge()
        initial_undo_len = len(wf.undo_stack)
        with self.assertRaises(TypeError):
            wf.add_edge(edge)
        self.assertEqual(len(wf.undo_stack), initial_undo_len)

    def _unfulfilled_edge(self) -> datatypes.EdgeTuple:
        # src=add (output_0 unhinted) -> tgt=typed_int (x hinted int)
        return datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="src", port="output_0"),
            fr.schemas.TargetHandle(node="tgt", port="x"),
        )

    def test_strict_rejects_unfulfilled_request(self) -> None:
        wf = self._sibling_wf(_fixtures.atomic_add_node, _fixtures.typed_int_node)
        edge = self._unfulfilled_edge()
        with self.assertRaises(TypeError):
            wf.add_edge(edge, strict=True)
        self.assertNotIn(edge, wf.edges)

    def test_default_allows_unfulfilled_request(self) -> None:
        wf = self._sibling_wf(_fixtures.atomic_add_node, _fixtures.typed_int_node)
        edge = self._unfulfilled_edge()
        wf.add_edge(edge)
        self.assertIn(edge, wf.edges)

    def test_multi_edge_call_rolls_back_when_later_edge_fails(self) -> None:
        """If a later edge in a single `add_edge` call fails validation, earlier
        edges from the same call must not remain in the graph."""
        wf = _fixtures.build_workflow(
            node_specs={
                "src": _fixtures.typed_int_node,
                "tgt_ok": _fixtures.typed_int_node,
                "tgt_bad": _fixtures.typed_float_node,
            },
            label="wf",
        )
        ok = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="src", port="output_0"),
            fr.schemas.TargetHandle(node="tgt_ok", port="x"),
        )
        bad = datatypes.EdgeTuple(  # int source → float target → fails
            fr.schemas.SourceHandle(node="src", port="output_0"),
            fr.schemas.TargetHandle(node="tgt_bad", port="x"),
        )
        with self.assertRaises(TypeError):
            wf.add_edge(ok, bad)
        self.assertNotIn(ok, wf.edges)
        self.assertNotIn(bad, wf.edges)


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
        self.assertIsInstance(diff[0], actions.AddInput)

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
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        self.wf.add_edge(edge)
        self.wf.remove_input("x")
        self.assertNotIn(edge, self.wf.edges)

    def test_remove_input_diff_includes_edges(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_input("x")
        types = [type(a) for a in diff]
        self.assertIn(actions.RemoveEdge, types)
        self.assertIn(actions.RemoveInput, types)

    def test_remove_input_undo(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
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
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_input("x", "z")
        self.assertEqual(len(self.wf.edges), 1)
        src = self.wf.edges[0].source
        self.assertIsInstance(src, fr.schemas.InputSource)
        self.assertEqual(src.port, "z")  # type: ignore[union-attr]

    def test_rename_input_undo(self) -> None:
        self.wf.create_input("x")
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
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
        self.assertIsInstance(diff[0], actions.AddOutput)

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
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.remove_output("out")
        self.assertNotIn(edge, self.wf.edges)

    def test_remove_output_diff_includes_edges(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        diff = self.wf.remove_output("out")
        types = [type(a) for a in diff]
        self.assertIn(actions.RemoveEdge, types)
        self.assertIn(actions.RemoveOutput, types)

    def test_remove_output_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
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
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
        )
        self.wf.add_edge(edge)
        self.wf.rename_output("out", "result")
        self.assertEqual(len(self.wf.edges), 1)
        tgt = self.wf.edges[0].target
        self.assertIsInstance(tgt, fr.schemas.OutputTarget)
        self.assertEqual(tgt.port, "result")  # type: ignore[union-attr]

    def test_rename_output_undo(self) -> None:
        node = _fixtures.atomic_add_node("adder")
        self.wf.add_node(node)
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
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


class TestCreateInputFor(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.wf.add_node(_fixtures.atomic_add_node("adder"))

    def test_creates_input_port(self) -> None:
        self.wf.create_input_for(self.wf.adder.inputs.x)
        self.assertIn("x", self.wf.inputs)

    def test_creates_multiple_input_ports(self) -> None:
        self.wf.create_input_for(
            self.wf.adder.inputs.x,
            self.wf.adder.inputs.y,
            label="pan",
        )
        self.assertIn("pan", self.wf.inputs)
        self.assertEqual(len(self.wf.inputs), 1)
        self.assertEqual(len(self.wf.edges), 2)

    def test_wires_edge_to_destination(self) -> None:
        self.wf.create_input_for(self.wf.adder.inputs.x)
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        self.assertIn(edge, self.wf.edges)

    def test_label_defaults_to_destination_label(self) -> None:
        self.wf.create_input_for(self.wf.adder.inputs.y)
        self.assertIn("y", self.wf.inputs)

    def test_custom_label(self) -> None:
        self.wf.create_input_for(self.wf.adder.inputs.x, label="custom")
        self.assertIn("custom", self.wf.inputs)
        edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="custom"),
            fr.schemas.TargetHandle(node="adder", port="x"),
        )
        self.assertIn(edge, self.wf.edges)

    def test_propagates_type_hint_and_metadata(self) -> None:
        self.wf.add_node(_fixtures.typed_int_node("ti"))
        destination = self.wf.ti.inputs.x
        self.wf.create_input_for(destination)
        self.assertEqual(self.wf.inputs["x"].type_hint, destination.type_hint)
        self.assertEqual(self.wf.inputs["x"].type_metadata, destination.type_metadata)

    def test_diff_includes_input_and_edge(self) -> None:
        diff = self.wf.create_input_for(self.wf.adder.inputs.x)
        types = [type(a) for a in diff]
        self.assertIn(actions.AddInput, types)
        self.assertIn(actions.AddEdge, types)

    def test_undo_removes_input_and_edge(self) -> None:
        self.wf.create_input_for(self.wf.adder.inputs.x)
        self.wf.undo()
        self.assertNotIn("x", self.wf.inputs)
        self.assertEqual([], self.wf.edges)

    def test_own_port_raises(self) -> None:
        self.wf.create_output("y")
        with self.assertRaises(ValueError):
            self.wf.create_input_for(self.wf.outputs.y)

    def test_foreign_child_port_raises(self) -> None:
        other = workflow.Workflow("other")
        other.add_node(_fixtures.atomic_add_node("adder"))
        with self.assertRaises(ValueError):
            self.wf.create_input_for(other.adder.inputs.x)

    def test_foreign_port_leaves_workflow_unchanged(self) -> None:
        other = workflow.Workflow("other")
        other.add_node(_fixtures.atomic_add_node("adder"))
        with contextlib.suppress(ValueError):
            self.wf.create_input_for(other.adder.inputs.x)
        self.assertEqual([], list(self.wf.inputs))
        self.assertEqual([], self.wf.edges)


class TestCreateOutputFrom(unittest.TestCase):
    def setUp(self) -> None:
        self.wf = workflow.Workflow("wf")
        self.wf.add_node(_fixtures.atomic_add_node("adder"))

    def test_creates_output_port_from_port(self) -> None:
        self.wf.create_output_from(self.wf.adder.outputs.output_0)
        self.assertIn("output_0", self.wf.outputs)

    def test_wires_edge_from_source(self) -> None:
        self.wf.create_output_from(self.wf.adder.outputs.output_0)
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="output_0"),
        )
        self.assertIn(edge, self.wf.edges)

    def test_accepts_node_with_single_output(self) -> None:
        self.wf.create_output_from(self.wf.adder, label="result")
        self.assertIn("result", self.wf.outputs)
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="adder", port="output_0"),
            fr.schemas.OutputTarget(port="result"),
        )
        self.assertIn(edge, self.wf.edges)

    def test_custom_label(self) -> None:
        self.wf.create_output_from(self.wf.adder.outputs.output_0, label="result")
        self.assertIn("result", self.wf.outputs)

    def test_propagates_type_hint_and_metadata(self) -> None:
        self.wf.add_node(_fixtures.typed_int_node("ti"))
        source = self.wf.ti.outputs.output_0
        self.wf.create_output_from(source)
        self.assertEqual(self.wf.outputs["output_0"].type_hint, source.type_hint)
        self.assertEqual(
            self.wf.outputs["output_0"].type_metadata, source.type_metadata
        )

    def test_diff_includes_output_and_edge(self) -> None:
        diff = self.wf.create_output_from(self.wf.adder.outputs.output_0)
        types = [type(a) for a in diff]
        self.assertIn(actions.AddOutput, types)
        self.assertIn(actions.AddEdge, types)

    def test_undo_removes_output_and_edge(self) -> None:
        self.wf.create_output_from(self.wf.adder.outputs.output_0)
        self.wf.undo()
        self.assertNotIn("output_0", self.wf.outputs)
        self.assertEqual([], self.wf.edges)

    def test_multi_output_node_raises(self) -> None:
        self.wf.add_node(_fixtures.macro_node("multi"))
        with self.assertRaises(ValueError):
            self.wf.create_output_from(self.wf.multi)

    def test_own_port_raises(self) -> None:
        self.wf.create_input("x")
        with self.assertRaises(ValueError):
            self.wf.create_output_from(self.wf.inputs.x)

    def test_foreign_child_source_raises(self) -> None:
        other = workflow.Workflow("other")
        other.add_node(_fixtures.atomic_add_node("adder"))
        with self.assertRaises(ValueError):
            self.wf.create_output_from(other.adder.outputs.output_0)


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
        wf = workflow.Workflow("wf", 3)
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
        self.assertIsInstance(undone[0][0], actions.RemoveInput)

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
        self.wf.create_output("out")
        edge = datatypes.EdgeTuple(
            fr.schemas.SourceHandle(node="n1", port="output_0"),
            fr.schemas.OutputTarget(port="out"),
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

        def patched_dispatch(action: actions.GraphAction) -> None:
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
        acc: actions.GraphDiff = []
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

    def test_existing_node_to_new_attr_relabels(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.adder = node
        self.wf.new_name = node
        self.assertIn("new_name", self.wf.nodes)
        self.assertNotIn("adder", self.wf.nodes)
        self.assertEqual("new_name", node.label)

    def test_reassinging_existing_has_no_impact(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.adder = node
        self.wf.adder = node
        self.assertIn("adder", self.wf.nodes)
        self.assertEqual(1, len(self.wf.nodes))

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
        wf = workflow.Workflow("fresh", 4)
        self.assertEqual(len(wf.nodes), 0)
        self.assertIsNone(wf.executor)
        self.assertIsNone(wf.last_run)
        self.assertEqual(wf.undo_limit, 4)

    def test_assigning_recipe_adds_macro(self) -> None:
        self.wf.m = _fixtures.macro.flowrep_recipe
        self.assertIsInstance(self.wf.nodes["m"], dag.Macro)

    def test_assigning_function_adds_atomic(self) -> None:
        self.wf.inc = _fixtures.plain_increment
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


class TestWorkflowEvaluate(unittest.TestCase):
    """
    End-to-end evaluation tests for `Workflow`, covering every edge-type scenario.
    """

    def test_empty_workflow_finishes(self) -> None:
        wf = _fixtures.build_workflow()
        run = wf.run()
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(len(run.steps), 0)
        self.assertEqual(len(run.outputs), 0)

    def test_node_with_defaults_no_edges(self) -> None:
        wf = _fixtures.build_workflow(
            node_specs={"m": _fixtures.multiply_with_defaults_node}
        )
        run = wf.run()
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(len(run.steps), 1)

    def test_unconnected_workflow_input_does_not_interfere(self) -> None:
        # Workflow has an input port that is not wired to any child.
        # The child should still evaluate using its own defaults (1*2=2).
        wf = _fixtures.build_workflow(
            inputs=["unused"],
            node_specs={"m": _fixtures.multiply_with_defaults_node},
        )
        run = wf.run()
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.steps[0].result.output_ports["output_0"].value, 2)

    def test_no_output_edges_run_finishes_with_empty_outputs(self) -> None:
        wf = _fixtures.build_workflow(
            node_specs={"m": _fixtures.multiply_with_defaults_node}
        )
        run = wf.run()
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(len(run.outputs), 0)

    def test_input_edge_feeds_child_node(self) -> None:
        # Workflow input "x" wired to multiply_with_defaults.x; y uses default (2).
        wf = _fixtures.build_workflow(
            inputs=["x"],
            node_specs={"m": _fixtures.multiply_with_defaults_node},
            edges=[
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="x"),
                    fr.schemas.TargetHandle(node="m", port="x"),
                )
            ],
        )
        run = wf.run(x=5)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        # x=5, y=2 (default) → 5*2=10
        self.assertEqual(run.steps[0].result.output_ports["output_0"].value, 10)

    def test_output_edge_captures_result(self) -> None:
        wf = _fixtures.build_workflow(
            outputs=["out"],
            node_specs={"m": _fixtures.multiply_with_defaults_node},
            edges=[
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="m", port="output_0"),
                    fr.schemas.OutputTarget(port="out"),
                )
            ],
        )
        run = wf.run()
        self.assertEqual(run.outputs.out, 2)  # 1*2=2

    def test_sibling_edge_sequences_nodes(self) -> None:
        # n1 uses defaults (1*2=2); n2 receives x=n1.output_0=2, uses default y=2 → 4.
        wf = _fixtures.build_workflow(
            outputs=["result"],
            node_specs={
                "n1": _fixtures.multiply_with_defaults_node,
                "n2": _fixtures.multiply_with_defaults_node,
            },
            edges=[
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="n1", port="output_0"),
                    fr.schemas.TargetHandle(node="n2", port="x"),
                ),
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="n2", port="output_0"),
                    fr.schemas.OutputTarget(port="result"),
                ),
            ],
        )
        run = wf.run()
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.result, 4)

    def test_full_wiring_matches_expected_values(self) -> None:
        wf = _fixtures.build_workflow(
            inputs=["x", "y", "z"],
            outputs=["a", "s"],
            node_specs={
                "add_0": _fixtures.atomic_add_node,
                "sub_0": _fixtures.atomic_sub_node,
            },
            edges=_fixtures._MACRO_WF_EDGES,
        )
        run = wf.run(x=1, y=2, z=3)
        self.assertEqual(run.outputs.a, 3)  # 1+2
        self.assertEqual(run.outputs.s, 0)  # 3-3

    def test_passthrough_input_to_output(self) -> None:
        # No child nodes: InputSource edge wired directly to OutputTarget.
        wf = _fixtures.build_workflow(
            inputs=["x"],
            outputs=["out"],
            edges=[
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="x"),
                    fr.schemas.OutputTarget(port="out"),
                )
            ],
        )
        run = wf.run(x=42)
        self.assertEqual(run.outputs.out, 42)

    def test_steps_and_status(self) -> None:
        wf = _fixtures.build_workflow(
            inputs=["x", "y", "z"],
            outputs=["a", "s"],
            node_specs={
                "add_0": _fixtures.atomic_add_node,
                "sub_0": _fixtures.atomic_sub_node,
            },
            edges=_fixtures._MACRO_WF_EDGES,
        )
        run = wf.run(x=1, y=2, z=3)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(len(run.steps), 2)
        self.assertEqual({step.label for step in run.steps}, {"add_0", "sub_0"})

    def test_parsed_workflow_with_constant(self) -> None:
        # `uses_constant` parses to a `Constant` node feeding `add`'s second
        # addend; the value flows through the live workflow.
        wf = constructors.macro2workflow(_fixtures.uses_constant_node())
        run = wf.run(x=10)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        self.assertEqual(run.outputs.y, 15)  # 10 + 5


class TestUnlockSubgraph(unittest.TestCase):
    def test_passing_atomic_raises_typeerror(self) -> None:
        parent = _fixtures.build_workflow(label="parent")
        parent.add_node(_fixtures.atomic_add_node("a"))
        with self.assertRaises(TypeError) as ctx:
            parent.unlock_subgraph("a")
        self.assertIn("a", str(ctx.exception))
        self.assertIn(dag.Macro.__name__, str(ctx.exception))

    def _parent_with_macro_child(self, child_label: str = "m") -> workflow.Workflow:
        parent = workflow.Workflow("parent")
        parent.create_input("x", "y", "z")
        parent.create_output("a", "s")
        parent.add_node(_fixtures.macro_node(child_label))
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node=child_label, port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node=child_label, port="y"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="z"),
                fr.schemas.TargetHandle(node=child_label, port="z"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node=child_label, port="a"),
                fr.schemas.OutputTarget(port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node=child_label, port="s"),
                fr.schemas.OutputTarget(port="s"),
            ),
        )
        return parent

    def test_replaces_macro_with_workflow(self) -> None:
        parent = self._parent_with_macro_child()
        edges_before = set(parent.edges)
        parent.unlock_subgraph("m")
        # Use class name to avoid dual-import-root isinstance failure
        self.assertEqual(type(parent.nodes["m"]).__name__, workflow.Workflow.__name__)
        self.assertEqual(set(parent.edges), edges_before)

    def test_passing_workflow_is_noop(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(workflow.Workflow("inner"))
        # Use class name to avoid dual-import-root isinstance failure
        self.assertEqual(
            type(parent.nodes["inner"]).__name__, workflow.Workflow.__name__
        )
        # _undoable always pushes a diff (even empty) so the stack grows by at
        # most 1; the diff itself should be empty (no graph mutations occurred).
        undo_len_before = len(parent.undo_stack)
        parent.unlock_subgraph("inner")
        self.assertLessEqual(len(parent.undo_stack), undo_len_before + 1)
        # The most recent entry must be an empty diff (no actual graph changes)
        if len(parent.undo_stack) > undo_len_before:
            self.assertEqual(parent.undo_stack[-1], [])

    def test_unknown_label_raises_keyerror(self) -> None:
        parent = workflow.Workflow("parent")
        with self.assertRaises(KeyError):
            parent.unlock_subgraph("not_there")

    def test_undo_restores_macro(self) -> None:
        parent = self._parent_with_macro_child()
        original_child = parent.nodes["m"]
        edges_before = set(parent.edges)
        parent.unlock_subgraph("m")
        parent.undo()
        self.assertIs(parent.nodes["m"], original_child)
        self.assertEqual(set(parent.edges), edges_before)

    def test_redo_replays(self) -> None:
        parent = self._parent_with_macro_child()
        parent.unlock_subgraph("m")
        unlocked = parent.nodes["m"]
        parent.undo()
        parent.redo()
        self.assertIs(parent.nodes["m"], unlocked)

    def test_run_after_unlock_matches_pre_unlock(self) -> None:
        baseline = self._parent_with_macro_child()
        baseline_run = baseline.run(x=1, y=2, z=4)
        candidate = self._parent_with_macro_child()
        candidate.unlock_subgraph("m")
        candidate_run = candidate.run(x=1, y=2, z=4)
        self.assertEqual(
            baseline_run.result.output_ports["a"].value,
            candidate_run.result.output_ports["a"].value,
        )
        self.assertEqual(
            baseline_run.result.output_ports["s"].value,
            candidate_run.result.output_ports["s"].value,
        )


class TestLockSubgraph(unittest.TestCase):
    def _parent_with_workflow_child(self) -> workflow.Workflow:
        parent = workflow.Workflow("parent")
        parent.create_input("x")
        parent.create_input("y")
        parent.create_input("z")
        parent.create_output("a")
        parent.create_output("s")
        inner = constructors.macro2workflow(_fixtures.macro_node("m"))
        parent.add_node(inner)
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="m", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="m", port="y"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="z"),
                fr.schemas.TargetHandle(node="m", port="z"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="m", port="a"),
                fr.schemas.OutputTarget(port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="m", port="s"),
                fr.schemas.OutputTarget(port="s"),
            ),
        )
        return parent

    def test_replaces_workflow_with_macro(self) -> None:
        parent = self._parent_with_workflow_child()
        edges_before = set(parent.edges)
        parent.lock_subgraph("m")
        self.assertIsInstance(parent.nodes["m"], dag.Macro)
        self.assertEqual(set(parent.edges), edges_before)

    def test_passing_macro_is_noop(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(_fixtures.macro_node("m"))
        undo_len_before = len(parent.undo_stack)
        parent.lock_subgraph("m")
        self.assertIsInstance(parent.nodes["m"], dag.Macro)
        # `@_undoable` always pushes a diff even for early-return; allow that
        # but require it to be empty.
        grew_by = len(parent.undo_stack) - undo_len_before
        self.assertLessEqual(grew_by, 1)
        if grew_by == 1:
            self.assertEqual(parent.undo_stack[-1], [])

    def test_passing_atomic_raises_typeerror(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(_fixtures.atomic_add_node("a"))
        with self.assertRaises(TypeError) as ctx:
            parent.lock_subgraph("a")
        self.assertIn("a", str(ctx.exception))
        self.assertIn(workflow.Workflow.__name__, str(ctx.exception))

    def test_unknown_label_raises_keyerror(self) -> None:
        parent = workflow.Workflow("parent")
        with self.assertRaises(KeyError):
            parent.lock_subgraph("not_there")

    def test_undo_restores_workflow(self) -> None:
        parent = self._parent_with_workflow_child()
        original_child = parent.nodes["m"]
        edges_before = set(parent.edges)
        parent.lock_subgraph("m")
        parent.undo()
        self.assertIs(parent.nodes["m"], original_child)
        self.assertEqual(set(parent.edges), edges_before)

    def test_redo_replays(self) -> None:
        parent = self._parent_with_workflow_child()
        parent.lock_subgraph("m")
        locked = parent.nodes["m"]
        parent.undo()
        parent.redo()
        self.assertIs(parent.nodes["m"], locked)

    def test_lock_then_unlock_round_trips_hints(self) -> None:
        parent = self._parent_with_workflow_child()
        inner = parent.nodes["m"]
        inner.add_port_hint(inner.inputs["x"], int)
        inner.add_port_hint(inner.outputs["a"], float)
        parent.lock_subgraph("m")
        parent.unlock_subgraph("m")
        round_tripped = parent.nodes["m"]
        self.assertIsInstance(round_tripped, workflow.Workflow)
        self.assertEqual(round_tripped.inputs["x"].type_hint, int)
        self.assertEqual(round_tripped.outputs["a"].type_hint, float)

    def test_run_after_lock_matches_pre_lock(self) -> None:
        baseline = self._parent_with_workflow_child()
        baseline_run = baseline.run(x=1, y=2, z=4)
        candidate = self._parent_with_workflow_child()
        candidate.lock_subgraph("m")
        candidate_run = candidate.run(x=1, y=2, z=4)
        self.assertEqual(
            baseline_run.result.output_ports["a"].value,
            candidate_run.result.output_ports["a"].value,
        )
        self.assertEqual(
            baseline_run.result.output_ports["s"].value,
            candidate_run.result.output_ports["s"].value,
        )


class TestGroup(unittest.TestCase):
    def test_groups_two_nodes_shape(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])

        self.assertEqual(set(parent.nodes.keys()), {"grp", "mul_0"})
        grp = parent.nodes["grp"]
        # Use class-name check to avoid dual-import-root isinstance failure
        self.assertEqual(type(grp).__name__, workflow.Workflow.__name__)
        self.assertEqual(set(grp.nodes.keys()), {"add_0", "sub_0"})

        # The internal `add_0 -> sub_0` edge moved into the subgraph.
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="sub_0", port="x"),
            ),
            grp.edges,
        )

        # Parent input edges now land on the subgraph's generated ports.
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="grp", port="add_0__x"),
            ),
            parent.edges,
        )
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="z"),
                fr.schemas.TargetHandle(node="grp", port="sub_0__y"),
            ),
            parent.edges,
        )
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="grp", port="sub_0__output_0"),
                fr.schemas.OutputTarget(port="diff"),
            ),
            parent.edges,
        )

        # Generated port labels match the convention.
        self.assertEqual(set(grp.inputs.keys()), {"add_0__x", "add_0__y", "sub_0__y"})
        self.assertEqual(set(grp.outputs.keys()), {"sub_0__output_0"})

    def test_groups_one_node(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        parent.group("grp", parent.nodes["mul_0"])
        self.assertIn("grp", parent.nodes)
        grp = parent.nodes["grp"]
        # Use class-name check to avoid dual-import-root isinstance failure
        self.assertEqual(type(grp).__name__, workflow.Workflow.__name__)
        self.assertEqual(set(grp.nodes.keys()), {"mul_0"})

    def test_label_collision_raises(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        with self.assertRaises(ValueError):
            parent.group("add_0", parent.nodes["sub_0"])

    def test_non_child_raises(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        stranger = _fixtures.atomic_add_node("stranger")
        with self.assertRaises(KeyError):
            parent.group("grp", stranger)

    def test_empty_node_list_raises(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        with self.assertRaises(ValueError):
            parent.group("grp")

    def test_unrelated_edges_untouched(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="mul_0", port="x"),
            )
        )
        unrelated_edge = datatypes.EdgeTuple(
            fr.schemas.InputSource(port="x"),
            fr.schemas.TargetHandle(node="mul_0", port="x"),
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        self.assertIn(unrelated_edge, parent.edges)

    def test_undo_restores_pre_group_state(self) -> None:
        parent = _fixtures.grouping_wf("parent")
        nodes_before = dict(parent.nodes._pwf_lexical_map__data)
        edges_before = set(parent.edges)
        inputs_before = dict(parent.inputs._pwf_lexical_map__data)
        outputs_before = dict(parent.outputs._pwf_lexical_map__data)

        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        parent.undo()

        self.assertEqual(dict(parent.nodes._pwf_lexical_map__data), nodes_before)
        self.assertEqual(set(parent.edges), edges_before)
        self.assertEqual(dict(parent.inputs._pwf_lexical_map__data), inputs_before)
        self.assertEqual(dict(parent.outputs._pwf_lexical_map__data), outputs_before)

    def test_run_after_group_matches_pre_group(self) -> None:
        baseline = _fixtures.grouping_wf("parent")
        baseline_run = baseline.run(x=2, y=3, z=4)

        candidate = _fixtures.grouping_wf("parent")
        candidate.group("grp", candidate.nodes["add_0"], candidate.nodes["sub_0"])
        candidate_run = candidate.run(x=2, y=3, z=4)

        self.assertEqual(
            baseline_run.result.output_ports["diff"].value,
            candidate_run.result.output_ports["diff"].value,
        )

    def test_dedups_multiple_inbound_edges_to_same_target(self) -> None:
        """Two cross-boundary edges hitting the same (child, port) collapse
        to a single subgraph input port."""
        parent = _fixtures.grouping_wf("parent")
        # `add_0/x` already receives `InputSource("x")`; add a second feeder.
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="add_0", port="x"),
            )
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        grp = parent.nodes["grp"]
        self.assertIn("add_0__x", grp.inputs)
        self.assertEqual(sum(1 for k in grp.inputs if k == "add_0__x"), 1)

    def test_dedups_multiple_outbound_edges_from_same_source(self) -> None:
        """Two cross-boundary edges leaving the same (child, port) collapse
        to a single subgraph output port."""
        parent = _fixtures.grouping_wf("parent")
        # `add_0/output_0` already feeds `sub_0/x` (inside). Add two outside
        # edges from the same source.
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="y"),
            ),
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        grp = parent.nodes["grp"]
        self.assertIn("add_0__output_0", grp.outputs)
        self.assertEqual(sum(1 for k in grp.outputs if k == "add_0__output_0"), 1)

    def test_fan_in_one_inner_edge_per_target(self) -> None:
        """
        Two cross-boundary feeders into the same (child, port) produce
        one inner edge, not one per feeder.

        Two inputs for the same target will fail to recipe, but preserve the mistake
        and let the recipe handle it.
        """
        parent = _fixtures.grouping_wf("parent")
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="add_0", port="x"),
            )
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        grp = parent.nodes["grp"]

        inner = [
            e
            for e in grp.edges
            if isinstance(e.source, fr.schemas.InputSource)
            and e.source.port == "add_0__x"
        ]
        self.assertEqual(len(inner), 1)

    def test_fan_out_one_inner_edge_per_source(self) -> None:
        """Two cross-boundary consumers of the same (child, port) produce
        one inner edge, not one per consumer."""
        parent = _fixtures.grouping_wf("parent")
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="y"),
            ),
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        grp = parent.nodes["grp"]
        inner = [
            e
            for e in grp.edges
            if isinstance(e.target, fr.schemas.OutputTarget)
            and e.target.port == "add_0__output_0"
        ]
        self.assertEqual(len(inner), 1)

    def test_fan_out_rewires_all_outer_edges(self) -> None:
        """Each cross-out edge still gets its own outer-side rewrite."""
        parent = _fixtures.grouping_wf("parent")
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="mul_0", port="y"),
            ),
        )
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        for tgt_port in ("x", "y"):
            self.assertIn(
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="grp", port="add_0__output_0"),
                    fr.schemas.TargetHandle(node="mul_0", port=tgt_port),
                ),
                parent.edges,
            )

    def test_group_leaves_subgraph_undo_stack_empty(self) -> None:
        """Constructing the subgraph during group() must not pollute its undo
        history; the new subgraph should look freshly minted."""
        parent = _fixtures.grouping_wf("parent")
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        grp = parent.nodes["grp"]
        self.assertEqual(len(grp.undo_stack), 0)
        self.assertEqual(len(grp.redo_stack), 0)


class TestUngroup(unittest.TestCase):
    def _group_then(self) -> workflow.Workflow:
        """Return a parent that has had `add_0` + `sub_0` grouped into 'grp'."""
        parent = _fixtures.grouping_wf("parent")
        parent.group("grp", parent.nodes["add_0"], parent.nodes["sub_0"])
        return parent

    def test_ungroup_workflow_child_shape(self) -> None:
        parent = self._group_then()
        parent.ungroup("grp")
        self.assertEqual(set(parent.nodes.keys()), {"grp_add_0", "grp_sub_0", "mul_0"})
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="grp_add_0", port="x"),
            ),
            parent.edges,
        )
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="grp_sub_0", port="output_0"),
                fr.schemas.OutputTarget(port="diff"),
            ),
            parent.edges,
        )
        self.assertIn(
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="grp_add_0", port="output_0"),
                fr.schemas.TargetHandle(node="grp_sub_0", port="x"),
            ),
            parent.edges,
        )

    def test_ungroup_macro_child_calls_unlock_first(self) -> None:
        parent = workflow.Workflow("parent")
        parent.create_input("x")
        parent.create_input("y")
        parent.create_input("z")
        parent.create_output("a")
        parent.create_output("s")
        parent.add_node(_fixtures.macro_node("m"))
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="m", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="m", port="y"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="z"),
                fr.schemas.TargetHandle(node="m", port="z"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="m", port="a"),
                fr.schemas.OutputTarget(port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="m", port="s"),
                fr.schemas.OutputTarget(port="s"),
            ),
        )

        original_macro = parent.nodes["m"]
        original_child_labels = set(original_macro.recipe.nodes.keys())
        parent_nodes_before = set(parent.nodes.keys())
        edges_before = set(parent.edges)

        parent.ungroup("m")

        self.assertNotIn("m", parent.nodes)
        self.assertIn("m_add_0", parent.nodes)
        self.assertIn("m_sub_0", parent.nodes)

        parent.undo()

        self.assertIs(
            parent.nodes["m"],
            original_macro,
            msg="undo should restore the original Macro instance, not a rebuild",
        )
        self.assertIsInstance(parent.nodes["m"], dag.Macro)
        self.assertEqual(
            set(parent.nodes["m"].recipe.nodes.keys()),
            original_child_labels,
            msg="restored macro should retain its original children",
        )
        self.assertEqual(
            set(parent.nodes.keys()),
            parent_nodes_before,
            msg="no lifted children should remain at the top level after undo",
        )
        self.assertEqual(set(parent.edges), edges_before)

    def test_label_map_overrides_defaults(self) -> None:
        parent = self._group_then()
        parent.ungroup("grp", label_map={"add_0": "renamed_add"})
        self.assertIn("renamed_add", parent.nodes)
        self.assertIn("grp_sub_0", parent.nodes)

    def test_label_map_unknown_key_raises(self) -> None:
        parent = self._group_then()
        with self.assertRaises(ValueError):
            parent.ungroup("grp", label_map={"not_a_child": "x"})

    def test_label_map_value_duplicates_raise(self) -> None:
        parent = self._group_then()
        with self.assertRaises(ValueError):
            parent.ungroup("grp", label_map={"add_0": "same", "sub_0": "same"})

    def test_label_collision_raises(self) -> None:
        parent = self._group_then()
        parent.rename_node("mul_0", "grp_add_0")
        with self.assertRaises(ValueError):
            parent.ungroup("grp")

    def test_block_if_reference_blocks(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(_fixtures.macro_node("m"))
        with self.assertRaises(ValueError):
            parent.ungroup("m", block_if_reference=True)

    def test_block_if_reference_false_succeeds(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(_fixtures.macro_node("m"))
        parent.ungroup("m", block_if_reference=False)
        self.assertNotIn("m", parent.nodes)

    def test_inner_passthrough_composes_outer_edges(self) -> None:
        parent = workflow.Workflow("parent")
        parent.create_input("p_in")
        parent.create_output("p_out")
        sub = _fixtures.passthrough_subgraph_wf("sub")
        parent.add_node(sub)
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="p_in"),
                fr.schemas.TargetHandle(node="sub", port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="sub", port="b"),
                fr.schemas.OutputTarget(port="p_out"),
            ),
        )

        parent.ungroup("sub")

        self.assertNotIn("sub", parent.nodes)
        self.assertEqual(
            set(parent.edges),
            {
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="p_in"),
                    fr.schemas.OutputTarget(port="p_out"),
                )
            },
        )

    def test_inner_passthrough_multi_driver_multi_consumer(self) -> None:
        parent = workflow.Workflow("parent")
        parent.create_input("p_in_1")
        parent.create_input("p_in_2")
        parent.create_output("p_out_1")
        parent.create_output("p_out_2")
        sub = _fixtures.passthrough_subgraph_wf("sub")
        parent.add_node(sub)
        parent.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="p_in_1"),
                fr.schemas.TargetHandle(node="sub", port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="p_in_2"),
                fr.schemas.TargetHandle(node="sub", port="a"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="sub", port="b"),
                fr.schemas.OutputTarget(port="p_out_1"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="sub", port="b"),
                fr.schemas.OutputTarget(port="p_out_2"),
            ),
        )

        parent.ungroup("sub")

        expected = {
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port=src),
                fr.schemas.OutputTarget(port=tgt),
            )
            for src in ("p_in_1", "p_in_2")
            for tgt in ("p_out_1", "p_out_2")
        }
        self.assertEqual(set(parent.edges), expected)

    def test_undo_restores_pre_ungroup_state(self) -> None:
        parent = self._group_then()
        nodes_before = dict(parent.nodes._pwf_lexical_map__data)
        edges_before = set(parent.edges)
        parent.ungroup("grp")
        parent.undo()
        self.assertEqual(dict(parent.nodes._pwf_lexical_map__data), nodes_before)
        self.assertEqual(set(parent.edges), edges_before)

    def test_run_after_ungroup_matches_pre_ungroup(self) -> None:
        baseline = self._group_then()
        baseline_run = baseline.run(x=2, y=3, z=4)
        candidate = self._group_then()
        candidate.ungroup("grp")
        candidate_run = candidate.run(x=2, y=3, z=4)
        self.assertEqual(
            baseline_run.result.output_ports["diff"].value,
            candidate_run.result.output_ports["diff"].value,
        )

    def test_passing_atomic_raises_typeerror(self) -> None:
        parent = workflow.Workflow("parent")
        parent.add_node(_fixtures.atomic_add_node("a"))
        with self.assertRaises(TypeError):
            parent.ungroup("a")

    def test_flatten(self):
        wf = workflow.Workflow("to_flatten")
        wf.create_input("m", "x", "b")
        wf.y = wf.inputs.m * wf.inputs.x + wf.inputs.b
        wf.create_output_from(wf.y, "y")
        expected_output = wf.run(m=2, x=3, b=4).result.output_ports["y"].value
        wf.flatten()
        self.assertEqual(
            len(wf.nodes), 2, msg="Expect the add node and the multiply node"
        )
        self.assertEqual(
            wf.run(m=2, x=3, b=4).result.output_ports["y"].value, expected_output
        )

    def test_flatten_flow_control_raises(self):
        wf = workflow.Workflow("to_flatten")
        wf.child_with_flow_controls = _fixtures.if_abs_node()
        with self.assertRaisesRegex(
            TypeError,
            "Cannot unlock 'to_flatten.child_with_flow_controls_if_0'",
        ):
            wf.flatten()


class TestWorkflowFromRecipe(unittest.TestCase):
    """`Workflow.from_recipe` round-trips structure through a `WorkflowRecipe`."""

    @staticmethod
    def _source() -> workflow.Workflow:
        # 3 inputs, 1 output, 3 nodes, edges covering input/peer/output kinds.
        return _fixtures.grouping_wf("source")

    def test_nodes_preserved(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        self.assertEqual(set(rebuilt.nodes.keys()), set(src.nodes.keys()))

    def test_inputs_preserved(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        self.assertEqual(set(rebuilt.inputs.keys()), set(src.inputs.keys()))

    def test_outputs_preserved(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        self.assertEqual(set(rebuilt.outputs.keys()), set(src.outputs.keys()))

    def test_edges_preserved(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        self.assertEqual(set(rebuilt.edges), set(src.edges))

    def test_label_uses_argument(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("custom", src.recipe)
        self.assertEqual(rebuilt.label, "custom")

    def test_rebuilt_owns_children(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        for child in rebuilt.nodes.values():
            self.assertIs(child.owner, rebuilt)

    def test_ports_owned_by_rebuilt(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        for port in rebuilt.inputs.values():
            self.assertIs(port.owner, rebuilt)
        for port in rebuilt.outputs.values():
            self.assertIs(port.owner, rebuilt)

    def test_run_matches_source(self) -> None:
        src = self._source()
        src_run = src.run(x=2, y=3, z=4)
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        rebuilt_run = rebuilt.run(x=2, y=3, z=4)
        self.assertEqual(
            src_run.outputs.diff,
            rebuilt_run.outputs.diff,
        )

    def test_undo_stack_empty(self) -> None:
        src = self._source()
        rebuilt = workflow.Workflow.from_recipe("rebuilt", src.recipe)
        self.assertEqual(len(rebuilt.undo_stack), 0)
        self.assertEqual(len(rebuilt.redo_stack), 0)

    def test_empty_recipe(self) -> None:
        empty_recipe = workflow.Workflow("empty").recipe
        rebuilt = workflow.Workflow.from_recipe("rebuilt", empty_recipe)
        self.assertEqual(len(rebuilt.nodes), 0)
        self.assertEqual(len(rebuilt.inputs), 0)
        self.assertEqual(len(rebuilt.outputs), 0)
        self.assertEqual(len(rebuilt.edges), 0)


class TestWorkflowConnectAtInit(unittest.TestCase):
    """`Workflow` connection sugar at construction.

    A freshly-built workflow has no owner, so every connection takes the
    'pending' route rather than being applied as an edge immediately.
    """

    def test_connect_port_is_pending(self) -> None:
        src = _fixtures.atomic_add_node("src")
        port = src.outputs["output_0"]
        wf = workflow.Workflow("wf", x=port)
        self.assertIs(wf._pending_connections["x"], port)

    def test_connect_single_output_node_coerces_to_its_port(self) -> None:
        src = _fixtures.atomic_add_node("src")
        wf = workflow.Workflow("wf", y=src)
        self.assertIs(wf._pending_connections["y"], src.outputs["output_0"])

    def test_connect_multi_output_node_raises(self) -> None:
        multi = _fixtures.macro_node("multi")  # outputs `a` and `s`
        with self.assertRaises(ValueError):
            workflow.Workflow("wf", x=multi)

    def test_connect_wrong_type_raises(self) -> None:
        with self.assertRaises(TypeError):
            workflow.Workflow("wf", x=42)

    def test_connections_stay_pending_without_owner(self) -> None:
        src = _fixtures.atomic_add_node("src")
        wf = workflow.Workflow("wf", x=src.outputs["output_0"])
        self.assertIsNone(wf.owner)
        self.assertEqual(wf.edges, [])
        self.assertIn("x", wf._pending_connections)

    def test_undo_limit_and_connections_coexist(self) -> None:
        src = _fixtures.atomic_add_node("src")
        wf = workflow.Workflow("wf", 5, x=src.outputs["output_0"])
        self.assertEqual(wf.undo_limit, 5)
        self.assertIn("x", wf._pending_connections)


class TestWorkflowData(unittest.TestCase):
    def test_annotations_propagate_from_wfms_to_data(self):
        wf = workflow.Workflow.from_recipe("wf", _fixtures.annotated_wf.flowrep_recipe)
        self.assertIs(wf.inputs.w.type_hint, None)
        self.assertIs(wf.inputs.w.type_metadata, None)
        self.assertIs(wf.inputs.x.type_hint, int)
        self.assertIs(wf.inputs.x.type_metadata, None)
        self.assertIs(wf.inputs.y.type_hint, int)
        self.assertIs(wf.inputs.y.type_metadata, None)
        self.assertFalse(
            wf.inputs.y.has_default,
            msg="Mutable workflows have no underlying python reference and thus eschew "
            "storing defaults to avoid the WfMS containing data state.",
        )
        self.assertIs(wf.inputs.z.type_hint, float)
        self.assertEqual(
            wf.inputs.z.type_metadata, semantikon.TypeMetadata(units="meters")
        )
        self.assertIs(wf.outputs.x.type_hint, int)
        self.assertIs(wf.outputs.x.type_metadata, None)
        self.assertIs(wf.outputs.m2cm.type_hint, float)
        self.assertEqual(
            wf.outputs.m2cm.type_metadata, semantikon.TypeMetadata(units="centimeters")
        )


class TestWorkflowCopy(unittest.TestCase):
    def test_copy_carries_executors_and_drops_parentage(self):
        # A recipe-valid (fully wired) workflow -- Workflow.copy routes through
        # `self.recipe`, which validates that every target has a source/default.
        wf = _fixtures.build_workflow(
            inputs=["x", "y", "z"],
            outputs=["a", "s"],
            node_specs={
                "add_0": _fixtures.atomic_add_node,
                "sub_0": _fixtures.atomic_sub_node,
            },
            edges=_fixtures._MACRO_WF_EDGES,
        )
        child = wf.nodes["add_0"]
        wf_exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 1},
        )
        child_exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 2},
        )
        wf.executor = wf_exe
        child.executor = child_exe

        copy = wf.copy()
        copied_child = copy.nodes["add_0"]

        self.assertIsNot(copy, wf)
        self.assertIs(copy.executor, wf_exe)
        self.assertIs(copied_child.executor, child_exe)
        self.assertIsNone(copy.owner)
        self.assertIs(copied_child.owner, copy)
        self.assertIs(wf.nodes["add_0"].owner, wf)


if __name__ == "__main__":
    unittest.main()
