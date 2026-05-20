from __future__ import annotations

import collections
import unittest

from _wfms import datatypes, workflow
from unit._wfms import _fixtures


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

    def test_setattr_delegates_to_setitem(self) -> None:
        # Attribute-style assignment runs through the same owner check.
        self.wf.inputs.x = self.port
        self.assertIs(self.wf.inputs["x"], self.port)

        foreign = datatypes.InputPort(
            label="y", owner=self.other, type_hint=None, type_metadata=None
        )
        with self.assertRaisesRegex(ValueError, "Port 'y' already has owner 'other'"):
            self.wf.inputs.y = foreign

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
        self.wf.nodes["adder"] = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_setitem_foreign_owned_raises(self) -> None:
        node = _fixtures.atomic_add_node()
        self.other.nodes["adder"] = node
        self.assertIs(node.owner, self.other)
        with self.assertRaisesRegex(
            ValueError, "ode 'adder' already has owner 'other'"
        ):
            self.wf.nodes["adder"] = node

    def test_setattr_delegates_to_setitem(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes.adder = node
        self.assertIs(node.owner, self.wf)
        self.assertIs(self.wf.nodes["adder"], node)

    def test_delitem_detaches_node(self) -> None:
        node = _fixtures.atomic_add_node()
        self.wf.nodes["adder"] = node
        del self.wf.nodes["adder"]
        self.assertIsNone(node.owner)
        self.assertNotIn("adder", self.wf.nodes)
