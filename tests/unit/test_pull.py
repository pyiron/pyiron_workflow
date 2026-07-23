from __future__ import annotations

import unittest
from concurrent import futures

import flowrep as fr
from unit import _fixtures

from pyiron_workflow._wfms import api, execution, flowcontrollers, pull
from pyiron_workflow._wfms.datatypes import EdgeTuple


class TestPullUnparented(unittest.TestCase):
    def test_runs_and_returns_node_outputs(self):
        n = _fixtures.atomic_add_node()  # add(x, y), unparented
        run = n.pull(x=2, y=3)
        self.assertEqual(run.outputs.output_0, 5)

    def test_required_input_keys_are_bare_port_names(self):
        n = _fixtures.atomic_add_node()
        self.assertEqual(set(n.pulled_inputs().keys()), {"x", "y"})

    def test_pulled_workflow_contains_just_the_node(self):
        n = _fixtures.atomic_add_node("addy")
        wf = n.pulled_workflow()
        self.assertEqual(set(wf.nodes.keys()), {"addy"})

    def test_defaults_ride_and_are_not_surfaced(self):
        m = _fixtures.multiply_with_defaults_node()  # x=1, y=2
        run = m.pull()
        self.assertEqual(run.outputs.output_0, 2)
        self.assertEqual(set(m.pulled_inputs().keys()), set())

    def test_expose_defaults_surfaces_them_as_required(self):
        m = _fixtures.multiply_with_defaults_node()
        self.assertEqual(set(m.pulled_inputs(False, True).keys()), {"x", "y"})
        run = m.pull(None, False, True, x=4, y=5)
        self.assertEqual(run.outputs.output_0, 20)

    def test_unknown_kwarg_raises_pointing_at_pulled_inputs(self):
        n = _fixtures.atomic_add_node()
        with self.assertRaises(ValueError) as ctx:
            n.pull(x=2, y=3, nonsense=1)
        self.assertIn("pulled_inputs", str(ctx.exception))

    def test_missing_required_input_raises(self):
        n = _fixtures.atomic_add_node()
        with self.assertRaises(ValueError) as ctx:
            n.pull(x=2)  # missing y
        self.assertIn("y", str(ctx.exception))

    def test_free_function_matches_method(self):
        n = _fixtures.atomic_add_node()
        run = pull.pull(n, None, False, False, x=1, y=1)
        self.assertEqual(run.outputs.output_0, 2)


def _diamond_workflow():
    """add_0 -> sub_0; mul_0 is a parallel, unrelated branch."""
    return _fixtures.build_workflow(
        inputs=["x", "y", "z"],
        outputs=["out", "side"],
        node_specs={
            "add_0": _fixtures.atomic_add_node,
            "sub_0": _fixtures.atomic_sub_node,
            "mul_0": _fixtures.multiply_with_defaults_node,
        },
        edges=[
            EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="add_0", port="x"),
            ),
            EdgeTuple(
                fr.schemas.InputSource(port="y"),
                fr.schemas.TargetHandle(node="add_0", port="y"),
            ),
            EdgeTuple(
                fr.schemas.SourceHandle(node="add_0", port="output_0"),
                fr.schemas.TargetHandle(node="sub_0", port="x"),
            ),
            EdgeTuple(
                fr.schemas.InputSource(port="z"),
                fr.schemas.TargetHandle(node="sub_0", port="y"),
            ),
            EdgeTuple(
                fr.schemas.SourceHandle(node="sub_0", port="output_0"),
                fr.schemas.OutputTarget(port="out"),
            ),
            EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="mul_0", port="x"),
            ),
            EdgeTuple(
                fr.schemas.SourceHandle(node="mul_0", port="output_0"),
                fr.schemas.OutputTarget(port="side"),
            ),
        ],
    )


class TestPullWorkflowStopping(unittest.TestCase):
    def test_cone_excludes_unrelated_branch(self):
        wf = _diamond_workflow()
        pw = wf.nodes["sub_0"].pulled_workflow()
        self.assertEqual(set(pw.nodes.keys()), {"add_0", "sub_0"})

    def test_keys_mix_peer_terminal_and_ceiling_terminal(self):
        wf = _diamond_workflow()
        # sub_0.x <- add_0 (peer); sub_0.y <- InputSource z (terminal); add_0.x/y <- terminals x/y
        self.assertEqual(set(wf.nodes["sub_0"].pulled_inputs().keys()), {"x", "y", "z"})

    def test_runs_the_cone(self):
        wf = _diamond_workflow()
        run = wf.nodes["sub_0"].pull(x=1, y=2, z=1)  # add_0=3, sub_0=3-1=2
        self.assertEqual(run.outputs.output_0, 2)

    def test_macro_child_stopping(self):
        macro = _fixtures.macro_node()  # a=add(x,y), s=sub(a,z)
        run = macro.nodes["sub_0"].pull(x=1, y=2, z=1)
        self.assertEqual(run.outputs.output_0, 2)
        self.assertEqual(
            set(macro.nodes["sub_0"].pulled_inputs().keys()), {"x", "y", "z"}
        )


class TestPullNestedPunchingAndStopping(unittest.TestCase):
    def setUp(self):
        # nested_macro(x, y): z = add(x, y); a, s = macro(x, y, z); return a, s
        # inner macro: a = add(x, y); s = sub(a, z)
        self.nm = _fixtures.nested_macro_node()
        self.inner_sub = self.nm.nodes["macro_0"].nodes["sub_0"]

    def test_stopping_uses_inner_ceiling_keys(self):
        # ceiling = inner macro: sub_0.x <- inner add_0 (peer), sub_0.y <- z, add_0.x/y <- x/y
        self.assertEqual(set(self.inner_sub.pulled_inputs().keys()), {"x", "y", "z"})
        run = self.inner_sub.pull(x=1, y=2, z=5)  # add_0=3, sub=3-5=-2
        self.assertEqual(run.outputs.output_0, -2)

    def test_punching_uses_root_keys_and_flat_labels(self):
        keys = set(self.inner_sub.pulled_inputs(True).keys())
        self.assertEqual(keys, {"x", "y"})  # everything bottoms out at root ports
        pw = self.inner_sub.pulled_workflow(True)
        self.assertEqual(
            set(pw.nodes.keys()), {"add_0", "macro_0__add_0", "macro_0__sub_0"}
        )

    def test_punching_runs_the_full_cone(self):
        # root add_0 (z) = 1+2 = 3; inner add_0 = 1+2 = 3; inner sub = 3 - 3 = 0
        run = self.inner_sub.pull(None, True, False, x=1, y=2)
        self.assertEqual(run.outputs.output_0, 0)


def _foreach_with_macro_body():
    """ForEach whose body is the `macro` fixture: a = add(x, y); s = sub(a, z)."""
    body = fr.schemas.LabeledRecipe(label="body", recipe=_fixtures.macro.flowrep_recipe)
    recipe = fr.schemas.ForEachRecipe(
        inputs=["xs", "y", "z"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="body", port="z"): fr.schemas.InputSource(
                port="z"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="s"
            ),
        },
        nested_ports=["x"],
        zipped_ports=[],
    )

    return flowcontrollers.ForEach(recipe, "fe")


class TestPullFlowControl(unittest.TestCase):
    def test_punching_out_of_a_controller_raises(self):
        fe = _fixtures.foreach_node()  # body = add(x, y)
        body = fe.nodes["body"]
        with self.assertRaises(ValueError) as ctx:
            body.pulled_workflow(True)
        msg = str(ctx.exception)
        self.assertIn("flow controller", msg)
        self.assertIn("ForEach", msg)

    def test_stopping_inside_a_controller_isolates_the_node(self):
        fe = _fixtures.foreach_node()
        body = fe.nodes["body"]
        self.assertEqual(set(body.pulled_inputs().keys()), {"body__x", "body__y"})
        run = body.pull(None, False, False, body__x=2, body__y=3)
        self.assertEqual(run.outputs.output_0, 5)

    def test_macro_inside_controller_punch_fails_stop_succeeds(self):
        fe = _foreach_with_macro_body()
        inner_macro = fe.nodes["body"]
        with self.assertRaises(ValueError):
            inner_macro.pulled_workflow(True)
        run = inner_macro.pull(None, False, False, body__x=1, body__y=2, body__z=1)
        self.assertEqual(run.outputs.a, 3)  # add(1, 2)
        self.assertEqual(run.outputs.s, 2)  # 3 - 1


class TestPullCoverageEdgeCases(unittest.TestCase):
    """Focused tests that close specific line-coverage gaps in pull.py."""

    def test_unconnected_nonfed_port_in_workflow(self):
        # Node in a Workflow where one non-default input has no edge:
        # _incoming_edge returns None (line 67) and _resolve_input takes the
        # edge-is-None branch (lines 253-254).
        wf = _fixtures.build_workflow(
            inputs=["x"],
            node_specs={"add_0": _fixtures.atomic_add_node},
            edges=[
                EdgeTuple(
                    fr.schemas.InputSource(port="x"),
                    fr.schemas.TargetHandle(node="add_0", port="x"),
                ),
                # add_0.y is intentionally left unconnected
            ],
        )
        keys = set(wf.nodes["add_0"].pulled_inputs().keys())
        # x routed via ceiling InputSource port; y has no edge -> bare "add_0__y" via _require
        self.assertEqual(keys, {"x", "add_0__y"})

    def test_punching_through_macro_with_unconnected_boundary(self):
        # Punching from a grandchild node through a Macro whose boundary ports
        # are NOT connected in the parent Workflow: _resolve_boundary takes the
        # edge-is-None branch (lines 169-177).
        wf = _fixtures.build_workflow(
            node_specs={"inner": _fixtures.macro_node},
        )
        inner_sub = wf.nodes["inner"].nodes["sub_0"]
        keys = set(inner_sub.pulled_inputs(True).keys())
        # All three of inner's boundary ports are unconnected in wf:
        self.assertEqual(
            keys, {"inner__sub_0__y", "inner__add_0__x", "inner__add_0__y"}
        )

    def test_punching_from_node_inside_macro_inside_foreach_raises(self):
        # Punching (break_out=True) from a node inside a Macro that is itself
        # inside a ForEach: _resolve_boundary hits the flow-controller barrier
        # (lines 154-155) and raises.
        fe = _foreach_with_macro_body()
        inner_sub = fe.nodes["body"].nodes["sub_0"]
        with self.assertRaises(ValueError) as ctx:
            inner_sub.pulled_workflow(True)
        self.assertIn("flow controller", str(ctx.exception))


class TestPullExposeDefaultsNestedScoping(unittest.TestCase):
    """expose_defaults must surface unconnected *defaulted* ports on nested
    children with labels scoped by the child's path from the ceiling."""

    def _nested_defaulted_child(self):
        # Workflow root -> macro "inner" (container) -> "multiply_with_defaults_0",
        # whose x/y inputs are defaulted and left unconnected inside the macro.
        wf = _fixtures.build_workflow(node_specs={"inner": _fixtures.container_node})
        return wf.nodes["inner"].nodes["multiply_with_defaults_0"]

    def test_exposed_nested_defaults_get_scoped_labels_and_run(self):
        child = self._nested_defaulted_child()
        # Riding the defaults (expose_defaults=False), nothing is surfaced.
        self.assertEqual(set(child.pulled_inputs(True).keys()), set())
        # Forcing exposure surfaces both ports, scoped by the child's path from
        # the root ceiling -- not bare "x"/"y".
        scoped = {
            "inner__multiply_with_defaults_0__x",
            "inner__multiply_with_defaults_0__y",
        }
        self.assertEqual(set(child.pulled_inputs(True, True).keys()), scoped)
        # The scoped ports wire correctly and feed the isolated child.
        run = child.pull(
            None,
            True,
            True,
            **{
                "inner__multiply_with_defaults_0__x": 3,
                "inner__multiply_with_defaults_0__y": 4,
            },
        )
        self.assertEqual(run.outputs.output_0, 12)


class TestPullCopiesExecutors(unittest.TestCase):
    def test_pulled_workflow_carries_member_executor(self):
        n = _fixtures.atomic_add_node("addy")
        exe: execution.ExecutorInstructions = (
            futures.ThreadPoolExecutor,
            (),
            {"max_workers": 1},
        )
        n.executor = exe

        wf = n.pulled_workflow()

        self.assertIs(wf.nodes["addy"].executor, exe)


class TestPullPublicSurface(unittest.TestCase):
    def test_exposed_via_tools(self):
        n = _fixtures.atomic_add_node()
        self.assertEqual(
            api.tools.pull(n, None, False, False, x=1, y=4).outputs.output_0, 5
        )
        self.assertEqual(set(api.tools.pulled_inputs(n).keys()), {"x", "y"})
        self.assertIsNotNone(api.tools.pulled_workflow(n))


if __name__ == "__main__":
    unittest.main()
