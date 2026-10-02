"""
Covers the testable surface of `dag.py`:

* `MutablePortMap` (owner-check insert, `__setattr__` delegation, `__delitem__`).
* `MutableNodeMap` (reparenting insert, conflict raise, `__setattr__`,
  `__delitem__`).
* `Workflow.__init__` and `Workflow.undo_limit` (the only non-stub surface).
* `Macro` end-to-end via fixtures (children, edges identity, `run`).
* `evaluate_dag` smoke (via a macro run) and greedy scheduling: punch-through,
  shared-source in-degree, failure isolation, `BaseException` passthrough.
* `topo_sort_nodes` for empty / single-layer / linear chain / order-determinism.
* `gather_target_inputs` for input-edge, sibling-edge, and port-omitted paths.
* `populate_outputs` for both `SourceHandle` and `InputSource` sources.
"""

from __future__ import annotations

import pickle
import threading
import unittest

import flowrep as fr
from unit import _fixtures

from pyiron_workflow import dag, datatypes, execution


@fr.atomic
def _problematic(x):
    raise ValueError("problem in node")
    return x  # noqa: F841


@fr.workflow
def _single_error(x):
    ok = _fixtures.plain_increment(x)
    problem = _problematic(x)
    ok_again = _fixtures.plain_increment(x)
    return ok, problem, ok_again


@fr.workflow
def _double_error(x):
    ok = _fixtures.plain_increment(x)
    problem = _problematic(x)
    problem_again = _problematic(x)
    return ok, problem, problem_again


_RELEASE_TIMEOUT = 2.0
_release = threading.Event()
_RECORDED: list[str] = []


@fr.atomic
def _set_release(x):
    _release.set()
    return x


@fr.atomic
def _wait_for_release(x):
    return _release.wait(timeout=_RELEASE_TIMEOUT)


@fr.workflow
def _punch_through(x):
    a = _fixtures.plain_increment(x)
    b = _set_release(a)
    c = _wait_for_release(x)
    return b, c


@fr.workflow
def _shared_source(x):
    a = _fixtures.plain_increment(x)
    b = _fixtures.plain_increment(a)
    c = _fixtures.add(a, a)
    d = _fixtures.add(b, c)
    return d


@fr.atomic
def _mark_downstream(x):
    _RECORDED.append("downstream")
    return x


@fr.atomic
def _mark_independent(x):
    _RECORDED.append("independent")
    return x


@fr.workflow
def _failure_isolation(x):
    problem = _problematic(x)
    downstream = _mark_downstream(problem)
    first = _fixtures.plain_increment(x)
    independent = _mark_independent(first)
    return downstream, independent


class _Abort(BaseException):
    pass


@fr.atomic
def _abort(x):
    raise _Abort()
    return x  # noqa: F841


@fr.workflow
def _abort_and_fail(x):
    stop = _abort(x)
    problem = _problematic(x)
    return stop, problem


class TestMacro(unittest.TestCase):
    """End-to-end exercise of `Macro` via the `macro` fixture."""

    def test_run_produces_expected_outputs(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        # Macro outputs come from inner `add` (a) and `sub` (s).
        self.assertEqual(run.outputs.a, 3)
        self.assertEqual(run.outputs.s, 0)

    def test_run_records_one_step_per_child(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        labels = [step.label for step in run.steps]
        self.assertEqual(sorted(labels), ["add_0", "sub_0"])
        self.assertEqual(len(run.steps), 2)

    def test_edges_constructed_from_recipe(self) -> None:
        n = _fixtures.macro_node()
        recipe_edges = (
            [(source, target) for target, source in n.recipe.input_edges.items()]
            + [(source, target) for target, source in n.recipe.edges.items()]
            + [(source, target) for target, source in n.recipe.output_edges.items()]
        )
        self.assertSetEqual(set(recipe_edges), set(n.edges))

    def test_nodes_property_returns_constructed_node_map(self) -> None:
        n = _fixtures.macro_node()
        self.assertIsInstance(n.nodes, datatypes.NodeMap)
        self.assertEqual(set(n.nodes), {"add_0", "sub_0"})

    def test_function_metadata_consistent_with_reference(self) -> None:
        # The fixture-decorated macro has no `_semantikon_metadata` attribute,
        # so the captured value is `None`. The attribute exists in both cases.
        undecorated = _fixtures.macro_node()
        self.assertIsNone(undecorated.function_metadata)

        decorated = _fixtures.annotated_macro_node()
        self.assertEqual("This is decorated", decorated.function_metadata["uri"])

    def test_result_type_classmethod(self) -> None:
        self.assertIs(dag.Macro._result_type(), fr.schemas.DagData)

    def test_transformer_children_round_trip_values(self) -> None:
        run = _fixtures.autoencoder_node().run(a=1, b=20, c=300)
        self.assertEqual((run.outputs.x, run.outputs.y, run.outputs.z), (1, 20, 300))


class TestEvaluateDag(unittest.TestCase):
    def test_children_results_attached_to_run(self) -> None:
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        # Every child has a result on the parent's run.result.nodes
        # and a successfully finished sub-run.
        self.assertEqual(set(run.result.nodes), {"add_0", "sub_0"})
        for step in run.steps:
            self.assertEqual(step.status, execution.RunStatus.FINISHED)


class TestTopoSortNodes(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(dag.topo_sort_nodes({}, {}), [])

    def test_single_layer_no_edges_sorted_by_label(self) -> None:
        # `topo_sort_nodes` only consumes `nodes` as an iterable of labels,
        # so a plain `dict` is a valid stand-in for `NodeMap`.
        nodes = {"c": None, "a": None, "b": None}
        self.assertEqual(dag.topo_sort_nodes(nodes, {}), [["a", "b", "c"]])

    def test_linear_chain(self) -> None:
        nodes = {"a": None, "b": None, "c": None}
        edges = {
            fr.schemas.TargetHandle(node="b", port="x"): fr.schemas.SourceHandle(
                node="a", port="out"
            ),
            fr.schemas.TargetHandle(node="c", port="x"): fr.schemas.SourceHandle(
                node="b", port="out"
            ),
        }
        self.assertEqual(dag.topo_sort_nodes(nodes, edges), [["a"], ["b"], ["c"]])

    def test_layered_deterministic_across_insertion_orders(self) -> None:
        edges = {
            fr.schemas.TargetHandle(node="b", port="x"): fr.schemas.SourceHandle(
                node="a", port="out"
            ),
            fr.schemas.TargetHandle(node="c", port="x"): fr.schemas.SourceHandle(
                node="b", port="out"
            ),
        }
        forward = {"a": None, "b": None, "c": None}
        reverse = {"c": None, "b": None, "a": None}
        self.assertEqual(
            dag.topo_sort_nodes(forward, edges),
            dag.topo_sort_nodes(reverse, edges),
        )


class TestGatherTargetInputs(unittest.TestCase):
    def test_input_edge_path(self) -> None:
        # `add_0` in the `macro` fixture takes both x and y from parent inputs.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs("add_0", run.result)
        self.assertEqual(inputs, {"x": 1, "y": 2})

    def test_sibling_edge_path(self) -> None:
        # `sub_0` reads `x` from sibling `add_0` output and `y` from
        # parent input `z`.
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        inputs = dag.gather_target_inputs("sub_0", run.result)
        self.assertEqual(inputs, {"x": 3, "y": 3})

    def test_port_omitted_when_no_edge(self) -> None:
        n = _fixtures.container_node()
        run = n.run()
        self.assertEqual(
            dag.gather_target_inputs("multiply_with_defaults_0", run.result),
            {},
            msg="The child has no input edges, so all ports should be omitted",
        )


class TestPopulateOutputs(unittest.TestCase):
    def test_source_handle_path(self) -> None:
        # `macro` output `s` is fed by `sub_0.output_0` (a SourceHandle).
        n = _fixtures.macro_node()
        run = n.run(x=1, y=2, z=3)
        self.assertEqual(run.outputs.s, 0)
        self.assertEqual(run.outputs.a, 3)

    def test_input_source_path(self) -> None:
        # `passthrough` output `x` is fed directly by parent input `x`
        # (an InputSource). `s` exercises the SourceHandle branch as a sanity
        # check that both branches coexist in one run.
        n = _fixtures.passthrough_node()
        run = n.run(x=42, y=5)
        self.assertEqual(run.outputs.x, 42)
        self.assertEqual(run.outputs.s, 47)


class TestMacroAttributeSugar(unittest.TestCase):
    """`__getattr__` node-map fallback on `Macro` (a `StaticGraph`)."""

    def test_sugar_returns_node(self) -> None:
        m = _fixtures.attr_sugar_macro_node()
        self.assertIs(m.plain, m.nodes["plain"])

    def test_real_attribute_shadows_node(self) -> None:
        m = _fixtures.attr_sugar_macro_node()
        # `executor` is a real attribute; the same-named node stays hidden.
        self.assertIsNone(m.executor)
        self.assertIsNot(m.executor, m.nodes["executor"])
        # `nodes` is a real property returning the node map itself.
        self.assertIsInstance(m.nodes, datatypes.NodeMap)

    def test_unknown_attribute_raises(self) -> None:
        m = _fixtures.attr_sugar_macro_node()
        with self.assertRaises(AttributeError):
            _ = m.does_not_exist

    def test_underscore_name_not_resolved(self) -> None:
        # Names starting with `_` are never routed to the nodes map.
        m = _fixtures.attr_sugar_macro_node()
        with self.assertRaises(AttributeError):
            _ = m._does_not_exist


class TestMacroPickle(unittest.TestCase):
    """A pickled graph must keep its children parented (executor round-trip)."""

    def test_round_trip_reparents_children(self) -> None:
        m = _fixtures.macro_node()
        restored = pickle.loads(pickle.dumps(m))
        self.assertEqual(sorted(restored.nodes), sorted(m.nodes))
        for label, child in restored.nodes.items():
            self.assertIs(child.owner, restored, msg=f"{label} lost its owner")
            self.assertIsNone(
                child._detached_root, msg=f"{label} kept a stale detached root"
            )

    def test_round_trip_reparents_nested_children(self) -> None:
        m = _fixtures.nested_macro_node()
        restored = pickle.loads(pickle.dumps(m))
        inner = next(c for c in restored.nodes.values() if isinstance(c, dag.Macro))
        self.assertIs(inner.owner, restored)
        for label, child in inner.nodes.items():
            self.assertIs(child.owner, inner, msg=f"nested {label} lost its owner")


class TestErrorParallelism(unittest.TestCase):
    def setUp(self) -> None:
        self.single = dag.Macro(_single_error.flowrep_recipe, "single")
        self.double = dag.Macro(_double_error.flowrep_recipe, "double")

    def test_single_error_raises(self):
        with self.assertRaises(ValueError):
            self.single.run(x=1)

    def test_double_error_raises_group(self):
        with self.assertRaises(ExceptionGroup) as e:
            self.double.run(x=1)
        group = e.exception
        self.assertEqual(len(group.exceptions), 2)
        for exc in group.exceptions:
            self.assertIsInstance(exc, ValueError)

    def test_fast_failure_raises_single_error(self):
        cfg = execution.RunConfig(dag_fail_fast=True)
        with self.assertRaises(ValueError):
            self.double.run(cfg, x=1)

    def test_unthreaded_raises_single_error(self):
        cfg = execution.RunConfig(dag_multithreaded=False)
        with self.assertRaises(ValueError):
            self.double.run(cfg, x=1)


class TestGreedyScheduling(unittest.TestCase):
    def setUp(self) -> None:
        _release.clear()
        _RECORDED.clear()

    def test_chain_punches_through_slow_sibling(self) -> None:
        # Layers would be [a, c], [b]: `c` blocks until `b` releases it, so `c`
        # only sees the release if `b` can start before `c` finishes.
        macro = dag.Macro(_punch_through.flowrep_recipe, "punch")
        run = macro.run(x=1)
        self.assertTrue(run.outputs.c)

    def test_shared_source_counts_every_edge(self) -> None:
        # `c` takes both ports from `a`, so `a` contributes in-degree 2 to it.
        macro = dag.Macro(_shared_source.flowrep_recipe, "shared")
        run = macro.run(x=1)
        self.assertEqual(run.outputs.d, 7)  # b=3, c=2+2=4, d=3+4
        self.assertEqual(len(run.steps), 4)

    def test_failure_does_not_stop_independent_branches(self) -> None:
        macro = dag.Macro(_failure_isolation.flowrep_recipe, "isolation")
        with self.assertRaises(ValueError):
            macro.run(x=1)
        self.assertEqual(_RECORDED, ["independent"])

    def test_base_exception_is_not_grouped(self) -> None:
        macro = dag.Macro(_abort_and_fail.flowrep_recipe, "abort")
        with self.assertRaises(_Abort):
            macro.run(x=1)


if __name__ == "__main__":
    unittest.main()
