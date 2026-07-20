from __future__ import annotations

import unittest

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow._wfms import (
    atomic,
    constructors,
    dag,
    datatypes,
    flowcontrollers,
    transformers,
    workflow,
)
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Module-level plain function (flowrep needs real source via inspect).        #
# --------------------------------------------------------------------------- #


def plain_add(x, y):
    """Undecorated callable — exercises the `fr.tools.parse_atomic` branch of `node`."""
    return x + y


# --------------------------------------------------------------------------- #
# Helpers for building minimal If / Try / While recipes.                      #
# --------------------------------------------------------------------------- #


def _conditional_case() -> fr.schemas.ConditionalCase:
    """Build a minimal `ConditionalCase` whose condition and body wrap `add`."""
    add_recipe = _fixtures.add.flowrep_recipe
    condition = fr.schemas.LabeledRecipe(label="cond", recipe=add_recipe)
    body = fr.schemas.LabeledRecipe(label="body", recipe=add_recipe)
    return fr.schemas.ConditionalCase(condition=condition, body=body)


def _conditional_input_edges() -> dict[fr.schemas.TargetHandle, fr.schemas.InputSource]:
    """Wire the `cond` / `body` inputs of the case above to parent `x`, `y`."""
    return {
        fr.schemas.TargetHandle(node="cond", port="x"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="cond", port="y"): fr.schemas.InputSource(
            port="y"
        ),
        fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
            port="y"
        ),
    }


def _if_recipe() -> fr.schemas.IfRecipe:
    return _fixtures.if_recipe()


def _while_recipe() -> fr.schemas.WhileRecipe:
    return fr.schemas.WhileRecipe(
        inputs=["x", "y"],
        outputs=["x"],  # While outputs must be a subset of inputs
        case=_conditional_case(),
        input_edges=_conditional_input_edges(),
        output_edges={
            fr.schemas.OutputTarget(port="x"): fr.schemas.SourceHandle(
                node="body", port="output_0"
            ),
        },
    )


def _try_recipe() -> fr.schemas.TryRecipe:
    add_recipe = _fixtures.add.flowrep_recipe
    try_body = fr.schemas.LabeledRecipe(label="trybody", recipe=add_recipe)
    handler = fr.schemas.LabeledRecipe(label="handler", recipe=add_recipe)
    exc_case = fr.schemas.ExceptionCase(
        exceptions=[versions.VersionInfo.of(ValueError)],
        body=handler,
    )
    input_edges = {
        fr.schemas.TargetHandle(node="trybody", port="x"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="trybody", port="y"): fr.schemas.InputSource(
            port="y"
        ),
        fr.schemas.TargetHandle(node="handler", port="x"): fr.schemas.InputSource(
            port="x"
        ),
        fr.schemas.TargetHandle(node="handler", port="y"): fr.schemas.InputSource(
            port="y"
        ),
    }
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["out"],
        try_node=try_body,
        exception_cases=[exc_case],
        input_edges=input_edges,
        prospective_output_edges={
            fr.schemas.OutputTarget(port="out"): [
                fr.schemas.SourceHandle(node="trybody", port="output_0")
            ],
        },
    )


# --------------------------------------------------------------------------- #
# Tests for `node`                                                   #
# --------------------------------------------------------------------------- #


class TestNode(unittest.TestCase):
    def test_node_passes_through_node_instances(self) -> None:
        node = _fixtures.atomic_add_node("original")
        still_node = constructors.node(node)
        self.assertIs(still_node, node)

    def test_node_relabels_node(self) -> None:
        node = _fixtures.atomic_add_node("original")
        result = constructors.node(node, "renamed")
        self.assertIs(result, node)
        self.assertEqual(result.label, "renamed")

    def test_node_rejects_non_node(self) -> None:
        with self.assertRaisesRegex(TypeError, "expected a Node"):
            constructors.node(42, "x")

    def test_atomic_recipe(self) -> None:
        result = constructors.node(_fixtures.add.flowrep_recipe, "added")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "added")

    def test_workflow_recipe(self) -> None:
        result = constructors.node(_fixtures.macro.flowrep_recipe, "m")
        self.assertIsInstance(result, dag.Macro)
        self.assertEqual(result.label, "m")

    def test_decorated_function(self) -> None:
        result = constructors.node(_fixtures.add, "added")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "added")

    def test_undecorated_function(self) -> None:
        result = constructors.node(_fixtures.plain_increment, "inc")
        self.assertIsInstance(result, atomic.Atomic)
        self.assertEqual(result.label, "inc")


# --------------------------------------------------------------------------- #
# Tests for `function2node`                                                   #
# --------------------------------------------------------------------------- #


class TestFunction2Node(unittest.TestCase):
    def test_atomic_decorated_default_label(self) -> None:
        n = constructors.function2node(_fixtures.add)
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "add")

    def test_atomic_decorated_explicit_label(self) -> None:
        n = constructors.function2node(_fixtures.add, label="custom")
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "custom")

    def test_workflow_decorated_default_label(self) -> None:
        n = constructors.function2node(_fixtures.macro)
        self.assertIsInstance(n, dag.Macro)
        self.assertEqual(n.label, "macro")

    def test_undecorated_function_parses_as_atomic(self) -> None:
        n = constructors.function2node(plain_add)
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "plain_add")


# --------------------------------------------------------------------------- #
# Tests for `recipe2node`                                                     #
# --------------------------------------------------------------------------- #


class TestRecipe2Node(unittest.TestCase):
    def test_atomic_recipe_returns_atomic(self) -> None:
        recipe = transformers.Transform1toN(2).recipe
        n = constructors.recipe2node(recipe)
        self.assertIsInstance(n, atomic.Atomic)

    def test_label(self) -> None:
        recipe = transformers.Transform1toN(2).recipe
        n = constructors.recipe2node(recipe)
        self.assertEqual(n.label, "atomic_recipe_node")
        n = constructors.recipe2node(recipe, label="explicit_label")
        self.assertEqual(n.label, "explicit_label")

    def test_workflow_recipe_returns_macro(self) -> None:
        recipe = _fixtures.macro.flowrep_recipe
        n = constructors.recipe2node(recipe)
        self.assertIsInstance(n, dag.Macro)

    def test_for_each_recipe_returns_for_each(self) -> None:
        recipe = _fixtures.for_wf.flowrep_recipe.nodes["for_each_0"]
        n = constructors.recipe2node(recipe)
        self.assertIsInstance(n, flowcontrollers.ForEach)

    def test_if_recipe_returns_if(self) -> None:
        n = constructors.recipe2node(_if_recipe())
        self.assertIsInstance(n, flowcontrollers.If)

    def test_try_recipe_returns_try(self) -> None:
        n = constructors.recipe2node(_try_recipe())
        self.assertIsInstance(n, flowcontrollers.Try)

    def test_while_recipe_returns_while(self) -> None:
        n = constructors.recipe2node(_while_recipe())
        self.assertIsInstance(n, flowcontrollers.While)

    def test_unknown_recipe_type_raises_type_error(self) -> None:
        with self.assertRaises(TypeError) as ctx:
            constructors.recipe2node(recipe=object(), label="x")  # type: ignore[arg-type]
        self.assertIn("Unknown recipe type", str(ctx.exception))


class TestWorkflow2MacroNonLossy(unittest.TestCase):
    def test_input_port_hints_preserved(self) -> None:
        wf = _fixtures.build_workflow(inputs=["x"], label="wf")
        wf.add_port_hint(wf.inputs["x"], int)
        macro = constructors.workflow2macro(wf)
        self.assertEqual(macro.inputs["x"].type_hint, int)

    def test_output_port_hints_preserved(self) -> None:
        wf = _fixtures.build_workflow(inputs=["y"], outputs=["y"], label="wf")
        wf.add_edge(
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="y"), fr.schemas.OutputTarget(port="y")
            )
        )
        wf.add_port_hint(wf.outputs["y"], float)
        macro = constructors.workflow2macro(wf)
        self.assertEqual(macro.outputs["y"].type_hint, float)


class _Sentinel:
    """Stand-in for `concurrent.futures.Executor` in tests; identity-comparable."""

    def __init__(self, name: str) -> None:
        self.name = name


class TestConvertersExecutorPreservation(unittest.TestCase):
    def test_workflow2macro_preserves_nested_executors(self) -> None:
        wf = _fixtures.build_workflow(
            inputs=["x", "y"],
            outputs=["a", "s"],
            node_specs={"nested": _fixtures.nested_macro_node},
            edges=[
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="x"),
                    fr.schemas.TargetHandle(node="nested", port="x"),
                ),
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="y"),
                    fr.schemas.TargetHandle(node="nested", port="y"),
                ),
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="nested", port="a"),
                    fr.schemas.OutputTarget(port="a"),
                ),
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="nested", port="s"),
                    fr.schemas.OutputTarget(port="s"),
                ),
            ],
            label="root",
        )
        outer = _Sentinel("outer")
        depth1 = _Sentinel("depth1")
        depth2 = _Sentinel("depth2")
        wf.executor = outer
        wf.nodes["nested"].executor = depth1
        wf.nodes["nested"].nodes["macro_0"].executor = depth2

        macro = constructors.workflow2macro(wf)

        self.assertIs(macro.executor, outer)
        self.assertIs(macro.nodes["nested"].executor, depth1)
        self.assertIs(macro.nodes["nested"].nodes["macro_0"].executor, depth2)

    def test_macro2workflow_preserves_nested_executors(self) -> None:
        m = _fixtures.nested_macro_node("nested")
        outer = _Sentinel("outer")
        depth1 = _Sentinel("depth1")
        m.executor = outer
        m.nodes["macro_0"].executor = depth1

        wf = constructors.macro2workflow(m)

        self.assertIs(wf.executor, outer)
        self.assertIs(wf.nodes["macro_0"].executor, depth1)


class TestEdges2EdgeList(unittest.TestCase):
    def test_empty(self) -> None:
        out = constructors.edges2edgelist({}, {}, {})
        self.assertEqual(out, [])

    def test_input_edges_only(self) -> None:
        e = {
            fr.schemas.TargetHandle(node="a", port="x"): fr.schemas.InputSource(
                port="x"
            ),
        }
        out = constructors.edges2edgelist(e, {}, {})
        self.assertEqual(
            out,
            [
                datatypes.EdgeTuple(
                    fr.schemas.InputSource(port="x"),
                    fr.schemas.TargetHandle(node="a", port="x"),
                )
            ],
        )

    def test_peer_edges_only(self) -> None:
        e = {
            fr.schemas.TargetHandle(node="b", port="x"): fr.schemas.SourceHandle(
                node="a", port="output_0"
            ),
        }
        out = constructors.edges2edgelist({}, e, {})
        self.assertEqual(
            out,
            [
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="a", port="output_0"),
                    fr.schemas.TargetHandle(node="b", port="x"),
                )
            ],
        )

    def test_output_edges_only(self) -> None:
        e = {
            fr.schemas.OutputTarget(port="out"): fr.schemas.SourceHandle(
                node="a", port="output_0"
            ),
        }
        out = constructors.edges2edgelist({}, {}, e)
        self.assertEqual(
            out,
            [
                datatypes.EdgeTuple(
                    fr.schemas.SourceHandle(node="a", port="output_0"),
                    fr.schemas.OutputTarget(port="out"),
                )
            ],
        )

    def test_round_trip_via_recipe_macro(self) -> None:
        recipe = _fixtures.macro.flowrep_recipe
        edge_list = constructors.edges2edgelist(
            recipe.input_edges, recipe.edges, recipe.output_edges
        )
        inp, peer, out = constructors.edgelist2edges(edge_list)
        self.assertEqual(inp, recipe.input_edges)
        self.assertEqual(peer, recipe.edges)
        self.assertEqual(out, recipe.output_edges)


class TestEdgeList2Edges(unittest.TestCase):
    def test_partitions_each_kind(self) -> None:
        edges: datatypes.EdgeList = [
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.TargetHandle(node="a", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="a", port="output_0"),
                fr.schemas.TargetHandle(node="b", port="x"),
            ),
            datatypes.EdgeTuple(
                fr.schemas.SourceHandle(node="b", port="output_0"),
                fr.schemas.OutputTarget(port="out"),
            ),
        ]
        inp, peer, out = constructors.edgelist2edges(edges)
        self.assertEqual(len(inp), 1)
        self.assertEqual(len(peer), 1)
        self.assertEqual(len(out), 1)
        self.assertEqual(
            inp[fr.schemas.TargetHandle(node="a", port="x")],
            fr.schemas.InputSource(port="x"),
        )

    def test_passthrough_output_edge(self) -> None:
        edges: datatypes.EdgeList = [
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.OutputTarget(port="y"),
            ),
        ]
        inp, peer, out = constructors.edgelist2edges(edges)
        self.assertEqual(inp, {})
        self.assertEqual(peer, {})
        self.assertEqual(
            out[fr.schemas.OutputTarget(port="y")], fr.schemas.InputSource(port="x")
        )

    def test_invalid_combination_raises(self) -> None:
        edges: datatypes.EdgeList = [
            datatypes.EdgeTuple(
                fr.schemas.InputSource(port="x"),
                fr.schemas.InputSource(port="y"),  # type: ignore[arg-type]
            ),
        ]
        with self.assertRaises(TypeError) as ctx:
            constructors.edgelist2edges(edges, scope="bad_owner")
        self.assertIn("bad_owner", str(ctx.exception))


class TestWorkflow2Macro(unittest.TestCase):
    def _build_wf(self) -> workflow.Workflow:
        return _fixtures.build_workflow(
            inputs=["x", "y", "z"],
            outputs=["a", "s"],
            node_specs={
                "add_0": _fixtures.atomic_add_node,
                "sub_0": _fixtures.atomic_sub_node,
            },
            edges=_fixtures._MACRO_WF_EDGES,
            label="wf",
        )

    def test_basic_shape(self) -> None:
        wf = self._build_wf()
        macro = constructors.workflow2macro(wf)
        self.assertIsInstance(macro, dag.Macro)
        self.assertEqual(macro.label, wf.label)
        self.assertEqual(set(macro.nodes.keys()), set(wf.nodes.keys()))
        self.assertEqual(set(macro.inputs.keys()), set(wf.inputs.keys()))
        self.assertEqual(set(macro.outputs.keys()), set(wf.outputs.keys()))
        self.assertEqual(set(macro.edges), set(wf.edges))

    def test_run_round_trips(self) -> None:
        wf = self._build_wf()
        macro = constructors.workflow2macro(wf)
        run = macro.run(x=1, y=2, z=4)
        self.assertEqual(run.result.output_ports["a"].value, 3)
        self.assertEqual(run.result.output_ports["s"].value, -1)


class TestMacro2Workflow(unittest.TestCase):
    def test_basic_shape(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        self.assertIsInstance(wf, workflow.Workflow)
        self.assertEqual(wf.label, m.label)
        self.assertEqual(set(wf.nodes.keys()), set(m.nodes.keys()))
        self.assertEqual(set(wf.inputs.keys()), set(m.inputs.keys()))
        self.assertEqual(set(wf.outputs.keys()), set(m.outputs.keys()))
        self.assertEqual(set(wf.edges), set(m.edges))

    def test_input_port_hints_preserved(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        for label, port in m.inputs.items():
            self.assertEqual(wf.inputs[label].type_hint, port.type_hint)
            self.assertEqual(wf.inputs[label].type_metadata, port.type_metadata)

    def test_output_port_hints_preserved(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        for label, port in m.outputs.items():
            self.assertEqual(wf.outputs[label].type_hint, port.type_hint)
            self.assertEqual(wf.outputs[label].type_metadata, port.type_metadata)

    def test_run_round_trips(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        run = wf.run(x=1, y=2, z=4)
        self.assertEqual(run.result.output_ports["a"].value, 3)
        self.assertEqual(run.result.output_ports["s"].value, -1)

    def test_recipe_round_trips(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        round_tripped = constructors.workflow2macro(wf)
        self.assertEqual(
            set(round_tripped.recipe.nodes.keys()),
            set(m.recipe.nodes.keys()),
        )
        self.assertEqual(round_tripped.recipe.input_edges, m.recipe.input_edges)
        self.assertEqual(round_tripped.recipe.edges, m.recipe.edges)
        self.assertEqual(round_tripped.recipe.output_edges, m.recipe.output_edges)

    def test_port_annotation_round_trips(self) -> None:
        m = _fixtures.macro_node("m")
        wf = constructors.macro2workflow(m)
        round_tripped = constructors.workflow2macro(wf)
        for label, port in m.outputs.items():
            self.assertEqual(round_tripped.outputs[label].type_hint, port.type_hint)
            self.assertEqual(
                round_tripped.outputs[label].type_metadata, port.type_metadata
            )


if __name__ == "__main__":
    unittest.main()
