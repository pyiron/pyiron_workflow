from __future__ import annotations

import unittest

from flowrep.api import schemas as frs
from pyiron_snippets import versions

from pyiron_workflow._wfms import (
    atomic,
    constructors,
    dag,
    flowcontrollers,
    transformers,
)
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Module-level plain function (flowrep needs real source via inspect).        #
# --------------------------------------------------------------------------- #


def plain_add(x, y):
    """Undecorated callable — exercises the `frt.parse_atomic` branch of `node`."""
    return x + y


# --------------------------------------------------------------------------- #
# Helpers for building minimal If / Try / While recipes.                      #
# --------------------------------------------------------------------------- #


def _conditional_case() -> frs.ConditionalCase:
    """Build a minimal `ConditionalCase` whose condition and body wrap `add`."""
    add_recipe = _fixtures.add.flowrep_recipe
    condition = frs.LabeledNode(label="cond", node=add_recipe)
    body = frs.LabeledNode(label="body", node=add_recipe)
    return frs.ConditionalCase(condition=condition, body=body)


def _conditional_input_edges() -> dict[frs.TargetHandle, frs.InputSource]:
    """Wire the `cond` / `body` inputs of the case above to parent `x`, `y`."""
    return {
        frs.TargetHandle(node="cond", port="x"): frs.InputSource(port="x"),
        frs.TargetHandle(node="cond", port="y"): frs.InputSource(port="y"),
        frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
        frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
    }


def _if_recipe() -> frs.IfNode:
    return _fixtures.if_recipe()


def _while_recipe() -> frs.WhileNode:
    return frs.WhileNode(
        inputs=["x", "y"],
        outputs=["x"],  # While outputs must be a subset of inputs
        case=_conditional_case(),
        input_edges=_conditional_input_edges(),
        output_edges={
            frs.OutputTarget(port="x"): frs.SourceHandle(node="body", port="output_0"),
        },
    )


def _try_recipe() -> frs.TryNode:
    add_recipe = _fixtures.add.flowrep_recipe
    try_body = frs.LabeledNode(label="trybody", node=add_recipe)
    handler = frs.LabeledNode(label="handler", node=add_recipe)
    exc_case = frs.ExceptionCase(
        exceptions=[versions.VersionInfo.of(ValueError)],
        body=handler,
    )
    input_edges = {
        frs.TargetHandle(node="trybody", port="x"): frs.InputSource(port="x"),
        frs.TargetHandle(node="trybody", port="y"): frs.InputSource(port="y"),
        frs.TargetHandle(node="handler", port="x"): frs.InputSource(port="x"),
        frs.TargetHandle(node="handler", port="y"): frs.InputSource(port="y"),
    }
    return frs.TryNode(
        inputs=["x", "y"],
        outputs=["out"],
        try_node=try_body,
        exception_cases=[exc_case],
        input_edges=input_edges,
        prospective_output_edges={
            frs.OutputTarget(port="out"): [
                frs.SourceHandle(node="trybody", port="output_0")
            ],
        },
    )


# --------------------------------------------------------------------------- #
# Tests for `node`                                                          #
# --------------------------------------------------------------------------- #


class TestNode(unittest.TestCase):
    def test_atomic_decorated_default_label(self) -> None:
        n = constructors.node(_fixtures.add)
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "add")

    def test_atomic_decorated_explicit_label(self) -> None:
        n = constructors.node(_fixtures.add, label="custom")
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "custom")

    def test_workflow_decorated_default_label(self) -> None:
        n = constructors.node(_fixtures.macro)
        self.assertIsInstance(n, dag.Macro)
        self.assertEqual(n.label, "macro")

    def test_undecorated_function_parses_as_atomic(self) -> None:
        n = constructors.node(plain_add)
        self.assertIsInstance(n, atomic.Atomic)
        self.assertEqual(n.label, "plain_add")


# --------------------------------------------------------------------------- #
# Tests for `recipe2static`                                                 #
# --------------------------------------------------------------------------- #


class TestRecipe2Static(unittest.TestCase):
    def test_atomic_recipe_returns_atomic(self) -> None:
        recipe = transformers.Transform1toN(2).recipe
        n = constructors.recipe2static("lbl", recipe)
        self.assertIsInstance(n, atomic.Atomic)

    def test_workflow_recipe_returns_macro(self) -> None:
        recipe = _fixtures.macro.flowrep_recipe
        n = constructors.recipe2static("lbl", recipe)
        self.assertIsInstance(n, dag.Macro)

    def test_for_each_recipe_returns_for_each(self) -> None:
        recipe = _fixtures.for_wf.flowrep_recipe.nodes["for_each_0"]
        n = constructors.recipe2static("lbl", recipe)
        self.assertIsInstance(n, flowcontrollers.ForEach)

    def test_if_recipe_returns_if(self) -> None:
        n = constructors.recipe2static("lbl", _if_recipe())
        self.assertIsInstance(n, flowcontrollers.If)

    def test_try_recipe_returns_try(self) -> None:
        n = constructors.recipe2static("lbl", _try_recipe())
        self.assertIsInstance(n, flowcontrollers.Try)

    def test_while_recipe_returns_while(self) -> None:
        n = constructors.recipe2static("lbl", _while_recipe())
        self.assertIsInstance(n, flowcontrollers.While)

    def test_unknown_recipe_type_raises_type_error(self) -> None:
        with self.assertRaises(TypeError) as ctx:
            constructors.recipe2static(label="x", recipe=object())  # type: ignore[arg-type]
        self.assertIn("Unknown recipe type", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
