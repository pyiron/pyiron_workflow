from __future__ import annotations

import importlib
import re
import unittest

import pyiron_workflow
from pyiron_workflow import api
from pyiron_workflow.compatibility import messages, stubs

# The complete public surface of pyiron_workflow-0.17.0, transcribed from the
# `pyiron_workflow-0.17.0` tag. These literals are the point of the table: they pin
# what a user upgrading from the last pre-flowrep release can possibly type, so that
# a name can never silently fall out of (or be typo'd into) the stub tables.
LEGACY_TOP_LEVEL = frozenset(
    {
        "NodeSlurmExecutor",
        "Workflow",
        "as_dataclass_node",
        "as_function_node",
        "as_macro_node",
        "dataclass_node",
        "for_node",
        "function_node",
        "macro_node",
        "std",
        "to_function_node",
        "while_node",
    }
)

# `_find_nodes` is deliberately omitted: 0.17.0's api.py flags it as not-formally-API.
LEGACY_API = frozenset(
    {
        "CloudpickleProcessPoolExecutor",
        "FailedChildError",
        "For",
        "Function",
        "Macro",
        "NOT_DATA",
        "NodeSlurmExecutor",
        "PickleStorage",
        "StorageInterface",
        "TypeNotFoundError",
        "While",
        "Workflow",
        "as_dataclass_node",
        "as_function_node",
        "as_macro_node",
        "available_backends",
        "dataclass_node",
        "for_node",
        "for_node_factory",
        "function_node",
        "inputs_to_dataframe",
        "inputs_to_dict",
        "inputs_to_list",
        "list_to_outputs",
        "logger",
        "macro_node",
        "std",
        "to_function_node",
        "while_node",
        "while_node_factory",
    }
)

# Legacy names that survived the rewrite at the same import path, so importing them
# must keep working instead of raising.
STILL_LIVE = frozenset({"Workflow", "as_function_node", "as_macro_node"})

_SCOPES = {
    "pyiron_workflow": (pyiron_workflow, stubs._TopLevel, LEGACY_TOP_LEVEL),
    "pyiron_workflow.api": (api, stubs._ApiSubmodule, LEGACY_API),
}

_BACKTICK_SPAN = re.compile(r"`([^`]*)`")
_DOTTED_SYMBOL = re.compile(r"\b(?:flowrep|pyiron_workflow)(?:\.[A-Za-z_]\w*)+")


def _addenda(scope: type) -> dict[str, str]:
    """Every guidance string the scope can attach to an error, keyed by legacy name."""
    return {**scope.RELOCATED, **scope.NOT_AVAILABLE}


def _resolve(dotted: str) -> object:
    """Walk a dotted path, falling back to importing intermediate submodules."""
    parts = dotted.split(".")
    obj = importlib.import_module(parts[0])
    for i, part in enumerate(parts[1:], start=2):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            obj = importlib.import_module(".".join(parts[:i]))
    return obj


class TestLegacySurfaceIsCovered(unittest.TestCase):
    """Every 0.17.0 name is either still live or explained; nothing else is listed."""

    def test_every_legacy_name_is_handled(self) -> None:
        for module_name, (module, _scope, legacy) in _SCOPES.items():
            for name in sorted(legacy):
                with self.subTest(module=module_name, name=name):
                    if name in STILL_LIVE:
                        self.assertIsNotNone(getattr(module, name))
                    else:
                        with self.assertRaises(stubs.RemovedFeatureError):
                            getattr(module, name)

    def test_no_stub_entry_invents_a_name(self) -> None:
        # A key that was never in 0.17.0 is dead weight: no user can trigger it, so it
        # is almost certainly a typo hiding a name that *is* reachable.
        for module_name, (_, scope, legacy) in _SCOPES.items():
            with self.subTest(module=module_name):
                self.assertSetEqual(set(), _addenda(scope).keys() - legacy)

    def test_live_names_are_not_also_stubbed(self) -> None:
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            with self.subTest(module=module_name):
                self.assertSetEqual(set(), _addenda(scope).keys() & STILL_LIVE)

    def test_relocated_and_unavailable_are_disjoint(self) -> None:
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            with self.subTest(module=module_name):
                self.assertSetEqual(
                    set(), scope.RELOCATED.keys() & scope.NOT_AVAILABLE.keys()
                )


class TestRemovedFeatureMessages(unittest.TestCase):
    """The raised error has to carry the name, the guidance, and a way forward."""

    def test_message_contains_name_and_guidance(self) -> None:
        for module_name, (module, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                with self.subTest(module=module_name, name=name):
                    with self.assertRaises(stubs.RemovedFeatureError) as ctx:
                        getattr(module, name)
                    message = str(ctx.exception)
                    self.assertIn(name, message)
                    self.assertIn(addendum, message)
                    self.assertIn(messages.DOWNGRADE, message)

    def test_exception_reports_the_name_attribute(self) -> None:
        # `ImportError.name` is what tooling reads to explain a failed import.
        for module_name, (module, scope, _legacy) in _SCOPES.items():
            for name in _addenda(scope):
                with self.subTest(module=module_name, name=name):
                    with self.assertRaises(stubs.RemovedFeatureError) as ctx:
                        getattr(module, name)
                    self.assertEqual(name, ctx.exception.name)

    def test_removed_names_point_at_the_flowrep_docs(self) -> None:
        for module_name, (module, scope, _legacy) in _SCOPES.items():
            for name in scope.NOT_AVAILABLE:
                with self.subTest(module=module_name, name=name):
                    with self.assertRaises(stubs.RemovedFeatureError) as ctx:
                        getattr(module, name)
                    self.assertIn(stubs._USER_GUIDE, str(ctx.exception))

    def test_relocated_names_are_not_described_as_removed(self) -> None:
        for module_name, (module, scope, _legacy) in _SCOPES.items():
            for name in scope.RELOCATED:
                with self.subTest(module=module_name, name=name):
                    with self.assertRaises(stubs.RemovedFeatureError) as ctx:
                        getattr(module, name)
                    self.assertNotIn("was removed", str(ctx.exception))

    def test_error_is_an_import_error(self) -> None:
        # `from pyiron_workflow import for_node` should read as an import failure.
        self.assertTrue(issubclass(stubs.RemovedFeatureError, ImportError))

    def test_from_import_form_raises(self) -> None:
        with self.assertRaises(stubs.RemovedFeatureError):
            from pyiron_workflow import for_node  # noqa: F401, PLC0415


class TestMessageHygiene(unittest.TestCase):
    """Cheap checks for the failure modes of assembling prose from string literals."""

    def test_no_doubled_whitespace(self) -> None:
        # Catches a missing space between adjacent implicitly-concatenated literals'
        # sibling: the doubled one.
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                with self.subTest(module=module_name, name=name):
                    self.assertNotIn("  ", addendum)

    def test_no_doubled_punctuation(self) -> None:
        # Catches a literal that ends in "." being concatenated onto another sentence.
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                with self.subTest(module=module_name, name=name):
                    self.assertNotIn("..", addendum)
                    self.assertNotIn(" .", addendum)

    def test_code_spans_are_closed(self) -> None:
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                with self.subTest(module=module_name, name=name):
                    self.assertEqual(
                        0,
                        addendum.count("`") % 2,
                        msg="An odd number of backticks means an unclosed code span.",
                    )

    def test_guidance_ends_in_a_sentence(self) -> None:
        # A missing final "." runs the guidance into whatever the stub appends next.
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                with self.subTest(module=module_name, name=name):
                    self.assertTrue(addendum.endswith("."), msg=repr(addendum[-30:]))


class TestReferencedSymbolsResolve(unittest.TestCase):
    """
    Every `flowrep.x.y` / `pyiron_workflow.x.y` the guidance tells a user to type must
    actually exist, so the messages cannot rot as flowrep evolves.
    """

    def test_backticked_symbols_exist(self) -> None:
        for module_name, (_, scope, _legacy) in _SCOPES.items():
            for name, addendum in _addenda(scope).items():
                for span in _BACKTICK_SPAN.findall(addendum):
                    for dotted in _DOTTED_SYMBOL.findall(span):
                        with self.subTest(module=module_name, name=name, symbol=dotted):
                            try:
                                _resolve(dotted)
                            except Exception as error:  # noqa: BLE001
                                self.fail(f"{dotted!r} does not resolve: {error}")

    def test_the_check_has_something_to_chew_on(self) -> None:
        # Guard against the regex silently matching nothing and the test above passing
        # vacuously.
        found = {
            dotted
            for _, scope, _legacy in _SCOPES.values()
            for addendum in _addenda(scope).values()
            for span in _BACKTICK_SPAN.findall(addendum)
            for dotted in _DOTTED_SYMBOL.findall(span)
        }
        self.assertIn("flowrep.schemas.ForEachRecipe", found)
        self.assertIn("pyiron_workflow.tools.NodeSlurmExecutor", found)


class TestUnknownAttributes(unittest.TestCase):
    """Names that were never part of 0.17.0 must behave like ordinary typos."""

    def test_unknown_name_is_a_plain_attribute_error(self) -> None:
        for module_name, (module, _scope, _legacy) in _SCOPES.items():
            with self.subTest(module=module_name):
                with self.assertRaises(AttributeError) as ctx:
                    _ = module.not_a_real_pyiron_workflow_name
                self.assertNotIsInstance(ctx.exception, stubs.RemovedFeatureError)
                self.assertIn(module_name, str(ctx.exception))
                self.assertIn("not_a_real_pyiron_workflow_name", str(ctx.exception))

    def test_unknown_name_is_invisible_to_hasattr(self) -> None:
        for module_name, (module, _scope, _legacy) in _SCOPES.items():
            with self.subTest(module=module_name):
                self.assertFalse(hasattr(module, "not_a_real_pyiron_workflow_name"))


if __name__ == "__main__":
    unittest.main()
