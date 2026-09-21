from __future__ import annotations

import inspect
import unittest
import warnings

from pyiron_workflow import compatibility
from pyiron_workflow.compatibility import decorators

REMOVAL_VERSION = "0.21.0"


def plain_add(x, y):
    """A module-level function, so the factory is not built from a `<locals>` scope."""
    z = x + y
    return z


def plain_macro(self, x, y):
    """A legacy-style macro body; module-level for the same reason."""
    self.s = x
    return self.s


def _warnings_from(apply):
    """Run ``apply()`` and hand back whatever warnings it emitted."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        apply()
    return caught


class TestDecoratorDeprecationWarnings(unittest.TestCase):
    """
    Both legacy decorators are deprecated, and both of their multiple-dispatch forms
    (bare, and called with output labels) have to say so.
    """

    def test_as_function_node_with_labels_warns(self) -> None:
        caught = _warnings_from(lambda: compatibility.as_function_node("z")(plain_add))
        self.assertEqual(1, len(caught))
        self.assertTrue(issubclass(caught[0].category, DeprecationWarning))

    def test_as_function_node_bare_warns(self) -> None:
        caught = _warnings_from(lambda: compatibility.as_function_node(plain_add))
        self.assertEqual(1, len(caught))
        self.assertTrue(issubclass(caught[0].category, DeprecationWarning))

    def test_as_macro_node_with_labels_warns(self) -> None:
        caught = _warnings_from(lambda: compatibility.as_macro_node("out")(plain_macro))
        self.assertEqual(1, len(caught))
        self.assertTrue(issubclass(caught[0].category, DeprecationWarning))

    def test_as_macro_node_bare_warns(self) -> None:
        caught = _warnings_from(lambda: compatibility.as_macro_node(plain_macro))
        self.assertEqual(1, len(caught))
        self.assertTrue(issubclass(caught[0].category, DeprecationWarning))

    def test_function_node_message_names_the_replacement(self) -> None:
        message = str(
            _warnings_from(lambda: compatibility.as_function_node("z")(plain_add))[
                0
            ].message
        )
        self.assertIn("as_function_node", message)
        self.assertIn("is deprecated", message)
        self.assertIn("flowrep.atomic", message)
        self.assertIn(REMOVAL_VERSION, message)

    def test_macro_node_message_names_the_replacement(self) -> None:
        message = str(
            _warnings_from(lambda: compatibility.as_macro_node("out")(plain_macro))[
                0
            ].message
        )
        self.assertIn("as_macro_node", message)
        self.assertIn("is deprecated", message)
        self.assertIn("flowrep.workflow", message)
        self.assertIn(REMOVAL_VERSION, message)

    def test_message_names_the_decorated_definition(self) -> None:
        # The whole point of warning from inside the closure: say *what* was decorated,
        # so a user with many legacy definitions knows which one to go fix.
        for apply, func in (
            (lambda: compatibility.as_function_node("z")(plain_add), plain_add),
            (lambda: compatibility.as_macro_node("out")(plain_macro), plain_macro),
        ):
            with self.subTest(func=func.__qualname__):
                message = str(_warnings_from(apply)[0].message)
                self.assertIn(func.__qualname__, message)
                self.assertIn(func.__module__, message)

    def test_undecorated_call_does_not_warn(self) -> None:
        # The warning belongs to applying the decorator, not to building it.
        self.assertEqual(
            [], _warnings_from(lambda: compatibility.as_function_node("z"))
        )
        self.assertEqual([], _warnings_from(lambda: compatibility.as_macro_node("out")))

    def test_decoration_still_produces_a_factory(self) -> None:
        # Deprecated, but not broken: the warning must not disturb the dispatch.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            labelled = compatibility.as_function_node("z")(plain_add)
            bare = compatibility.as_function_node(plain_add)
            macro = compatibility.as_macro_node("out")(plain_macro)
        self.assertIsInstance(labelled, decorators._AtomicFactory)
        self.assertIsInstance(bare, decorators._AtomicFactory)
        self.assertIsInstance(macro, decorators._MacroFactory)

    def test_kwargs_error_is_unaffected(self) -> None:
        for decorator in (
            compatibility.as_function_node,
            compatibility.as_macro_node,
        ):
            with self.subTest(decorator=decorator):
                with self.assertRaises(ValueError) as ctx:
                    decorator("z", forbid_locals=False)
                self.assertIn("are not meaningful", str(ctx.exception))


class TestDeprecationWarningAttribution(unittest.TestCase):
    """
    A deprecation warning is only actionable if it points at the decorated definition,
    not at pyiron_workflow's own dispatch plumbing.
    """

    def _assert_blames(self, apply, func) -> None:
        caught = _warnings_from(apply)
        self.assertEqual(inspect.getsourcefile(func), caught[0].filename)
        self.assertEqual(func.__code__.co_firstlineno, caught[0].lineno)

    def test_function_node_with_labels_blames_the_definition(self) -> None:
        self._assert_blames(
            lambda: compatibility.as_function_node("z")(plain_add), plain_add
        )

    def test_function_node_bare_blames_the_definition(self) -> None:
        # The bare form routes through an extra dispatch frame; the attribution must
        # not care.
        self._assert_blames(
            lambda: compatibility.as_function_node(plain_add), plain_add
        )

    def test_macro_node_with_labels_blames_the_definition(self) -> None:
        self._assert_blames(
            lambda: compatibility.as_macro_node("out")(plain_macro), plain_macro
        )

    def test_macro_node_bare_blames_the_definition(self) -> None:
        self._assert_blames(
            lambda: compatibility.as_macro_node(plain_macro), plain_macro
        )


if __name__ == "__main__":
    unittest.main()
