import typing
import unittest

from pint import UnitRegistry

from pyiron_workflow.type_hinting import (
    _get_type_hints,
    type_hint_is_as_or_more_specific_than,
    type_hint_to_tuple,
    valid_value,
)


class TestTypeHinting(unittest.TestCase):
    def test_value_validation(self):
        class Foo:
            pass

        class Bar:
            def __call__(self):
                return None

        ureg = UnitRegistry()

        for hint, good, bad in (
            (int | float, 1, "foo"),
            (int | float, 2.0, "bar"),
            (typing.Literal[1, 2], 2, 3),
            (typing.Literal[1, 2], 1, "baz"),
            (Foo, Foo(), Foo),
            (type[Bar], Bar, Bar()),
            (typing.Callable, Bar(), Foo()),
            (tuple[int, float], (1, 1.1), ("fo", 0)),
            (dict[str, int], {"a": 1}, {"a": "b"}),
            (int, 1 * ureg.seconds, 1.0 * ureg.seconds),  # Disregard unit, look@type
        ):
            with self.subTest(msg=f"Good {good} vs hint {hint}"):
                self.assertTrue(valid_value(good, hint))
            with self.subTest(msg=f"Bad {bad} vs hint {hint}"):
                self.assertFalse(valid_value(bad, hint))

        with self.subTest(msg="Test strictness"):
            self.assertTrue(
                valid_value(Bar(), callable, strict_callables=False),
                msg="Sanity: Bar implements __call__ and should always appear callable",
            )
            self.assertTrue(
                valid_value(Bar(), callable, strict_callables=True),
                msg="Sanity: Bar implements __call__ and should always appear callable",
            )

            self.assertTrue(
                valid_value(Foo(), callable, strict_callables=False),
                msg="typeguard is relaxed about callable, and doesn't care that Foo "
                "fails to implement __call__",
            )
            self.assertFalse(
                valid_value(Foo(), callable, strict_callables=True),
                msg="typeguard is stringent about Callable, and notices that Foo fails "
                "to implement __call__",
            )

    def test_hint_comparisons(self):
        # Standard types and typing types should be interoperable
        # tuple, dict, and typing.Callable care about the exact matching of args
        # Everyone else just needs to have args be a subset (e.g. typing.Literal)

        for target, reference, is_more_specific in [
            (int, int | float, True),
            (int | float, int, False),
            (typing.Literal[1, 2], typing.Literal[1, 2, 3], True),
            (typing.Literal[1, 2, 3], typing.Literal[1, 2], False),
            (tuple[str, int], tuple[str, int], True),
            (tuple[int, str], tuple[str, int], False),
            (tuple[str, int], tuple[str, int | float], True),
            (tuple[str, int | float], tuple[str, int], False),
            (tuple[str, int], tuple, True),
            (tuple[str, int], tuple[str, int, float], False),
            (list[int], list[int], True),
            (list, list[int], False),
            (list[int], list, True),
            (dict[str, int], dict[str, int], True),
            (dict[int, str], dict[str, int], False),
            (typing.Callable[[int, float], None], typing.Callable, True),
            (
                typing.Callable[[int, float], None],
                typing.Callable[[float, int], None],
                False,
            ),
            (
                typing.Callable[[int, float], float],
                typing.Callable[[int, float], float | str],
                True,
            ),
            (
                typing.Callable[[int, float, str], float],
                typing.Callable[[int, float], float],
                False,
            ),
            (
                typing.Annotated[int, "foo"],
                int,
                True,
            ),
            (
                int,
                typing.Annotated[int, "foo"],
                True,
            ),
            (
                typing.Optional[typing.Dict[str, int]],  # noqa: UP006, UP007
                dict[str, int] | None,
                True,  # Old- and new-styles should be equivalent
            ),
            (
                dict[str, int] | None,
                typing.Optional[typing.Dict[str, int]],  # noqa: UP006, UP007
                True,  # Thus, we expect the reverse direction to work to prove identity
            ),
        ]:
            with self.subTest(
                target=target, reference=reference, expected=is_more_specific
            ):
                self.assertEqual(
                    type_hint_is_as_or_more_specific_than(target, reference),
                    is_more_specific,
                    msg=f"{target} is {'not ' if not is_more_specific else ''}more specific than {reference}",
                )

    def test_get_type_hints(self):
        for hint, origin in [
            (int | float, type(int | float)),
            (typing.Annotated[int | float, "foo"], type(int | float)),
            (int, None),
            (typing.Annotated[int, "foo"], None),
            (typing.Annotated[list[int], "foo"], list),
            (list[int], list),
        ]:
            with self.subTest(hint=hint, origin=origin):
                self.assertEqual(_get_type_hints(hint)[0], origin)

    def test_type_hint_to_tuple(self):
        for hint, tuple_ in (
            (dict[str, int] | None, (dict[str, int], type(None))),
            (
                typing.Dict[str, int] | None,
                (typing.Dict[str, int], type(None)),
            ),  # noqa: UP006
            (
                typing.Optional[dict[str, int]],
                (dict[str, int], type(None)),
            ),  # noqa: UP007
            (
                typing.Optional[typing.Dict[str, int]],  # noqa: UP006, UP007
                (typing.Dict[str, int], type(None)),  # noqa: UP006
            ),
        ):
            self.assertEqual(
                type_hint_to_tuple(hint),
                tuple_,
                msg="Old- and new-style hints should both be split into a tuple",
            )


if __name__ == "__main__":
    unittest.main()
