import typing
from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.type_hinting import (
    type_hint_is_as_or_more_specific_than, valid_value
)


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestTypeHinting(TestCase):
    def test_value_validation(self):
        class Foo:
            pass

        class Bar:
            def __call__(self):
                return None

        for hint, good, bad in (
                (int | float, 1, "foo"),
                (typing.Union[int, float], 2.0, "bar"),
                (typing.Literal[1, 2], 2, 3),
                (typing.Literal[1, 2], 1, "baz"),
                (Foo, Foo(), Foo),
                (typing.Type[Bar], Bar, Bar()),
                # (callable, Bar(), Foo()),  # Misses the bad!
                # Can't hint args and returns without typing.Callable anyhow, so that's
                # what people should be using regardless
                (typing.Callable, Bar(), Foo()),
                (tuple[int, float], (1, 1.1), ("fo", 0)),
                (dict[str, int], {'a': 1}, {'a': 'b'}),
        ):
            with self.subTest(msg=f"Good {good} vs hint {hint}"):
                self.assertTrue(valid_value(good, hint))
            with self.subTest(msg=f"Bad {bad} vs hint {hint}"):
                self.assertFalse(valid_value(bad, hint))

    def test_hint_comparisons(self):
        # Standard types and typing types should be interoperable
        # tuple, dict, and typing.Callable care about the exact matching of args
        # Everyone else just needs to have args be a subset (e.g. typing.Literal)

        for target, reference, is_more_specific in [
            (int, int | float, True),
            (int | float, int, False),
            (typing.Literal[1, 2], typing.Literal[1, 2, 3], True),
            (typing.Literal[1, 2, 3], typing.Literal[1, 2], False),
            (tuple[str, int], typing.Tuple[str, int], True),
            (typing.Tuple[int, str], tuple[str, int], False),
            (tuple[str, int], typing.Tuple[str, int | float], True),
            (typing.Tuple[str, int | float], tuple[str, int], False),
            (tuple[str, int], typing.Tuple, True),
            (tuple[str, int], tuple[str, int, float], False),
            (list[int], typing.List[int], True),
            (typing.List, list[int], False),
            (dict[str, int], typing.Dict[str, int], True),
            (dict[int, str], typing.Dict[str, int], False),
            (typing.Callable[[int, float], None], typing.Callable, True),
            (
                    typing.Callable[[int, float], None],
                    typing.Callable[[float, int], None],
                    False
            ),
            (
                    typing.Callable[[int, float], float],
                    typing.Callable[[int, float], float | str],
                    True
            ),
            (
                    typing.Callable[[int, float, str], float],
                    typing.Callable[[int, float], float],
                    False
            ),
        ]:
            with self.subTest(
                    target=target, reference=reference, expected=is_more_specific
            ):
                self.assertEqual(
                    type_hint_is_as_or_more_specific_than(target, reference),
                    is_more_specific
                )