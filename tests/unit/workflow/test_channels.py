import typing
from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.channels import Channel, InputChannel, OutputChannel
from pyiron_contrib.workflow.type_hinting import (
    type_hint_is_as_or_more_specific_than, valid_value
)


class DummyNode:
    def update(self):
        pass


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestChannels(TestCase):

    def setUp(self) -> None:
        self.ni1 = InputChannel(label="numeric", node=DummyNode(), default=1, type_hint=int | float)
        self.ni2 = InputChannel(label="numeric", node=DummyNode(), default=1, type_hint=int | float)
        self.no = OutputChannel(label="numeric", node=DummyNode(), default=1, type_hint=int | float)

        self.so1 = OutputChannel(label="list", node=DummyNode(), default=["foo"], type_hint=list)
        self.so2 = OutputChannel(label="list", node=DummyNode(), default=["foo"], type_hint=list)

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
            self.assertTrue(valid_value(good, hint))
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

    def test_mutable_defaults(self):
        self.so1.default.append("bar")
        self.assertEqual(
            len(self.so2.default),
            len(self.so1.default) - 1,
            msg="Mutable defaults should avoid sharing between instances"
        )

    def test_connections(self):

        with self.subTest("Test connection reflexivity"):
            self.ni1.connect(self.no)
            self.assertIn(self.no, self.ni1.connections)
            self.assertIn(self.ni1, self.no.connections)

        with self.subTest("Test disconnection"):
            self.ni2.disconnect(self.no)  # Should do nothing
            self.ni1.disconnect(self.no)
            self.assertEqual(
                [], self.ni1.connections, msg="No connections should be left"
            )
            self.assertEqual(
                [],
                self.no.connections,
                msg="Disconnection should also have been reflexive"
            )

        with self.subTest("Test multiple connections"):
            self.no.connect(self.ni1, self.ni2)
            self.assertEqual(2, len(self.no.connections), msg="Should connect to all")

        with self.subTest("Test iteration"):
            self.assertTrue(all([con in self.no.connections for con in self.no]))

    def test_connection_validity_tests(self):
        self.ni1.type_hint = int | float | bool  # Override with a larger set
        self.ni2.type_hint = int  # Override with a smaller set

        with self.assertRaises(TypeError):
            self.ni1.connect("Not a channel at all")

        self.no.connect(self.ni1)
        self.assertIn(
            self.no,
            self.ni1.connections,
            "Input types should be allowed to be a super-set of output types"
        )

        self.no.connect(self.ni2)
        self.assertNotIn(
            self.no,
            self.ni2.connections,
            "Input types should not be allowed to be a sub-set of output types"
        )

        self.so1.connect(self.ni2)
        self.assertNotIn(
            self.so1,
            self.ni2.connections,
            "Totally different types should not allow connections"
        )

    def test_ready(self):
        self.no.value = 1
        self.assertTrue(self.no.ready)
        self.no.value = "Not numeric at all"
        self.assertFalse(self.no.ready)

    def test_update(self):
        self.no.connect(self.ni1, self.ni2)
        self.no.update(42)
        for inp in self.no.connections:
            self.assertEqual(
                self.no.value,
                inp.value,
                msg="Value should have been passed downstream"
            )
