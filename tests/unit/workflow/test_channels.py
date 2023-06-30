from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_contrib.workflow.channels import (
    InputData, OutputData, InputSignal, OutputSignal, NotData
)


class DummyNode:
    def __init__(self):
        self.foo = [0]
        self.running = False
        self.label = "node_label"

    def update(self):
        self.foo.append(self.foo[-1] + 1)


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestDataChannels(TestCase):

    def setUp(self) -> None:
        self.ni1 = InputData(label="numeric", node=DummyNode(), default=1, type_hint=int | float)
        self.ni2 = InputData(label="numeric", node=DummyNode(), default=1, type_hint=int | float)
        self.no = OutputData(label="numeric", node=DummyNode(), default=0, type_hint=int | float)

        self.so1 = OutputData(label="list", node=DummyNode(), default=["foo"], type_hint=list)
        self.so2 = OutputData(label="list", node=DummyNode(), default=["foo"], type_hint=list)

    def test_mutable_defaults(self):
        self.so1.default.append("bar")
        self.assertEqual(
            len(self.so2.default),
            len(self.so1.default) - 1,
            msg="Mutable defaults should avoid sharing between instances"
        )

    def test_connections(self):

        with self.subTest("Test connection reflexivity and value updating"):
            self.assertEqual(self.no.value, 0)
            self.ni1.connect(self.no)
            self.assertIn(self.no, self.ni1.connections)
            self.assertIn(self.ni1, self.no.connections)
            self.assertEqual(self.no.value, self.ni1.value)

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

        self.ni2.strict_connections = False
        self.so1.connect(self.ni2)
        self.assertIn(
            self.so1,
            self.ni2.connections,
            "With strict connections turned off, we should allow type-violations"
        )

    def test_ready(self):
        with self.subTest("Test defaults and not-data"):
            without_default = InputData(label="without_default", node=DummyNode())
            self.assertIs(
                without_default.value,
                NotData,
                msg=f"Without a default, spec is to have a NotData value but got "
                    f"{type(without_default.value)}"
            )
            self.assertFalse(
                without_default.ready,
                msg="Even without type hints, readiness should be false when the value"
                    "is NotData"
            )

        self.ni1.value = 1
        self.assertTrue(self.ni1.ready)

        with self.subTest("Test the waiting mechanism"):
            self.ni1.wait_for_update()
            self.assertTrue(self.ni1.waiting_for_update)
            self.assertFalse(self.ni1.ready)
            self.ni1.update(2)
            self.assertFalse(self.ni1.waiting_for_update)
            self.assertTrue(self.ni1.ready)

        self.ni1.value = "Not numeric at all"
        self.assertFalse(self.ni1.ready)

    def test_update(self):
        self.no.connect(self.ni1, self.ni2)
        self.no.update(42)
        for inp in self.no.connections:
            self.assertEqual(
                self.no.value,
                inp.value,
                msg="Value should have been passed downstream"
            )

        self.ni1.node.running = True
        with self.assertRaises(RuntimeError):
            self.no.update(42)


class TestSignalChannels(TestCase):
    def setUp(self) -> None:
        node = DummyNode()
        self.inp = InputSignal(label="inp", node=node, callback=node.update)
        self.out = OutputSignal(label="out", node=DummyNode())

    def test_connections(self):
        with self.subTest("Good connection"):
            self.inp.connect(self.out)
            self.assertEqual(self.inp.connections, [self.out])
            self.assertEqual(self.out.connections, [self.inp])

        with self.subTest("Ignore repeated connection"):
            self.out.connect(self.inp)
            self.assertEqual(len(self.inp), 1)
            self.assertEqual(len(self.out), 1)

        with self.subTest("Check disconnection"):
            self.out.disconnect_all()
            self.assertEqual(len(self.inp), 0)
            self.assertEqual(len(self.out), 0)

        with self.subTest("No connections to non-SignalChannels"):
            bad = InputData(label="numeric", node=DummyNode(), default=1, type_hint=int)
            with self.assertRaises(TypeError):
                self.inp.connect(bad)

    def test_calls(self):
        self.out.connect(self.inp)
        self.out()
        self.assertListEqual(self.inp.node.foo, [0, 1])
        self.inp()
        self.assertListEqual(self.inp.node.foo, [0, 1, 2])
