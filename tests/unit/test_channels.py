from unittest import TestCase, skipUnless
from sys import version_info

from pyiron_workflow.channels import (
    InputData, OutputData, InputSignal, OutputSignal, NotData, ChannelConnectionError
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
        self.no_empty = OutputData(label="not_data", node=DummyNode(), type_hint=int | float)

        self.si = InputData(label="list", node=DummyNode(), type_hint=list)
        self.so1 = OutputData(label="list", node=DummyNode(), default=["foo"], type_hint=list)
        self.so2 = OutputData(label="list", node=DummyNode(), default=["foo"], type_hint=list)

        self.unhinted = InputData(label="unhinted", node=DummyNode)

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
            self.assertNotEqual(self.no.value, self.ni1.value)
            self.ni1.fetch()
            self.assertEqual(self.no.value, self.ni1.value)

        with self.subTest("Test disconnection"):
            disconnected = self.ni2.disconnect(self.no)
            self.assertEqual(
                len(disconnected),
                0,
                msg="There were no connections to begin with, nothing should be there"
            )
            disconnected = self.ni1.disconnect(self.no)
            self.assertEqual(
                [], self.ni1.connections, msg="No connections should be left"
            )
            self.assertEqual(
                [],
                self.no.connections,
                msg="Disconnection should also have been reflexive"
            )
            self.assertListEqual(
                disconnected,
                [(self.ni1, self.no)],
                msg="Expected a list of the disconnected pairs."
            )

        with self.subTest("Test multiple connections"):
            self.no.connect(self.ni1, self.ni2)
            self.assertEqual(2, len(self.no.connections), msg="Should connect to all")

        with self.subTest("Test iteration"):
            self.assertTrue(all([con in self.no.connections for con in self.no]))

        with self.subTest("Data should update on fetch"):
            self.ni1.disconnect_all()

            self.no.value = NotData
            self.ni1.value = 1

            self.ni1.connect(self.no_empty)
            self.ni1.connect(self.no)
            self.assertEqual(
                self.ni1.value,
                1,
                msg="Data should not be getting pushed on connection"
            )
            self.ni1.fetch()
            self.assertEqual(
                self.ni1.value,
                1,
                msg="NotData values should not be getting pulled"
            )
            self.no.value = 3
            self.ni1.fetch()
            self.assertEqual(
                self.ni1.value,
                3,
                msg="Data fetch should to first connected value that's actually data,"
                    "in this case skipping over no_empty"
            )
            self.no_empty.value = 4
            self.ni1.fetch()
            self.assertEqual(
                self.ni1.value,
                4,
                msg="As soon as no_empty actually has data, it's position as 0th "
                    "element in the connections list should give it priority"
            )

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

        with self.assertRaises(
            ChannelConnectionError,
            msg="Input types should not be allowed to be a sub-set of output types"
        ):
            self.no.connect(self.ni2)

        with self.assertRaises(
            ChannelConnectionError,
            msg="Totally different type hints should not allow connections"
        ):
            self.so1.connect(self.ni2)

        self.ni2.strict_hints = False
        self.so1.connect(self.ni2)
        self.assertIn(
            self.so1,
            self.ni2.connections,
            "With strict connections turned off, we should allow type-violations"
        )

    def test_copy_connections(self):
        self.ni1.connect(self.no)
        self.ni2.connect(self.no_empty)
        self.ni2.copy_connections(self.ni1)
        self.assertListEqual(
            self.ni2.connections,
            [self.no_empty, *self.ni1.connections],
            msg="Copying should be additive, existing connections should still be there"
        )

        self.ni2.disconnect(*self.ni1.connections)
        self.ni1.connections.append(self.so1)  # Manually include a poorly-typed conn
        with self.assertRaises(
            ChannelConnectionError,
            msg="Should not be able to connect to so1 because of type hint "
                "incompatibility"
        ):
            self.ni2.copy_connections(self.ni1)
        self.assertListEqual(
            self.ni2.connections,
            [self.no_empty],
            msg="On failing, copy should revert the copying channel to its orignial "
                "state"
        )

    def test_value_receiver(self):
        self.ni1.value_receiver = self.ni2
        new_value = 42
        self.assertNotEqual(
            self.ni2.value,
            42,
            msg="Sanity check that we're not starting with our target value",
        )
        self.ni1.value = new_value
        self.assertEqual(
            new_value,
            self.ni2.value,
            msg="Value-linked nodes should automatically get new values"
        )

        with self.assertRaises(
            ValueError,
            msg="Linking should obey type hint requirements",
        ):
            self.ni1.value_receiver = self.si

        self.si.strict_hints = False
        self.ni1.value_receiver = self.si  # Should work fine if the receiver is not
        # strictly checking hints

        self.ni1.value_receiver = self.unhinted
        self.unhinted.value_receiver = self.ni2
        # Should work fine if either is unhinted

    def test_copy_value(self):
        self.ni1.value = 2
        self.ni2.copy_value(self.ni1)
        self.assertEqual(
            self.ni2.value,
            self.ni1.value,
            msg="Should be able to copy values matching type hints"
        )

        self.ni2.copy_value(self.no_empty)
        self.assertIs(
            self.ni2.value,
            NotData,
            msg="Should be able to copy values that are not-data"
        )

        with self.assertRaises(
            TypeError,
            msg="Should not be able to copy values of the wrong type"
        ):
            self.ni2.copy_value(self.so1)

        self.ni2.type_hint = None
        self.ni2.copy_value(self.ni1)
        self.assertEqual(
            self.ni2.value,
            self.ni1.value,
            msg="Should be able to copy any data if we have no type hint"
        )
        self.ni2.copy_value(self.so1)
        self.assertEqual(
            self.ni2.value,
            self.so1.value,
            msg="Should be able to copy any data if we have no type hint"
        )
        self.ni2.copy_value(self.no_empty)
        self.assertEqual(
            self.ni2.value,
            NotData,
            msg="Should be able to copy not-data if we have no type hint"
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

        self.ni1._value = "Not numeric at all"  # Bypass type checking
        self.assertFalse(self.ni1.ready)

    def test_input_coupling(self):
        self.assertNotEqual(
            self.ni2.value,
            2,
            msg="Ensure we start from a setup that the next test is meaningful"
        )
        self.ni1.value = 2
        self.ni1.value_receiver = self.ni2
        self.assertEqual(
            self.ni2.value,
            2,
            msg="Coupled value should get updated on coupling"
        )
        self.ni1.value = 3
        self.assertEqual(
            self.ni2.value,
            3,
            msg="Coupled value should get updated after partner update"
        )
        self.ni2.value = 4
        self.assertEqual(
            self.ni1.value,
            3,
            msg="Coupling is uni-directional, the partner should not push values back"
        )

        with self.assertRaises(
            TypeError,
            msg="Only input data channels are valid partners"
        ):
            self.ni1.value_receiver = self.no

        with self.assertRaises(
            ValueError,
            msg="Must not couple to self to avoid infinite recursion"
        ):
            self.ni1.value_receiver = self.ni1


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

        with self.subTest("Test syntactic sugar"):
            self.out.disconnect_all()
            self.out > self.inp
            self.assertIn(self.out, self.inp.connections)

    def test_calls(self):
        self.out.connect(self.inp)
        self.out()
        self.assertListEqual(self.inp.node.foo, [0, 1])
        self.inp()
        self.assertListEqual(self.inp.node.foo, [0, 1, 2])
