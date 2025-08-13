from __future__ import annotations

import unittest

from pyiron_workflow.channels import (
    NOT_DATA,
    AccumulatingInputSignal,
    BadCallbackError,
    Channel,
    ChannelConnectionError,
    ConjugateType,
    InputData,
    InputLockedError,
    InputSignal,
    OutputData,
    OutputSignal,
    TooManyConnectionsError,
)


class DummyParent:
    def __init__(self, label):
        self.label = label
        self.children = []

    @property
    def full_label(self):
        return self.label

    def add_child(self, child):
        self.children.append(child)

    def remove_child(self, child):
        self.children.remove(child)


class DummyOwner:
    def __init__(self, parent: DummyParent | None, label: str):
        self.foo = [0]
        self.locked = False
        self.label = label
        self.parent = parent

    @property
    def full_label(self):
        return self.label

    def update(self):
        self.foo.append(self.foo[-1] + 1)

    def data_input_locked(self):
        return self.locked


class DummyChannel(Channel[ConjugateType]):
    """Just to de-abstract the base class"""

    def __str__(self):
        return "non-abstract input"

    def _valid_connection(self, other: object) -> bool:
        return isinstance(other, self.connection_conjugate())


class InputChannel(DummyChannel["OutputChannel"]):
    @classmethod
    def connection_conjugate(cls) -> type[OutputChannel]:
        return OutputChannel


class OutputChannel(DummyChannel["InputChannel"]):
    @classmethod
    def connection_conjugate(cls) -> type[InputChannel]:
        return InputChannel


class TestChannel(unittest.TestCase):
    def setUp(self) -> None:
        self.p = DummyParent("parent_label")
        self.inp = InputChannel("inp", DummyOwner(self.p, "has_inp"))
        self.out = OutputChannel("out", DummyOwner(self.p, "has_out"))
        self.out2 = OutputChannel("out2", DummyOwner(self.p, "has_out2"))

    def test_connection_validity(self):
        with self.assertRaises(TypeError, msg="Can't connect to non-channels"):
            self.inp.connect("not an owner")

        with self.assertRaises(
            TypeError, msg="Can't connect to channels that are not the partner type"
        ):
            self.inp.connect(
                InputChannel("also_input", DummyOwner(self.p, "has_also_input"))
            )

        self.inp.connect(self.out)
        # A conjugate pair should work fine

    def test_connection_reflexivity(self):
        self.inp.connect(self.out)

        self.assertIs(
            self.inp.connections[0],
            self.out,
            msg="Connecting a conjugate pair should work fine",
        )
        self.assertIs(
            self.out.connections[0], self.inp, msg="Promised connection to be reflexive"
        )
        self.out.disconnect_all()
        self.assertListEqual(
            [], self.inp.connections, msg="Promised disconnection to be reflexive too"
        )

        self.out.connect(self.inp)
        self.assertIs(
            self.inp.connections[0],
            self.out,
            msg="Connecting should work in either direction",
        )

    def test_connect_and_disconnect(self):
        self.inp.connect(self.out, self.out2)
        # Should allow multiple (dis)connections at once
        disconnected = self.inp.disconnect(self.out2, self.out)
        self.assertListEqual(
            [(self.inp, self.out2), (self.inp, self.out)],
            disconnected,
            msg="Broken connection pairs should be returned in the order they were "
            "broken",
        )

    def test_iterability(self):
        self.inp.connect(self.out)
        self.out2.connect(self.inp)
        for i, conn in enumerate(self.inp):
            self.assertIs(
                self.inp.connections[i],
                conn,
                msg="Promised channels to be iterable over connections",
            )


class TestDataChannels(unittest.TestCase):
    def setUp(self) -> None:
        self.p = DummyParent("parent_label")
        self.ni1 = InputData(
            label="numeric",
            owner=DummyOwner(self.p, "has_numeric1"),
            default=1,
            type_hint=int | float,
        )
        self.ni2 = InputData(
            label="numeric",
            owner=DummyOwner(self.p, "has_numeric2"),
            default=1,
            type_hint=int | float,
        )
        self.no = OutputData(
            label="numeric",
            owner=DummyOwner(self.p, "has_numeric3"),
            default=0,
            type_hint=int | float,
        )
        self.no_empty = OutputData(
            label="not_data",
            owner=DummyOwner(self.p, "has_node_data"),
            type_hint=int | float,
        )

        self.si = InputData(
            label="list", owner=DummyOwner(self.p, "has_listi"), type_hint=list
        )
        self.so1 = OutputData(
            label="list",
            owner=DummyOwner(self.p, "has_listo"),
            default=["foo"],
            type_hint=list,
        )

    def test_mutable_defaults(self):
        so2 = OutputData(
            label="list",
            owner=DummyOwner(self.p, "has_list"),
            default=["foo"],
            type_hint=list,
        )
        self.so1.default.append("bar")
        self.assertEqual(
            len(so2.default),
            len(self.so1.default) - 1,
            msg="Mutable defaults should avoid sharing between different instances",
        )

    def test_fetch(self):
        self.no.value = NOT_DATA
        self.ni1.value = 1

        self.ni1.connect(self.no)

        self.assertEqual(
            self.ni1.value, 1, msg="Data should not be getting pushed on connection"
        )

        self.ni1.fetch()
        self.assertEqual(
            self.ni1.value,
            1,
            msg="NOT_DATA values should not be getting pulled, so no update expected",
        )

        self.no.value = 3
        self.ni1.fetch()
        self.assertEqual(
            self.ni1.value,
            3,
            msg="Data fetch should retrieve available data",
        )

    def test_connection_validity(self):
        self.ni1.type_hint = int | float | bool  # Override with a larger set
        self.ni2.type_hint = int  # Override with a smaller set

        self.no.connect(self.ni1)
        self.assertIn(
            self.no,
            self.ni1.connections,
            msg="Input types should be allowed to be a super-set of output types",
        )

        with self.assertRaises(
            ChannelConnectionError,
            msg="Input types should not be allowed to be a sub-set of output types",
        ):
            self.no.connect(self.ni2)

        with self.assertRaises(
            ChannelConnectionError,
            msg="Totally different type hints should not allow connections",
        ):
            self.so1.connect(self.ni2)

        self.ni2.strict_hints = False
        self.so1.connect(self.ni2)
        self.assertIn(
            self.so1,
            self.ni2.connections,
            msg="With strict connections turned off, we should allow type-violations",
        )

        with self.assertRaises(
            TooManyConnectionsError,
            msg="Only one input connection at a time is allowed",
        ):
            self.ni2.connect(self.no)

    def test_moving_connections(self):
        self.ni1.connect(self.no)
        self.ni2.connect(self.no_empty)

        self.ni2.move_connections(self.ni1)
        self.assertFalse(self.ni1.connected)
        self.assertListEqual(
            self.ni2.connections,
            [self.no],
            msg="Copying should hard-transfer the connection",
        )

    def test_moving_connections_failure(self):
        self.ni1.connect(self.no)
        self.si.connect(self.so1)

        with self.assertRaises(
            ChannelConnectionError,
            msg="Should not be able to connect to no because of type hint "
            "incompatibility",
        ):
            self.si.move_connections(self.ni1)
        self.assertListEqual(
            self.ni1.connections,
            [self.no],
            msg="On failing, copy should revert the copied channel to its orignial "
            "state",
        )
        self.assertListEqual(
            self.si.connections,
            [self.so1],
            msg="On failing, copy should revert the copying channel to its orignial "
            "state",
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
            msg="Value-linked owners should automatically get new values",
        )

        self.ni2.value = 3
        self.assertEqual(
            self.ni1.value,
            new_value,
            msg="Coupling is uni-directional, the partner should not push values back",
        )

        with self.assertRaises(
            TypeError, msg="Only data channels of the same class are valid partners"
        ):
            self.ni1.value_receiver = self.no

        with self.assertRaises(
            ValueError, msg="Must not couple to self to avoid infinite recursion"
        ):
            self.ni1.value_receiver = self.ni1

        with self.assertRaises(
            ValueError,
            msg="Linking should obey type hint requirements",
        ):
            self.ni1.value_receiver = self.si

        with self.subTest("Value receivers avoiding type checking"):
            self.si.strict_hints = False
            self.ni1.value_receiver = self.si  # Should work fine if the receiver is not
            # strictly checking hints

            unhinted = InputData(
                label="unhinted", owner=DummyOwner(self.p, "has_unhinted")
            )
            self.ni1.value_receiver = unhinted
            unhinted.value_receiver = self.ni2
            # Should work fine if either lacks a hint

    def test_value_assignment(self):
        self.ni1.value = 2  # Should be fine when value matches hint
        self.ni1.value = NOT_DATA  # Should be able to clear the data

        self.ni1.owner.locked = True
        with self.assertRaises(
            InputLockedError,
            msg="Input data should be locked while its owner has data_input_locked",
        ):
            self.ni1.value = 3
        self.ni1.owner.locked = False

        with self.assertRaises(
            TypeError, msg="Should not be able to take values of the wrong type"
        ):
            self.ni2.value = [2]

        self.ni2.strict_hints = False
        self.ni2.value = "now we can take any value"
        self.ni2.strict_hints = True

        self.ni2.type_hint = None
        self.ni2.value = "Also if our hint doesn't exist"

    def test_ready(self):
        with self.subTest("Test defaults and not-data"):
            without_default = InputData(
                label="without_default", owner=DummyOwner(self.p, "has_no_default")
            )
            self.assertIs(
                without_default.value,
                NOT_DATA,
                msg=f"Without a default, spec is to have a NOT_DATA value but got "
                f"{type(without_default.value)}",
            )
            self.assertFalse(
                without_default.ready,
                msg="Even without type hints, readiness should be false when the value "
                "is NOT_DATA",
            )

        self.ni1.value = 1
        self.assertTrue(self.ni1.ready)

        self.ni1._value = "Not numeric at all"  # Bypass type checking
        self.assertFalse(self.ni1.ready)

        self.ni1.strict_hints = False
        self.assertTrue(
            self.ni1.ready,
            msg="Without checking the hint, we should only car that there's data",
        )

    def test_if_not_data(self):
        a = 0 if NOT_DATA else 1
        self.assertEqual(
            a, 1, msg="NOT_DATA failed behave like None in the if-statement"
        )


class TestSignalChannels(unittest.TestCase):
    def setUp(self) -> None:
        self.p = DummyParent("parent_label")
        owner = DummyOwner(self.p, "owner")
        self.inp = InputSignal(label="inp", owner=owner, callback=owner.update)
        self.out = OutputSignal(label="out", owner=DummyOwner(self.p, "owner2"))

    def test_connections(self):
        with self.subTest("Good connection"):
            self.inp.connect(self.out)
            self.assertEqual(self.inp.connections, [self.out])
            self.assertEqual(self.out.connections, [self.inp])

        with self.subTest("Ignore repeated connection"):
            self.out.connect(self.inp)
            self.assertEqual(len(self.inp.connections), 1)
            self.assertEqual(len(self.out.connections), 1)

        with self.subTest("Check disconnection"):
            self.out.disconnect_all()
            self.assertEqual(len(self.inp.connections), 0)
            self.assertEqual(len(self.out.connections), 0)

        with self.subTest("No connections to non-SignalChannels"):
            bad = InputData(
                label="numeric",
                owner=DummyOwner(self.p, "owner3"),
                default=1,
                type_hint=int,
            )
            with self.assertRaises(TypeError):
                self.inp.connect(bad)

        with self.subTest("Test syntactic sugar"):
            self.out.disconnect_all()
            self.out >> self.inp
            self.assertIn(self.out, self.inp.connections)

    def test_calls(self):
        self.out.connect(self.inp)
        self.out()
        self.assertListEqual(self.inp.owner.foo, [0, 1])
        self.inp()
        self.assertListEqual(self.inp.owner.foo, [0, 1, 2])

    def test_aggregating_call(self):
        owner = DummyOwner(self.p, "owner")
        agg = AccumulatingInputSignal(label="agg", owner=owner, callback=owner.update)

        out2 = OutputSignal(label="out2", owner=DummyOwner(self.p, "owner2"))
        agg.connect(self.out, out2)

        out_unrelated = OutputSignal(
            label="out_unrelated", owner=DummyOwner(self.p, "owner3")
        )

        signals_sent = 0
        self.assertEqual(
            2, len(agg.connections), msg="Sanity check on initial conditions"
        )
        self.assertEqual(
            signals_sent,
            len(agg.received_signals),
            msg="Sanity check on initial conditions",
        )
        self.assertListEqual([0], owner.foo, msg="Sanity check on initial conditions")

        agg()
        signals_sent += 0
        self.assertListEqual(
            [0],
            owner.foo,
            msg="Aggregating calls should only matter when they come from a connection",
        )
        agg(out_unrelated)
        signals_sent += 1
        self.assertListEqual(
            [0],
            owner.foo,
            msg="Aggregating calls should only matter when they come from a connection",
        )

        self.out()
        signals_sent += 1
        self.assertEqual(
            signals_sent,
            len(agg.received_signals),
            msg="Signals from other channels should be received",
        )
        self.assertListEqual(
            [0],
            owner.foo,
            msg="Receiving only _one_ of your connections should not fire the callback",
        )

        self.out()
        signals_sent += 0
        self.assertEqual(
            signals_sent,
            len(agg.received_signals),
            msg="Repeatedly receiving the same signal should have no effect",
        )
        self.assertListEqual(
            [0],
            owner.foo,
            msg="Repeatedly receiving the same signal should have no effect",
        )

        out2()
        self.assertListEqual(
            [0, 1],
            owner.foo,
            msg="After 2/2 output signals have fired, the callback should fire",
        )
        self.assertEqual(
            0,
            len(agg.received_signals),
            msg="Firing the callback should reset the list of received signals",
        )

        out2()
        agg.disconnect(out2)
        self.out()
        self.assertListEqual(
            [0, 1, 2],
            owner.foo,
            msg="Having a vestigial received signal (i.e. one from an output signal "
            "that is no longer connected) shouldn't hurt anything",
        )
        self.assertEqual(
            0,
            len(agg.received_signals),
            msg="All signals, including vestigial ones, should get cleared on call",
        )

    def test_callbacks(self):
        class Extended(DummyOwner):
            def method_with_args(self, x):
                return x + 1

            def method_with_only_kwargs(self, x=0):
                return x + 1

            @staticmethod
            def staticmethod_without_args():
                return 42

            @staticmethod
            def staticmethod_with_args(x):
                return x + 1

            @classmethod
            def classmethod_without_args(cls):
                return 42

            @classmethod
            def classmethod_with_args(cls, x):
                return x + 1

        def doesnt_belong_to_owner():
            return 42

        owner = Extended(self.p, "extended")
        with self.subTest("Callbacks that belong to the owner and take no arguments"):
            for callback in [
                owner.update,
                owner.method_with_only_kwargs,
                owner.staticmethod_without_args,
                owner.classmethod_without_args,
            ]:
                with self.subTest(callback.__name__):
                    InputSignal(label="inp", owner=owner, callback=callback)

        with self.subTest("Invalid callbacks"):
            for callback in [
                owner.method_with_args,
                owner.staticmethod_with_args,
                owner.classmethod_with_args,
                doesnt_belong_to_owner,
            ]:
                with (
                    self.subTest(callback.__name__),
                    self.assertRaises(BadCallbackError),
                ):
                    InputSignal(label="inp", owner=owner, callback=callback)


class TestChannelParenting(unittest.TestCase):
    def setUp(self) -> None:
        self.p1 = DummyParent("parent_label1")
        self.p2 = DummyParent("parent_label2")
        self.owner1a = DummyOwner(self.p1, "owner1a")
        self.owner1b = DummyOwner(self.p1, "owner1b")
        self.owner2a = DummyOwner(self.p2, "owner2a")
        self.owner_orphan1 = DummyOwner(None, "owner_orphan")
        self.owner_orphan2 = DummyOwner(None, "owner_orphan")

        self.inp1 = InputChannel(label="inp1a", owner=self.owner1a)
        self.out2 = OutputChannel(label="out1b", owner=self.owner1b)
        self.out_a = OutputChannel(label="out2a", owner=self.owner2a)
        self.inp_orphan = InputChannel(label="inp_orphan", owner=self.owner_orphan1)
        self.out_orphan = OutputChannel(label="out_orphan", owner=self.owner_orphan2)

    def test_without_parents(self):
        # Neither parented to start with
        # Connection works fine
        # Parent is still None for both at the end
        pass

    def test_parenting_an_orphan(self):
        # Parented to None-parent connection, and vice versa
        # Connection works fine
        # None-parent adopts the parent of the parented owner at the end
        pass

    def test_same_parent(self):
        # Works fine
        pass

    def test_different_parents(self):
        # Raises exception
        pass


if __name__ == "__main__":
    unittest.main()
