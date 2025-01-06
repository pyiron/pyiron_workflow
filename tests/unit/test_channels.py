import unittest

from pyiron_workflow.channels import (
    NOT_DATA,
    AccumulatingInputSignal,
    BadCallbackError,
    Channel,
    ChannelConnectionError,
    InputData,
    InputSignal,
    OutputData,
    OutputSignal,
)


class DummyOwner:
    def __init__(self):
        self.foo = [0]
        self.locked = False
        self.label = "owner_label"

    @property
    def full_label(self):
        return self.label

    def update(self):
        self.foo.append(self.foo[-1] + 1)

    def data_input_locked(self):
        return self.locked


class InputChannel(Channel):
    """Just to de-abstract the base class"""

    def __str__(self):
        return "non-abstract input"

    @property
    def connection_partner_type(self) -> type[Channel]:
        return OutputChannel


class OutputChannel(Channel):
    """Just to de-abstract the base class"""

    def __str__(self):
        return "non-abstract output"

    @property
    def connection_partner_type(self) -> type[Channel]:
        return InputChannel


class TestChannel(unittest.TestCase):
    def setUp(self) -> None:
        self.inp = InputChannel("inp", DummyOwner())
        self.out = OutputChannel("out", DummyOwner())
        self.out2 = OutputChannel("out2", DummyOwner())

    def test_connection_validity(self):
        with self.assertRaises(TypeError, msg="Can't connect to non-channels"):
            self.inp.connect("not an owner")

        with self.assertRaises(
            TypeError, msg="Can't connect to channels that are not the partner type"
        ):
            self.inp.connect(InputChannel("also_input", DummyOwner()))

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
        self.ni1 = InputData(
            label="numeric", owner=DummyOwner(), default=1, type_hint=int | float
        )
        self.ni2 = InputData(
            label="numeric", owner=DummyOwner(), default=1, type_hint=int | float
        )
        self.no = OutputData(
            label="numeric", owner=DummyOwner(), default=0, type_hint=int | float
        )
        self.no_empty = OutputData(
            label="not_data", owner=DummyOwner(), type_hint=int | float
        )

        self.si = InputData(label="list", owner=DummyOwner(), type_hint=list)
        self.so1 = OutputData(
            label="list", owner=DummyOwner(), default=["foo"], type_hint=list
        )

    def test_mutable_defaults(self):
        so2 = OutputData(
            label="list", owner=DummyOwner(), default=["foo"], type_hint=list
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
        self.ni1.connect(self.no_empty)

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
            msg="Data fetch should to first connected value that's actually data,"
            "in this case skipping over no_empty",
        )

        self.no_empty.value = 4
        self.ni1.fetch()
        self.assertEqual(
            self.ni1.value,
            4,
            msg="As soon as no_empty actually has data, it's position as 0th "
            "element in the connections list should give it priority",
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

    def test_copy_connections(self):
        self.ni1.connect(self.no)
        self.ni2.connect(self.no_empty)
        self.ni2.copy_connections(self.ni1)
        self.assertListEqual(
            self.ni2.connections,
            [*self.ni1.connections, self.no_empty],
            msg="Copying should be additive, existing connections should still be there",
        )

        self.ni2.disconnect(*self.ni1.connections)
        self.ni1.connections.append(self.so1)  # Manually include a poorly-typed conn
        with self.assertRaises(
            ChannelConnectionError,
            msg="Should not be able to connect to so1 because of type hint "
            "incompatibility",
        ):
            self.ni2.copy_connections(self.ni1)
        self.assertListEqual(
            self.ni2.connections,
            [self.no_empty],
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

            unhinted = InputData(label="unhinted", owner=DummyOwner())
            self.ni1.value_receiver = unhinted
            unhinted.value_receiver = self.ni2
            # Should work fine if either lacks a hint

    def test_value_assignment(self):
        self.ni1.value = 2  # Should be fine when value matches hint
        self.ni1.value = NOT_DATA  # Should be able to clear the data

        self.ni1.owner.locked = True
        with self.assertRaises(
            RuntimeError,
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
            without_default = InputData(label="without_default", owner=DummyOwner())
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
        owner = DummyOwner()
        self.inp = InputSignal(label="inp", owner=owner, callback=owner.update)
        self.out = OutputSignal(label="out", owner=DummyOwner())

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
                label="numeric", owner=DummyOwner(), default=1, type_hint=int
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
        owner = DummyOwner()
        agg = AccumulatingInputSignal(label="agg", owner=owner, callback=owner.update)

        with self.assertRaises(
            TypeError,
            msg="For an aggregating input signal, it _matters_ who called it, so "
            "receiving an output signal is not optional",
        ):
            agg()

        out2 = OutputSignal(label="out2", owner=DummyOwner())
        agg.connect(self.out, out2)

        self.assertEqual(
            2, len(agg.connections), msg="Sanity check on initial conditions"
        )
        self.assertEqual(
            0, len(agg.received_signals), msg="Sanity check on initial conditions"
        )
        self.assertListEqual([0], owner.foo, msg="Sanity check on initial conditions")

        self.out()
        self.assertEqual(1, len(agg.received_signals), msg="Signal should be received")
        self.assertListEqual(
            [0],
            owner.foo,
            msg="Receiving only _one_ of your connections should not fire the callback",
        )

        self.out()
        self.assertEqual(
            1,
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

        owner = Extended()
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


if __name__ == "__main__":
    unittest.main()
