import unittest

from pyiron_workflow.channels import (
    DataChannel,
    InputData,
    InputSignal,
    OutputData,
    OutputSignal,
)
from pyiron_workflow.io import (
    ConnectionCopyError,
    HasIO,
    Inputs,
    Outputs,
    Signals,
    ValueCopyError,
)


class Dummy(HasIO[Outputs]):
    def __init__(self, label: str = "has_io"):
        super().__init__()
        self._label = label
        self._inputs = Inputs()
        self._outputs = Outputs()
        self._locked = False

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> Outputs:
        return self._outputs

    def run(self, **kwargs):
        pass

    def update(self):
        pass

    def data_input_locked(self):
        return self._locked


class TestDataIO(unittest.TestCase):
    def setUp(self) -> None:
        has_io = Dummy()
        self.inputs = [
            InputData(label="x", owner=has_io, default=0.0, type_hint=float),
            InputData(label="y", owner=has_io, default=1.0, type_hint=float),
        ]
        outputs = [
            OutputData(label="a", owner=has_io, type_hint=float),
        ]

        self.post_facto_output = OutputData(label="b", owner=has_io, type_hint=float)

        self.input = Inputs(*self.inputs)
        self.output = Outputs(*outputs)

    def test_access(self):
        self.assertEqual(self.input.x, self.input["x"])

    def test_assignment(self):
        with self.assertRaises(TypeError):
            self.input.foo = "not an input channel"

        with self.assertRaises(TypeError):
            # Right label, and a channel, but wrong type of channel
            self.input.b = self.post_facto_output

        with self.subTest("Successful channel assignment"):
            self.output.b = self.post_facto_output

        with self.subTest("Can assign to a key that is not the label"):
            label_before_assignment = self.post_facto_output.label
            self.output.not_this_channels_name = self.post_facto_output
            self.assertIs(
                self.output.not_this_channels_name,
                self.post_facto_output,
                msg="Expected channel to get assigned",
            )
            self.assertEqual(
                self.post_facto_output.label,
                label_before_assignment,
                msg="Labels should not get updated on assignment of channels to IO "
                "collections",
            )

    def test_connection(self):
        with self.assertRaises(
            TypeError, msg="Shouldn't be allowed to connect two inputs"
        ):
            self.input.x = self.input.y
        self.assertEqual(
            0,
            len(self.input.x.connections),
            msg="Sanity check that the above error-raising connection never got made",
        )

        self.input.x = self.output.a
        self.assertIn(
            self.input.x,
            self.output.a.connections,
            msg="Should be able to create connections by assignment",
        )

        self.input.x = 7.0
        self.assertEqual(self.input.x.value, 7.0)

        self.input.y = self.output.a
        disconnected = self.input.disconnect()
        self.assertListEqual(
            disconnected,
            [(self.input.x, self.output.a), (self.input.y, self.output.a)],
            msg="Disconnecting the panel should disconnect all children",
        )

    def test_conversion(self):
        converted = self.input.to_value_dict()
        for template in self.inputs:
            self.assertEqual(template.default, converted[template.label])
        self.assertEqual(
            len(self.inputs),
            len(converted),
            msg="And it shouldn't have any extra items either",
        )

    def test_iteration(self):
        self.assertTrue(all(c.label in self.input.labels for c in self.input))

    def test_connections_property(self):
        self.assertEqual(
            len(self.input.connections),
            0,
            msg="Sanity check expectations about self.input",
        )
        self.assertEqual(
            len(self.output.connections),
            0,
            msg="Sanity check expectations about self.input",
        )

        for inp in self.input:
            inp.connect(self.output.a)

        self.assertEqual(
            len(self.output.connections),
            len(self.input),
            msg="Expected to find all the channels in the input",
        )
        self.assertEqual(
            len(self.input.connections),
            1,
            msg="Each unique connection should appear only once",
        )
        self.assertIs(
            self.input.connections[0],
            self.input.x.connections[0],
            msg="The IO connection found should be the same object as the channel "
            "connection",
        )

    def test_to_list(self):
        self.assertListEqual(
            [0.0, 1.0],
            self.input.to_list(),
            msg="Expected a shortcut to channel values. Order is explicitly not "
            "guaranteed in the docstring, but it would be nice to appear in the "
            "order the channels are added here",
        )


class TestSignalIO(unittest.TestCase):
    def setUp(self) -> None:
        class Extended(Dummy):
            @staticmethod
            def do_nothing():
                pass

        has_io = Extended()

        signals = Signals()
        signals.input.run = InputSignal("run", has_io, has_io.do_nothing)
        signals.input.foo = InputSignal("foo", has_io, has_io.do_nothing)
        signals.output.ran = OutputSignal("ran", has_io)
        signals.output.bar = OutputSignal("bar", has_io)

        signals.output.ran >> signals.input.run
        signals.output.ran >> signals.input.foo
        signals.output.bar >> signals.input.run
        signals.output.bar >> signals.input.foo

        self.signals = signals

    def test_disconnect(self):
        self.assertEqual(
            4,
            len(self.signals.disconnect()),
            msg="Disconnect should disconnect all on panels and the Signals super-panel",
        )

    def test_disconnect_run(self):
        self.assertEqual(
            2,
            len(self.signals.disconnect_run()),
            msg="Should disconnect exactly everything connected to run",
        )

        no_run_signals = Signals()
        self.assertEqual(
            0,
            len(no_run_signals.disconnect_run()),
            msg="If there is no run channel, the list of disconnections should be empty",
        )


class TestHasIO(unittest.TestCase):
    def test_init(self):
        has_io = Dummy()
        self.assertIsInstance(has_io.inputs, Inputs)
        self.assertIsInstance(has_io.outputs, Outputs)
        self.assertIsInstance(has_io.signals, Signals)

    def test_set_input_values(self):
        has_io = Dummy()
        has_io.inputs["input_channel"] = InputData("input_channel", has_io)
        has_io.inputs["more_input"] = InputData("more_input", has_io)

        has_io.set_input_values("v1", "v2")
        self.assertDictEqual(
            {"input_channel": "v1", "more_input": "v2"},
            has_io.inputs.to_value_dict(),
            msg="Args should be set by order of channel appearance",
        )
        has_io.set_input_values(more_input="v4", input_channel="v3")
        self.assertDictEqual(
            {"input_channel": "v3", "more_input": "v4"},
            has_io.inputs.to_value_dict(),
            msg="Kwargs should be set by key-label matching",
        )
        has_io.set_input_values("v5", more_input="v6")
        self.assertDictEqual(
            {"input_channel": "v5", "more_input": "v6"},
            has_io.inputs.to_value_dict(),
            msg="Mixing and matching args and kwargs is permissible",
        )

        with self.assertRaises(ValueError, msg="More args than channels is disallowed"):
            has_io.set_input_values(1, 2, 3)

        with self.assertRaises(
            ValueError, msg="A channel updating from both args and kwargs is disallowed"
        ):
            has_io.set_input_values(1, input_channel=2)

        with self.assertRaises(ValueError, msg="Kwargs not among input is disallowed"):
            has_io.set_input_values(not_a_channel=42)

    def test_connected_and_disconnect(self):
        has_io1 = Dummy(label="io1")
        has_io2 = Dummy(label="io2")
        has_io1 >> has_io2
        self.assertTrue(
            has_io1.connected,
            msg="Any connection should result in a positive connected status",
        )
        has_io1.disconnect()
        self.assertFalse(
            has_io1.connected, msg="Disconnect should break all connections"
        )

    def test_strict_hints(self):
        has_io = Dummy()
        has_io.inputs["input_channel"] = InputData("input_channel", has_io)
        self.assertTrue(has_io.inputs.input_channel.strict_hints, msg="Sanity check")
        has_io.deactivate_strict_hints()
        self.assertFalse(
            has_io.inputs.input_channel.strict_hints,
            msg="Hint strictness should be accessible from the top level",
        )
        has_io.activate_strict_hints()
        self.assertTrue(
            has_io.inputs.input_channel.strict_hints,
            msg="Hint strictness should be accessible from the top level",
        )

    def test_rshift_operator(self):
        has_io1 = Dummy(label="io1")
        has_io2 = Dummy(label="io2")
        has_io1 >> has_io2
        self.assertIn(
            has_io1.signals.output.ran,
            has_io2.signals.input.run.connections,
            msg="Right shift should be syntactic sugar for an 'or' run connection",
        )

    def test_lshift_operator(self):
        has_io1 = Dummy(label="io1")
        has_io2 = Dummy(label="io2")
        has_io1 << has_io2
        self.assertIn(
            has_io1.signals.input.accumulate_and_run,
            has_io2.signals.output.ran.connections,
            msg="Left shift should be syntactic sugar for an 'and' run connection",
        )
        has_io1.disconnect()

        has_io3 = Dummy(label="io3")
        has_io1 << (has_io2, has_io3)
        print(has_io1.signals.input.accumulate_and_run.connections)
        self.assertListEqual(
            [has_io3.signals.output.ran, has_io2.signals.output.ran],
            has_io1.signals.input.accumulate_and_run.connections,
            msg="Left shift should accommodate groups of connections",
        )

    def test_copy_io(self):
        # Setup
        upstream = Dummy(label="upstream")
        upstream.outputs["output_channel"] = OutputData(
            "output_channel", upstream, type_hint=float
        )

        to_copy = Dummy(label="to_copy")
        to_copy.inputs["used_input"] = InputData("used_input", to_copy, default=42)
        to_copy.inputs["hinted_input"] = InputData(
            "hinted_input", to_copy, type_hint=float
        )
        to_copy.inputs["unused_input"] = InputData(
            "unused_input", to_copy, default="has a value but not connected"
        )
        to_copy.outputs["used_output"] = OutputData("used_output", to_copy)
        to_copy.outputs["unused_output"] = OutputData("unused_output", to_copy)
        to_copy.signals.input["custom_signal"] = InputSignal(
            "custom_signal",
            to_copy,
            to_copy.update,
        )
        to_copy.signals.input["unused_signal"] = InputSignal(
            "unused_signal",
            to_copy,
            to_copy.update,
        )

        downstream = Dummy(label="downstream")
        downstream.inputs["input_channel"] = InputData("input_channel", downstream)

        to_copy.inputs.used_input.connect(upstream.outputs.output_channel)
        to_copy.inputs.hinted_input.connect(upstream.outputs.output_channel)
        to_copy.signals.input.custom_signal.connect(upstream.signals.output.ran)
        to_copy >> downstream

        # Create copy candidates that will pass or fail
        copier = Dummy("subset")

        with self.subTest("Fails on missing connections"):
            with self.assertRaises(
                ConnectionCopyError,
                msg="The copier is missing all sorts of connected channels and should "
                "fail to copy",
            ):
                copier.copy_io(
                    to_copy, connections_fail_hard=True, values_fail_hard=False
                )
            self.assertFalse(
                copier.connected,
                msg="After a failure, any connections that _were_ made should get "
                "reset",
            )

        with self.subTest("Force missing connections"):
            copier.copy_io(to_copy, connections_fail_hard=False, values_fail_hard=False)
            self.assertIn(
                copier.signals.output.ran,
                downstream.signals.input.run,
                msg="The channel that _can_ get copied _should_ get copied",
            )
            copier.signals.output.ran.disconnect_all()
            self.assertFalse(
                copier.connected,
                msg="Sanity check that that was indeed the only connection",
            )

        copier.inputs["used_input"] = InputData("used_input", copier)
        copier.inputs["hinted_input"] = InputData(
            "hinted_input",
            copier,
            type_hint=str,  # Different hint!
        )
        copier.inputs["extra_input"] = InputData(
            "extra_input", copier, default="not on the copied object but that's ok"
        )
        copier.outputs["used_output"] = OutputData("used_output", copier)
        copier.signals.input["custom_signal"] = InputSignal(
            "custom_signal",
            copier,
            copier.update,
        )

        with (
            self.subTest("Bad hint causes connection error"),
            self.assertRaises(
                ConnectionCopyError,
                msg="Can't connect channels with incommensurate type hints",
            ),
        ):
            copier.copy_io(to_copy, connections_fail_hard=True, values_fail_hard=False)

        # Bring the copier's type hint in-line with the object being copied
        copier.inputs.hinted_input.type_hint = float

        with self.subTest("Passes missing values"):
            copier.copy_io(to_copy, connections_fail_hard=True, values_fail_hard=False)
            for copier_panel, copied_panel in zip(
                copier._owned_io_panels, to_copy._owned_io_panels, strict=False
            ):
                for copier_channel in copier_panel:
                    try:
                        copied_channel = copied_panel[copier_channel.label]
                        self.assertListEqual(
                            copier_channel.connections,
                            copied_channel.connections,
                            msg="All connections on shared channels should copy",
                        )

                        if isinstance(copier_channel, DataChannel):
                            self.assertEqual(
                                copier_channel.value,
                                copied_channel.value,
                                msg="All values on shared channels should copy",
                            )
                    except AttributeError:
                        # We only need to check shared channels
                        pass

        with (
            self.subTest("Force failure on value copy fail"),
            self.assertRaises(
                ValueCopyError,
                msg="The copier doesn't have channels to hold all the values that need"
                "copying, so we should fail",
            ),
        ):
            copier.copy_io(to_copy, connections_fail_hard=True, values_fail_hard=True)


if __name__ == "__main__":
    unittest.main()
