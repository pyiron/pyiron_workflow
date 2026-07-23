import unittest

from pyiron_workflow.channels import (
    InputData,
    InputSignal,
    OutputData,
    OutputSignal,
)
from pyiron_workflow.io import Inputs, Outputs, Signals


class Dummy:
    def __init__(self, label: str | None = "has_io"):
        super().__init__()
        self._label = label
        self._inputs = Inputs()
        self._outputs = Outputs()
        self._locked = False
        self.parent = None

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> Outputs:
        return self._outputs

    @property
    def label(self) -> str:
        return self._label

    @property
    def full_label(self):
        return "mocked_up/" + self._label

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
        self.input.x = self.post_facto_output
        self.assertIn(
            self.post_facto_output,
            self.input.x.connections,
            msg="The assignment shortcut should allow providing new input connections"
            "even if one already exists",
        )
        self.assertNotIn(
            self.input.x,
            self.output.a.connections,
            msg="Since each data input can only have one incoming connection, the "
            "assignment shortcut should remove the old connection",
        )

        self.input.x = 7.0
        self.assertEqual(self.input.x.value, 7.0)

        self.input.y = self.output.a
        disconnected = self.input.disconnect()
        self.assertListEqual(
            disconnected,
            [(self.input.x, self.post_facto_output), (self.input.y, self.output.a)],
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


if __name__ == "__main__":
    unittest.main()
