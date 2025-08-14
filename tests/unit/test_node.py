import contextlib
import unittest

import bagofholding as boh

from pyiron_workflow.channels import (
    NOT_DATA,
    InputData,
    InputLockedError,
)
from pyiron_workflow.io import Inputs
from pyiron_workflow.mixin.injection import (
    OutputDataWithInjection,
    OutputsWithInjection,
)
from pyiron_workflow.mixin.run import ReadinessError
from pyiron_workflow.node import (
    AmbiguousOutputError,
    Node,
)
from pyiron_workflow.storage import PickleStorage, available_backends


def add_one(x):
    return x + 1


class ANode(Node):
    """To de-abstract the class"""

    def _setup_node(self) -> None:
        self._inputs = Inputs(
            InputData("x", self, type_hint=int),
            InputData("s", self, type_hint=str, default="hello"),
        )
        self._outputs = OutputsWithInjection(
            OutputDataWithInjection("y", self, type_hint=int),
        )

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> OutputsWithInjection:
        return self._outputs

    def _on_run(self, *args, **kwargs):
        return add_one(*args)

    @property
    def run_args(self) -> dict:
        return (self.inputs.x.value,), {}

    def process_run_result(self, run_output):
        self.outputs.y.value = run_output
        return run_output

    def to_dict(self):
        pass


class TwOutputs(ANode):
    """To de-abstract the class"""

    def _setup_node(self) -> None:
        super()._setup_node()
        self._outputs = OutputsWithInjection(
            OutputDataWithInjection("y", self, type_hint=int),
            OutputDataWithInjection("z", self, type_hint=int),
        )

    def process_run_result(self, run_output):
        self.outputs.y.value = run_output
        self.outputs.z.value = run_output + 1
        return self.outputs.y.value, self.outputs.z.value


class TestNode(unittest.TestCase):
    def setUp(self):
        self.n1 = ANode(label="start", x=0)
        self.n2 = ANode(label="middle", x=self.n1.outputs.y)
        self.n3 = ANode(label="end", x=self.n2.outputs.y)

    def test_set_input_values(self):
        n = ANode()
        n.set_input_values(x=2)
        self.assertEqual(
            2,
            n.inputs.x.value,
            msg="Post-instantiation update of inputs should also work",
        )

        n.set_input_values(42, "goodbye")
        self.assertDictEqual(
            {"x": 42, "s": "goodbye"},
            n.inputs.to_value_dict(),
            msg="Args should be set by order of channel appearance",
        )

        n.set_input_values(s="again", x=0)
        self.assertDictEqual(
            {"x": 0, "s": "again"},
            n.inputs.to_value_dict(),
            msg="Kwargs should be set by key-label matching",
        )

        n.set_input_values(1, s="something")
        self.assertDictEqual(
            {"x": 1, "s": "something"},
            n.inputs.to_value_dict(),
            msg="Mixing and matching args and kwargs is permissible",
        )

        with self.assertRaises(ValueError, msg="More args than channels is disallowed"):
            n.set_input_values(1, "str", None)

        with self.assertRaises(ValueError, msg="Non-input-channel kwargs not allowed"):
            n.set_input_values(z=3)

        with self.assertRaises(
            ValueError, msg="A channel updating from both args and kwargs is disallowed"
        ):
            n.set_input_values(1, x=2)

        with self.assertRaises(
            TypeError,
            msg="Type checking should be applied",
        ):
            n.set_input_values(x="not an int")

        n.deactivate_strict_hints()
        n.set_input_values(x="not an int")
        self.assertEqual(
            "not an int",
            n.inputs.x.value,
            msg="It should be possible to deactivate type checking from the node level",
        )

    def test_connected_and_disconnect(self):
        n1 = ANode(label="io1")
        n2 = ANode(label="io2", x=n1.outputs.y)
        n1 >> n2
        self.assertTrue(
            n1.connected,
            msg="Any connection should result in a positive connected status",
        )
        self.assertFalse(n1.fully_connected)

        n1.disconnect()
        self.assertFalse(n1.connected, msg="Disconnect should break all connections")

    def test_strict_hints(self):
        n = ANode()
        n.inputs["input_channel"] = InputData("input_channel", n)
        self.assertTrue(n.inputs.input_channel.strict_hints, msg="Sanity check")
        n.deactivate_strict_hints()
        self.assertFalse(
            n.inputs.input_channel.strict_hints,
            msg="Hint strictness should be accessible from the top level",
        )
        n.activate_strict_hints()
        self.assertTrue(
            n.inputs.input_channel.strict_hints,
            msg="Hint strictness should be accessible from the top level",
        )

    def test_rshift_operator(self):
        n1 = ANode(label="n1")
        n2 = ANode(label="n2")
        n1 >> n2
        self.assertIn(
            n1.signals.output.ran,
            n2.signals.input.run.connections,
            msg="Right shift should be syntactic sugar for an 'or' run connection",
        )

    def test_lshift_operator(self):
        n1 = ANode(label="io1")
        n2 = ANode(label="io2")
        n1 << n2
        self.assertIn(
            n1.signals.input.accumulate_and_run,
            n2.signals.output.ran.connections,
            msg="Left shift should be syntactic sugar for an 'and' run connection",
        )
        n1.disconnect()

        n3 = ANode(label="io3")
        n1 << (n2, n3)
        print(n1.signals.input.accumulate_and_run.connections)
        self.assertListEqual(
            [n3.signals.output.ran, n2.signals.output.ran],
            n1.signals.input.accumulate_and_run.connections,
            msg="Left shift should accommodate groups of connections",
        )

    def test_no_new_input_while_running(self):
        n = ANode()
        n.set_input_values(x=2)
        n.running = True  # Fake it

        with self.assertRaises(
            InputLockedError, msg="With existing input, we should asplode"
        ):
            n.run(x=2, rerun=True)

    def test_run_data_tree(self):
        self.assertEqual(
            add_one(add_one(add_one(self.n1.inputs.x.value))),
            self.n3.run(run_data_tree=True),
            msg="Should pull start down to end, even with no flow defined",
        )

    def test_fetch_input(self):
        self.n1.outputs.y.value = 0
        with self.assertRaises(
            ValueError, msg="Without input, we should not achieve readiness"
        ):
            self.n2.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.assertEqual(
            add_one(self.n1.outputs.y.value),
            self.n2.run(run_data_tree=False, fetch_input=True),
            msg="After fetching the upstream data, should run fine",
        )

    def test_cache(self):
        n = ANode()
        n.set_input_values(x=2)
        self.assertFalse(n.cache_hit)
        n._cache_inputs()
        self.assertTrue(n.cache_hit)
        n.clear_cache()
        self.assertFalse(n.cache_hit)

    def test_check_readiness(self):
        n3_cache = self.n3.use_cache
        self.n3.use_cache = False
        self.n3.recovery = None  # We intentionally raise errors,
        # but don't care about generating a file

        with self.assertRaises(
            ValueError, msg="When input is not data, we should fail early"
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.assertFalse(
            self.n3.failed,
            msg="The benefit of the readiness check should be that we don't actually "
            "qualify as failed",
        )

        with self.assertRaises(
            TypeError,
            msg="If we bypass the check, we should get the failing function error",
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=False)

        self.assertTrue(
            self.n3.failed,
            msg="If the node operation itself fails, the status should be failed",
        )

        self.n3.inputs.x = 0
        with self.assertRaises(
            ValueError,
            msg="When status is failed, we should fail early, even if input data is ok",
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.n3.failed = False
        self.assertEqual(
            1,
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True),
            msg="After manually resetting the failed state and providing good input, "
            "running should proceed",
        )

        self.n3.use_cache = n3_cache

    def test_emit_ran_signal(self):
        self.n1 >> self.n2 >> self.n3  # Chained connection declaration

        self.n1.run(emit_ran_signal=False)
        self.assertFalse(
            self.n3.inputs.x.ready,
            msg="Without emitting the ran signal, nothing should happen downstream",
        )

        self.n1.run(emit_ran_signal=True)
        self.assertEqual(
            add_one(add_one(add_one(self.n1.inputs.x.value))),
            self.n3.outputs.y.value,
            msg="With the connection and signal, we should have pushed downstream "
            "execution",
        )

    def test_failure_signal(self):
        n = ANode(label="failing")
        n.inputs.x._value = "cannot add 1 to this"  # Bypass type hint with private

        class Counter:
            def __init__(self):
                self.count = 0

            def add(self, signal):
                self.count += 1

        c = Counter()
        n.signals.output.failed.connections = [c.add]
        try:
            n.run(check_readiness=False)
        except TypeError:
            # Expected -- we're _trying_ to get failure to fire
            n.delete_storage(filename=n.as_path().joinpath("recovery"))
        self.assertEqual(c.count, 1, msg="Failed signal should fire after type error")

    def test_failure_recovery(self):
        n = ANode(label="failing")
        n.use_cache = False
        n.inputs.x._value = "cannot add 1 to this"  # Bypass type hint with private

        try:
            n.run(check_readiness=False, raise_run_exceptions=False)
            self.assertFalse(
                n.as_path().exists(),
                msg="When the run exception is not raised, we don't expect any "
                "recovery file to be needed",
            )

            default_recovery = n.recovery
            n.recovery = None
            with contextlib.suppress(TypeError):
                # Expected -- we're _trying_ to get failure to fire
                n.run(check_readiness=False)
            self.assertFalse(
                n.has_saved_content(filename=n.as_path().joinpath("recovery")),
                msg="Without a recovery back end specified, we don't expect a file to "
                "be saved on failure.",
            )

            n.recovery = default_recovery
            with contextlib.suppress(TypeError):
                # Expected -- we're _trying_ to get failure to fire
                n.run(check_readiness=False)
            self.assertTrue(
                n.has_saved_content(filename=n.as_path().joinpath("recovery")),
                msg="Expect a recovery file to be saved on failure",
            )

            reloaded = ANode(label="failing")
            self.assertIs(
                reloaded.inputs.x.value,
                NOT_DATA,
                msg="We don't anticipate _auto_ loading from recovery files",
            )
            self.assertFalse(reloaded.failed, msg="Sanity check")
            reloaded.load(filename=reloaded.as_path().joinpath("recovery"))
            self.assertTrue(
                reloaded.failed, msg="Expect to have reloaded the failed node."
            )
            self.assertEqual(
                reloaded.inputs.x.value,
                n.inputs.x.value,
                msg="Expect data to have been reloaded from the failed node",
            )

        finally:
            n.delete_storage(filename=n.as_path().joinpath("recovery"))
            self.assertFalse(
                n.as_path().exists(),
                msg="The recovery file should have been the only thing in the node "
                "directory, so cleaning should remove the directory entirely.",
            )

    def test_execute(self):
        self.n1.outputs.y = 0  # Prime the upstream data source for fetching
        self.n2 >> self.n3
        self.assertEqual(
            self.n2.run(fetch_input=False, emit_ran_signal=False, x=10) + 1,
            self.n2.execute(x=11),
            msg="Execute should _not_ fetch in the upstream data",
        )
        self.assertFalse(
            self.n3.ready,
            msg="Executing should not be triggering downstream runs, even though we "
            "made a ran/run connection",
        )

        self.n2.inputs.x._value = "manually override the desired int"
        self.n2.recovery = None  # We are intentionally about to fail, no need for file
        with self.assertRaises(
            TypeError,
            msg="Execute should be running without a readiness check and hitting the "
            "string + int error",
        ):
            self.n2.execute()

    def test_pull(self):
        n_a = ANode(label="middle_neighbour", x=self.n1)
        self.n1 >> n_a

        self.n2 >> self.n3
        self.n1.inputs.x = 0
        by_run = self.n2.run(
            run_data_tree=True, fetch_input=True, emit_ran_signal=False
        )
        self.assertIs(
            n_a.outputs.y.value,
            NOT_DATA,
            msg="Even though n1 is connected to n_a, n_a is not part of the data tree "
            "for n2 and should not get run",
        )
        self.assertEqual(
            2,
            n_a.run(),
            msg="After pulling n2, we should still recover all the connections between "
            "n1 and n_a.",
        )
        self.n1.inputs.x = 1
        self.assertEqual(
            by_run + 1, self.n2.pull(), msg="Pull should be running the upstream node"
        )
        self.assertFalse(
            self.n3.ready,
            msg="Pulling should not be triggering downstream runs, even though we "
            "made a ran/run connection",
        )

        self.n1.inputs.x._value = "manually override the desired int"
        with self.assertRaises(ReadinessError):
            self.n2.pull()
        self.assertTrue(
            n_a.connected,
            msg="After a failed pull, we should safely recover original connections -- "
            "in this case, those that were broken to exclude n_a from the tree",
        )

    def test_push(self):
        self.n1.push(x=0)
        self.assertIs(self.n1.outputs.y.value, 1)
        self.assertIs(self.n2.outputs.y.value, NOT_DATA)
        self.n1 >> self.n2
        self.n1.push(x=2)
        self.assertIs(self.n1.outputs.y.value, 3)
        self.assertIs(self.n2.outputs.y.value, 4)

    def test___call__(self):
        # __call__ is just a pull that punches through macro walls, so we'll need to
        # test it again over in macro to really make sure it's working
        self.n2 >> self.n3
        self.n1.inputs.x = 0
        by_run = self.n2.run(
            run_data_tree=True, fetch_input=True, emit_ran_signal=False
        )
        self.n1.inputs.x = 1
        self.assertEqual(
            by_run + 1, self.n2(), msg="A call should be running the upstream node"
        )
        self.assertFalse(
            self.n3.ready,
            msg="Calling should not be triggering downstream runs, even though we "
            "made a ran/run connection",
        )

    def test_draw(self):
        try:
            self.n1.draw()
            self.assertFalse(self.n1.as_path().exists())

            for fmt in ["pdf", "png"]:
                with self.subTest(f"Testing with format {fmt}"):
                    self.n1.draw(save=True, format=fmt)
                    expected_name = self.n1.label + "_graph." + fmt
                    # That name is just an implementation detail, update it as
                    # needed
                    self.assertTrue(
                        self.n1.as_path().joinpath(expected_name).is_file(),
                        msg="If `save` is called, expect the rendered image to "
                        "exist in the working directory",
                    )

            user_specified_name = "foo"
            self.n1.draw(filename=user_specified_name, format=fmt)
            expected_name = user_specified_name + "." + fmt
            self.assertTrue(
                self.n1.as_path().joinpath(expected_name).is_file(),
                msg="If the user specifies a filename, we should assume they want the "
                "thing saved",
            )
        finally:
            # No matter what happens in the tests, clean up after yourself
            if self.n1.as_path().exists():
                for p in self.n1.as_path().iterdir():
                    p.unlink()
                self.n1.as_path().rmdir()

    def test_autorun(self):
        self.assertIs(
            self.n1.outputs.y.value,
            NOT_DATA,
            msg="By default, nodes should not be getting run until asked",
        )
        self.assertEqual(
            1,
            ANode(label="right_away", autorun=True, x=0).outputs.y.value,
            msg="With autorun, the node should run right away",
        )

    def test_graph_info(self):
        n = ANode()

        self.assertEqual(
            n.lexical_delimiter + n.label,
            n.graph_path,
            msg="Lone nodes should just have their label as the path, as there is no "
            "parent above.",
        )

        self.assertIs(
            n,
            n.graph_root,
            msg="Lone nodes should be their own graph_root, as there is no parent "
            "above.",
        )

    def test_single_value(self):
        node = ANode(label="n")
        self.assertIs(
            node.outputs.y,
            node.channel,
            msg="With a single output, the `HasChannel` interface fulfillment should "
            "use that output.",
        )

        with_addition = node + 5
        self.assertIsInstance(
            with_addition,
            Node,
            msg="With a single output, acting on the node should fall back on acting "
            "on the single (with-injection) output",
        )

        node2 = ANode(label="n2")
        node2.inputs.x = node
        self.assertListEqual(
            [node.outputs.y],
            node2.inputs.x.connections,
            msg="With a single output, the node should fall back on the single output "
            "for output-like use cases",
        )

        node.outputs["z"] = OutputDataWithInjection("z", node, type_hint=int)
        with self.assertRaises(
            AmbiguousOutputError,
            msg="With multiple outputs, trying to exploit the `HasChannel` interface "
            "should fail cleanly",
        ):
            node.channel  # noqa: B018

    def test_injection_hasattr(self):
        x = 2
        node = ANode(label="n")
        node.set_input_values(x=x)
        node.run()

        self.assertEqual(
            node.value,
            add_one(x),
            msg="With a single output, we expect to access the channel attribute",
        )

        two_outputs = TwOutputs(label="to")
        two_outputs.set_input_values(x=x)
        two_outputs.run()

        with self.assertRaises(
            AmbiguousOutputError,
            msg="With two output channels, we should not be able to isolate a well "
            "defined value attribute because there is no single `channel` to look on",
        ):
            getattr(two_outputs, "value")  # noqa: B009

        with self.assertRaises(
            AmbiguousOutputError,
            msg="As a corollary to the last assertion, hasattr should fail",
        ):
            hasattr(two_outputs, "value")

    def test_storage(self):
        self.assertIs(
            self.n1.outputs.y.value, NOT_DATA, msg="Sanity check on initial state"
        )
        y = self.n1()

        self.assertFalse(self.n1.has_saved_content())

        with self.assertRaises(
            FileNotFoundError,
            msg="We just verified there is no save file, so loading should fail.",
        ):
            self.n1.load()

        for backend in available_backends():
            with self.subTest(backend):
                try:
                    self.n1.save(backend=backend)
                    self.assertTrue(self.n1.has_saved_content())

                    x = self.n1.inputs.x.value
                    reloaded = ANode(label=self.n1.label, x=x, autoload=backend)
                    self.assertEqual(
                        y,
                        reloaded.outputs.y.value,
                        msg="Nodes should load by default if they find a save file",
                    )

                    clean_slate = ANode(
                        label=self.n1.label, x=x, delete_existing_savefiles=True
                    )
                    self.assertIs(
                        clean_slate.outputs.y.value,
                        NOT_DATA,
                        msg="Users should be able to ignore a save",
                    )

                    run_right_away = ANode(
                        label=self.n1.label,
                        x=x,
                        autorun=True,
                    )
                    self.assertEqual(
                        y,
                        run_right_away.outputs.y.value,
                        msg="With nothing to load, running after init is fine",
                    )

                    run_right_away.save(backend=backend)
                    load_and_rerun_origal_input = ANode(
                        label=self.n1.label, autorun=True, autoload=backend
                    )
                    self.assertEqual(
                        load_and_rerun_origal_input.outputs.y.value,
                        run_right_away.outputs.y.value,
                        msg="Loading and then running immediately is fine, and should "
                        "recover existing input",
                    )
                    load_and_rerun_new_input = ANode(
                        label=self.n1.label, x=x + 1, autorun=True, autoload=backend
                    )
                    self.assertEqual(
                        load_and_rerun_new_input.outputs.y.value,
                        run_right_away.outputs.y.value + 1,
                        msg="Loading and then running immediately is fine, and should "
                        "notice the new input",
                    )

                    force_run = ANode(
                        label=self.n1.label,
                        x=x,
                        autorun=True,
                        delete_existing_savefiles=True,
                    )
                    self.assertEqual(
                        y,
                        force_run.outputs.y.value,
                        msg="Destroying the save should allow immediate re-running",
                    )

                    hard_input = ANode(label="hard")
                    hard_input.inputs.x.type_hint = callable
                    hard_input.inputs.x = lambda x: x * 2
                    if isinstance(backend, PickleStorage):
                        hard_input.save(backend=backend, cloudpickle_fallback=True)
                        reloaded = ANode(label=hard_input.label, autoload=backend)
                        self.assertEqual(
                            reloaded.inputs.x.value(4),
                            hard_input.inputs.x.value(4),
                            msg="Cloud pickle should be strong enough to recover this",
                        )
                    else:
                        with self.assertRaises(
                            boh.StringNotImportableError,
                            msg="Other backends are not powerful enough for some values",
                        ):
                            hard_input.save(backend=backend)
                finally:
                    self.n1.delete_storage(backend)
                    hard_input.delete_storage(backend)

    def test_storage_to_filename(self):
        y = self.n1()
        fname = "foo"

        for backend in available_backends():
            with self.subTest(backend):
                try:
                    self.n1.save(backend=backend, filename=fname)
                    self.assertFalse(
                        self.n1.has_saved_content(backend=backend),
                        msg="There should be no content at the default location",
                    )
                    self.assertTrue(
                        self.n1.has_saved_content(backend=backend, filename=fname),
                        msg="There should be content at the specified file location",
                    )
                    new = ANode()
                    new.load(backend=backend, filename=fname)
                    self.assertEqual(new.label, self.n1.label)
                    self.assertEqual(new.outputs.y.value, y)
                finally:
                    self.n1.delete_storage(backend=backend, filename=fname)
                self.assertFalse(
                    self.n1.has_saved_content(backend=backend, filename=fname),
                    msg="Deleting storage should have cleaned up the file",
                )

    def test_checkpoint(self):
        for backend in available_backends():
            with self.subTest(backend):
                try:
                    ANode(
                        label="just_run",
                        x=0,
                        autorun=True,
                    )
                    saves = ANode(
                        label="run_and_save",
                        x=0,
                        autorun=True,
                        checkpoint=backend,
                    )
                    y = saves.outputs.y.value

                    not_reloaded = ANode(label="just_run", autoload=backend)
                    self.assertIs(
                        NOT_DATA,
                        not_reloaded.outputs.y.value,
                        msg="Should not have saved, therefore should have been nothing "
                        "to load",
                    )

                    find_saved = ANode(label="run_and_save", autoload=backend)
                    self.assertEqual(
                        y,
                        find_saved.outputs.y.value,
                        msg="Should have saved automatically after run, and reloaded "
                        "on instantiation",
                    )
                finally:
                    saves.delete_storage(backend)  # Clean up

    def test_load_running(self):
        n = ANode(label="to_save", x=42)
        n.running = True
        n.save(backend="pickle")
        reloaded = ANode(label="to_save", autoload="pickle")
        self.assertTrue(
            reloaded.running,
            msg="We should be able to load a node that was saved in the running state.",
        )
        self.assertEqual(
            n.inputs.x.value,
            reloaded.inputs.x.value,
            msg="Sanity check that we have loaded the data too",
        )
        with self.assertRaises(
            ValueError,
            msg="We should not be able to load new data into a currently running "
            "node",
        ):
            reloaded.load()
        n.delete_storage()


if __name__ == "__main__":
    unittest.main()
