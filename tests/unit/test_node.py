from concurrent.futures import Future
import os
import platform
from subprocess import CalledProcessError
import sys
from typing import Literal, Optional
import unittest

from pyiron_workflow.channels import InputData, NOT_DATA
from pyiron_workflow.snippets.files import DirectoryObject
from pyiron_workflow.create import Executor
from pyiron_workflow.injection import OutputDataWithInjection, OutputsWithInjection
from pyiron_workflow.io import Inputs
from pyiron_workflow.node import Node
from pyiron_workflow.single_output import AmbiguousOutputError


def add_one(x):
    return x + 1


class ANode(Node):
    """To de-abstract the class"""

    def __init__(
        self,
        label,
        overwrite_save=False,
        run_after_init=False,
        storage_backend: Optional[Literal["h5io", "tinybase"]] = None,
        save_after_run: bool = False,
        x=None,
    ):
        super().__init__(
            label=label, save_after_run=save_after_run, storage_backend=storage_backend
        )
        self._inputs = Inputs(InputData("x", self, type_hint=int))
        self._outputs = OutputsWithInjection(
            OutputDataWithInjection("y", self, type_hint=int),
        )
        if x is not None:
            self.inputs.x = x

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> OutputsWithInjection:
        return self._outputs

    @property
    def on_run(self):
        return add_one

    @property
    def run_args(self) -> dict:
        return {"x": self.inputs.x.value}

    def process_run_result(self, run_output):
        self.outputs.y.value = run_output
        return run_output

    def to_dict(self):
        pass


class TestNode(unittest.TestCase):
    def setUp(self):
        self.n1 = ANode("start", x=0)
        self.n2 = ANode("middle", x=self.n1.outputs.y)
        self.n3 = ANode("end", x=self.n2.outputs.y)

    def test_set_input_values(self):
        n = ANode("some_node")
        n.set_input_values(x=2)
        self.assertEqual(
            2,
            n.inputs.x.value,
            msg="Post-instantiation update of inputs should also work"
        )

        n.set_input_values(y=3)
        # Missing keys may throw a warning, but are otherwise allowed to pass

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
            msg="It should be possible to deactivate type checking from the node level"
        )

    def test_run_data_tree(self):
        self.assertEqual(
            add_one(add_one(add_one(self.n1.inputs.x.value))),
            self.n3.run(run_data_tree=True),
            msg="Should pull start down to end, even with no flow defined"
        )

    def test_fetch_input(self):
        self.n1.outputs.y.value = 0
        with self.assertRaises(
            ValueError,
            msg="Without input, we should not achieve readiness"
        ):
            self.n2.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.assertEqual(
            add_one(self.n1.outputs.y.value),
            self.n2.run(run_data_tree=False, fetch_input=True),
            msg="After fetching the upstream data, should run fine"
        )

    def test_check_readiness(self):
        with self.assertRaises(
            ValueError,
            msg="When input is not data, we should fail early"
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.assertFalse(
            self.n3.failed,
            msg="The benefit of the readiness check should be that we don't actually "
                "qualify as failed"
        )

        with self.assertRaises(
            TypeError,
            msg="If we bypass the check, we should get the failing function error"
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=False)

        self.assertTrue(
            self.n3.failed,
            msg="If the node operation itself fails, the status should be failed"
        )

        self.n3.inputs.x = 0
        with self.assertRaises(
            ValueError,
            msg="When status is failed, we should fail early, even if input data is ok"
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True)

        self.n3.failed = False
        self.assertEqual(
            1,
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=True),
            msg="After manually resetting the failed state and providing good input, "
                "running should proceed"
        )

    def test_force_local_execution(self):
        self.n1.executor = Executor()
        out = self.n1.run(force_local_execution=False)
        with self.subTest("Test running with an executor fulfills promises"):
            self.assertIsInstance(
                out,
                Future,
                msg="With an executor, we expect a futures object back"
            )
            self.assertTrue(
                self.n1.running,
                msg="The running flag should be true while it's running, and "
                    "(de)serialization is time consuming enough that we still expect"
                    "this to be the case"
            )
            self.assertFalse(
                self.n1.ready,
                msg="While running, the node should not be ready."
            )
            with self.assertRaises(
                RuntimeError,
                msg="Running nodes should not be allowed to get their input updated",
            ):
                self.n1.inputs.x = 42
            self.assertEqual(
                1,
                out.result(timeout=120),
                msg="If we wait for the remote execution to finish, it should give us"
                    "the right thing"
            )
            self.assertEqual(
                1,
                self.n1.outputs.y.value,
                msg="The callback on the executor should ensure the output processing "
                    "happens"
            )

        self.n2.executor = Executor()
        self.n2.inputs.x = 0
        self.assertEqual(
            1,
            self.n2.run(fetch_input=False, force_local_execution=True),
            msg="Forcing local execution should do just that."
        )
        self.n1.executor_shutdown()
        self.n2.executor_shutdown()

    def test_emit_ran_signal(self):
        self.n1 >> self.n2 >> self.n3  # Chained connection declaration

        self.n1.run(emit_ran_signal=False)
        self.assertFalse(
            self.n3.inputs.x.ready,
            msg="Without emitting the ran signal, nothing should happen downstream"
        )

        self.n1.run(emit_ran_signal=True)
        self.assertEqual(
            add_one(add_one(add_one(self.n1.inputs.x.value))),
            self.n3.outputs.y.value,
            msg="With the connection and signal, we should have pushed downstream "
                "execution"
        )

    def test_execute(self):
        self.n1.outputs.y = 0  # Prime the upstream data source for fetching
        self.n2 >> self.n3
        self.assertEqual(
            self.n2.run(fetch_input=False, emit_ran_signal=False, x=10) + 1,
            self.n2.execute(x=11),
            msg="Execute should _not_ fetch in the upstream data"
        )
        self.assertFalse(
            self.n3.ready,
            msg="Executing should not be triggering downstream runs, even though we "
                "made a ran/run connection"
        )

        self.n2.inputs.x._value = "manually override the desired int"
        with self.assertRaises(
            TypeError,
            msg="Execute should be running without a readiness check and hitting the "
                "string + int error"
        ):
            self.n2.execute()

    def test_pull(self):
        self.n2 >> self.n3
        self.n1.inputs.x = 0
        by_run = self.n2.run(
                run_data_tree=True,
                fetch_input=True,
                emit_ran_signal=False
            )
        self.n1.inputs.x = 1
        self.assertEqual(
            by_run + 1,
            self.n2.pull(),
            msg="Pull should be running the upstream node"
        )
        self.assertFalse(
            self.n3.ready,
            msg="Pulling should not be triggering downstream runs, even though we "
                "made a ran/run connection"
        )

    def test___call__(self):
        # __call__ is just a pull that punches through macro walls, so we'll need to
        # test it again over in macro to really make sure it's working
        self.n2 >> self.n3
        self.n1.inputs.x = 0
        by_run = self.n2.run(
            run_data_tree=True,
            fetch_input=True,
            emit_ran_signal=False
        )
        self.n1.inputs.x = 1
        self.assertEqual(
            by_run + 1,
            self.n2(),
            msg="A call should be running the upstream node"
        )
        self.assertFalse(
            self.n3.ready,
            msg="Calling should not be triggering downstream runs, even though we "
                "made a ran/run connection"
        )

    def test_working_directory(self):
        self.assertTrue(
            self.n1._working_directory is None,
            msg="Sanity check -- No working directory should be made unless asked for"
        )
        self.assertFalse(
            os.path.isdir(self.n1.label),
            msg="Sanity check -- No working directory should be made unless asked for"
        )
        self.assertIsInstance(
            self.n1.working_directory,
            DirectoryObject,
            msg="Directory should be created on first access"
        )
        self.assertTrue(
            str(self.n1.working_directory.path).endswith(self.n1.label),
            msg="Directory name should be based off of label"
        )
        self.assertTrue(
            os.path.isdir(self.n1.label),
            msg="Now we asked for it, it should be there"
        )
        self.n1.working_directory.delete()
        self.assertFalse(
            os.path.isdir(self.n1.label),
            msg="Just want to make sure we cleaned up after ourselves"
        )

    def test_draw(self):
        try:
            self.n1.draw()
            self.assertFalse(
                any(self.n1.working_directory.path.iterdir())
            )

            for fmt in ["pdf", "png"]:
                with self.subTest(f"Testing with format {fmt}"):
                    if fmt == "pdf" and platform.system() == "Windows":
                        with self.assertRaises(
                            CalledProcessError,
                            msg="Graphviz doesn't seem to be happy about the "
                                "combindation PDF format and Windows right now. We "
                                "throw a warning for it in `Node.draw`, so if this "
                                "test ever fails and this combination _doesn't_ fail, "
                                "remove this extra bit of testing and remove the "
                                "warning."
                        ):
                            self.n1.draw(save=True, format=fmt)
                    else:
                        self.n1.draw(save=True, format=fmt)
                        expected_name = self.n1.label + "_graph." + fmt
                        # That name is just an implementation detail, update it as
                        # needed
                        self.assertTrue(
                            self.n1.working_directory.path.joinpath(
                                expected_name
                            ).is_file(),
                            msg="If `save` is called, expect the rendered image to "
                                "exist in the working directory"
                        )

            user_specified_name = "foo"
            self.n1.draw(filename=user_specified_name, format=fmt)
            expected_name = user_specified_name + "." + fmt
            self.assertTrue(
                self.n1.working_directory.path.joinpath(expected_name).is_file(),
                msg="If the user specifies a filename, we should assume they want the "
                    "thing saved"
            )
        finally:
            # No matter what happens in the tests, clean up after yourself
            self.n1.working_directory.delete()

    def test_run_after_init(self):
        self.assertIs(
            self.n1.outputs.y.value,
            NOT_DATA,
            msg="By default, nodes should not be getting run until asked"
        )
        self.assertEqual(
            1,
            ANode("right_away", run_after_init=True, x=0).outputs.y.value,
            msg="With run_after_init, the node should run right away"
        )

    def test_graph_info(self):
        n = ANode("n")

        self.assertEqual(
            n.semantic_delimiter + n.label,
            n.graph_path,
            msg="Lone nodes should just have their label as the path, as there is no "
                "parent above."
        )

        self.assertIs(
            n,
            n.graph_root,
            msg="Lone nodes should be their own graph_root, as there is no parent "
                "above."
        )

    def test_single_value(self):
        node = ANode("n")
        self.assertIs(
            node.outputs.y,
            node.channel,
            msg="With a single output, the `HasChannel` interface fulfillment should "
                "use that output."
        )

        with_addition = node + 5
        self.assertIsInstance(
            with_addition,
            Node,
            msg="With a single output, acting on the node should fall back on acting "
                "on the single (with-injection) output"
        )

        node2 = ANode("n2")
        node2.inputs.x = node
        self.assertListEqual(
            [node.outputs.y],
            node2.inputs.x.connections,
            msg="With a single output, the node should fall back on the single output "
                "for output-like use cases"
        )

        node.outputs["z"] = OutputDataWithInjection("z", node, type_hint=int)
        with self.assertRaises(
            AmbiguousOutputError,
            msg="With multiple outputs, trying to exploit the `HasChannel` interface "
                "should fail cleanly"
        ):
            node.channel

    @unittest.skipIf(sys.version_info >= (3, 11), "Storage should only work in 3.11+")
    def test_storage_failure(self):
        with self.assertRaises(
            NotImplementedError,
            msg="Storage is only available in python 3.11+, so we should fail hard and "
                "clean here"
        ):
            self.n1.storage

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_storage(self):
        self.assertIs(
            self.n1.outputs.y.value,
            NOT_DATA,
            msg="Sanity check on initial state"
        )
        y = self.n1()

        for backend in Node.allowed_backends():
            with self.subTest(backend):
                self.n1.storage_backend = backend
                try:
                    self.n1.save()

                    x = self.n1.inputs.x.value
                    reloaded = ANode(self.n1.label, x=x, storage_backend=backend)
                    self.assertEqual(
                        y,
                        reloaded.outputs.y.value,
                        msg="Nodes should load by default if they find a save file"
                    )

                    clean_slate = ANode(self.n1.label, x=x, overwrite_save=True)
                    self.assertIs(
                        clean_slate.outputs.y.value,
                        NOT_DATA,
                        msg="Users should be able to ignore a save"
                    )

                    run_right_away = ANode(
                        self.n1.label, x=x, run_after_init=True, storage_backend=backend
                    )
                    self.assertEqual(
                        y,
                        run_right_away.outputs.y.value,
                        msg="With nothing to load, running after init is fine"
                    )

                    run_right_away.save()
                    with self.assertRaises(
                        ValueError,
                        msg="Should be able to both immediately run _and_ load a node at "
                            "once"
                    ):
                        ANode(
                            self.n1.label, x=x, run_after_init=True, storage_backend=backend
                        )

                    force_run = ANode(
                        self.n1.label, x=x, run_after_init=True, overwrite_save=True
                    )
                    self.assertEqual(
                        y,
                        force_run.outputs.y.value,
                        msg="Destroying the save should allow immediate re-running"
                    )
                finally:
                    self.n1.delete_storage()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_save_after_run(self):
        for backend in Node.allowed_backends():
            with self.subTest(backend):
                try:
                    ANode("just_run", x=0, run_after_init=True, storage_backend=backend)
                    saves = ANode(
                        "run_and_save",
                        x=0,
                        run_after_init=True,
                        save_after_run=True,
                        storage_backend=backend
                    )
                    y = saves.outputs.y.value

                    not_reloaded = ANode("just_run", storage_backend=backend)
                    self.assertIs(
                        NOT_DATA,
                        not_reloaded.outputs.y.value,
                        msg="Should not have saved, therefore should have been nothing "
                            "to load"
                    )

                    find_saved = ANode("run_and_save", storage_backend=backend)
                    self.assertEqual(
                        y,
                        find_saved.outputs.y.value,
                        msg="Should have saved automatically after run, and reloaded "
                            "on instantiation"
                    )
                finally:
                    saves.storage.delete()  # Clean up


if __name__ == '__main__':
    unittest.main()
