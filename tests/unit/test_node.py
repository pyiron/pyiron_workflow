from concurrent.futures import Future
import os
from sys import version_info
import unittest

from pyiron_workflow.channels import InputData, OutputData
from pyiron_workflow.files import DirectoryObject
from pyiron_workflow.interfaces import Executor
from pyiron_workflow.io import Inputs, Outputs
from pyiron_workflow.node import Node


def add_one(x):
    return x + 1


class ANode(Node):
    """To de-abstract the class"""

    def __init__(self, label):
        super().__init__(label=label)
        self._inputs = Inputs(InputData("x", self, type_hint=int))
        self._outputs = Outputs(OutputData("y", self, type_hint=int))

    @property
    def inputs(self) -> Inputs:
        return self._inputs

    @property
    def outputs(self) -> Inputs:
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


@unittest.skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestNode(unittest.TestCase):
    def setUp(self):
        n1 = ANode("start")
        n2 = ANode("middle")
        n3 = ANode("end")
        n1.inputs.x = 0
        n2.inputs.x = n1.outputs.y
        n3.inputs.x = n2.outputs.y
        self.n1 = n1
        self.n2 = n2
        self.n3 = n3

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

        with self.assertRaises(
            RuntimeError,
            msg="If we manage to run with bad input, being in a failed state still "
                "stops us"
        ):
            self.n3.run(run_data_tree=False, fetch_input=False, check_readiness=False)

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
        self.n1 > self.n2 > self.n3  # Chained connection declaration

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
        self.n2 > self.n3
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
        self.n2 > self.n3
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
        self.n2 > self.n3
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

