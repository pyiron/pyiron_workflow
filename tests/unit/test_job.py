from abc import ABC, abstractmethod
import sys
from time import sleep
import unittest

from pyiron_base import Project
from pyiron_workflow import Workflow
from pyiron_workflow.channels import NOT_DATA
import pyiron_workflow.job  # To get the job classes registered
from pyiron_workflow.node import Node


class _WithAJob(unittest.TestCase, ABC):
    @abstractmethod
    def make_a_job_from_node(self, node):
        pass

    def setUp(self) -> None:
        self.pr = Project("test")

    def tearDown(self) -> None:
        self.pr.remove_jobs(recursive=True, silently=True)
        self.pr.remove(enable=True)


class TestNodeOutputJob(_WithAJob):
    def make_a_job_from_node(self, node):
        job = self.pr.create.job.NodeOutputJob(node.label)
        job.input["node"] = node
        return job

    @unittest.skipIf(sys.version_info >= (3, 11), "Storage should only work in 3.11+")
    def test_clean_failure(self):
        with self.assertRaises(
            NotImplementedError,
            msg="Storage, and therefore node jobs, are only available in python 3.11+, "
                "so we should fail hard and clean here"
        ):
            node = Workflow.create.standard.UserInput(42)
            self.make_a_job_from_node(node)

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_node(self):
        node = Workflow.create.standard.UserInput(42)
        nj = self.make_a_job_from_node(node)

        self.assertIsInstance(nj.get_input_node(), Node, msg="Sanity check")

        nj.run()
        self.assertEqual(
            42,
            nj.output.user_input,
            msg="A single node should run just as well as a workflow"
        )

        self.assertIsInstance(
            nj.input["node"],
            str,
            msg="On saving, we convert the input to a bytestream so DataContainer can "
                "handle storing it."
        )
        self.assertIsInstance(
            nj.get_input_node(),
            Node,
            msg="But we might want to look at it again, so make sure this convenience "
                "method works."
        )

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_modal(self):
        modal_wf = Workflow("modal_wf")
        modal_wf.sleep = Workflow.create.standard.Sleep(0)
        modal_wf.out = modal_wf.create.standard.UserInput(modal_wf.sleep)
        nj = self.make_a_job_from_node(modal_wf)

        nj.run()
        self.assertTrue(
            nj.status.finished,
            msg="The interpreter should not release until the job is done"
        )
        self.assertEqual(
            0,
            nj.output.out__user_input,
            msg="The node should have run, and since it's modal there's no need to "
                "update the instance"
        )

        lj = self.pr.load(nj.job_name)
        self.assertIsNot(
            lj,
            nj,
            msg="The loaded job should be a new instance."
        )
        self.assertEqual(
            nj.output.out__user_input,
            lj.output.out__user_input,
            msg="The loaded job should still have all the same values"
        )

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_nonmodal(self):
        nonmodal_node = Workflow("non_modal")
        nonmodal_node.out = Workflow.create.standard.UserInput(42)

        nj = self.make_a_job_from_node(nonmodal_node)
        nj.run(run_mode="non_modal")
        self.assertFalse(
            nj.status.finished,
            msg=f"The local process should released immediately per non-modal "
                f"style, but got status {nj.status}"
        )
        while not nj.status.finished:
            sleep(0.1)
        self.assertTrue(
            nj.status.finished,
            msg="The job status should update on completion"
        )
        self.assertEqual(
            0,
            len(nj.output),
            msg="Non-modal stuff needs to be reloaded"
        )

        lj = self.pr.load(nj.job_name)
        self.assertEqual(
            42,
            lj.output.out__user_input,
            msg="The loaded job should have the finished values"
        )

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_bad_input(self):
        with self.subTest("Not a node"):
            nj = self.pr.create.job.NodeOutputJob("will_fail")
            nj.input["node"] = 42
            with self.assertRaises(TypeError, msg="The input is not a node"):
                nj.run()

        with self.subTest("Node not ready"):
            node = Workflow.create.standard.UserInput()  # No value!
            self.assertFalse(node.ready, msg="Sanity check")

            nj = self.make_a_job_from_node(node)
            with self.assertRaises(ValueError, msg="The input is not ready"):
                nj.run()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_unloadable(self):
        @Workflow.wrap.as_function_node("y")
        def not_importable_directy_from_module(x):
            return x + 1

        nj = self.make_a_job_from_node(not_importable_directy_from_module(42))
        with self.assertRaises(
            AttributeError,
            msg="On saving we cloudpickle the node, then at run time we try to "
                "recreate the dynamic class, but this doesn't work from the <locals> "
                "scope, i.e. when the function is nested inside another function."
        ):
            nj.run()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_shorter_name(self):
        j1 = self.pr.create.job.NodeOutputJob("foo")
        j2 = self.pr.create.job.NodeOutputJob("bar")
        self.assertIsInstance(j2, j1.__class__)


class TestStoredNodeJob(_WithAJob):
    def make_a_job_from_node(self, node):
        job = self.pr.create.job.StoredNodeJob(node.label)
        job.node = node
        return job

    @unittest.skipIf(sys.version_info >= (3, 11), "Storage should only work in 3.11+")
    def test_clean_failure(self):
        with self.assertRaises(
            NotImplementedError,
            msg="Storage, and therefore node jobs, are only available in python 3.11+, "
                "so we should fail hard and clean here"
        ):
            node = Workflow.create.standard.UserInput(42)
            self.make_a_job_from_node(node)

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_node(self):
        node = Workflow.create.standard.UserInput(42)
        nj = self.make_a_job_from_node(node)
        try:
            nj.run()
            self.assertEqual(
                42,
                nj.node.outputs.user_input.value,
                msg="A single node should run just as well as a workflow"
            )
        finally:
            try:
                node.storage.delete()
            except FileNotFoundError:
                pass

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_modal(self):
        modal_wf = Workflow("modal_wf")
        modal_wf.sleep = Workflow.create.standard.Sleep(0)
        modal_wf.out = modal_wf.create.standard.UserInput(modal_wf.sleep)
        nj = self.make_a_job_from_node(modal_wf)

        try:
            nj.run()
            self.assertTrue(
                nj.status.finished,
                msg="The interpreter should not release until the job is done"
            )
            self.assertEqual(
                0,
                nj.node.outputs.out__user_input.value,
                msg="The node should have run, and since it's modal there's no need to "
                    "update the instance"
            )

            lj = self.pr.load(nj.job_name)
            self.assertIsNot(
                lj,
                nj,
                msg="The loaded job should be a new instance."
            )
            self.assertEqual(
                nj.node.outputs.out__user_input.value,
                lj.node.outputs.out__user_input.value,
                msg="The loaded job should still have all the same values"
            )
        finally:
            modal_wf.storage.delete()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_nonmodal(self):
        nonmodal_node = Workflow("non_modal")
        nonmodal_node.out = Workflow.create.standard.UserInput(42)

        nj = self.make_a_job_from_node(nonmodal_node)

        try:
            nj.run(run_mode="non_modal")
            self.assertFalse(
                nj.status.finished,
                msg=f"The local process should released immediately per non-modal "
                    f"style, but got status {nj.status}"
            )
            while not nj.status.finished:
                sleep(0.1)
            self.assertTrue(
                nj.status.finished,
                msg="The job status should update on completion"
            )
            self.assertIs(
                nj.node.outputs.out__user_input.value,
                NOT_DATA,
                msg="As usual with remote processes, we expect to require a data read "
                    "before the local instance reflects its new state."
            )

            lj = self.pr.load(nj.job_name)
            self.assertEqual(
                42,
                lj.node.outputs.out__user_input.value,
                msg="The loaded job should have the finished values"
            )
        finally:
            nonmodal_node.storage.delete()

    @unittest.skipIf(sys.version_info < (3, 11), "Storage will only work in 3.11+")
    def test_bad_workflow(self):
        has_wd_wf = Workflow("not_empty")
        try:
            has_wd_wf.working_directory  # Touch the working directory, creating it
            with self.assertRaises(
                ValueError,
                msg="To make sure the node gets stored _inside_ the job, we only "
                    "accept the assignment of nodes who haven't looked at their working "
                    "directory yet"
            ):
                self.make_a_job_from_node(has_wd_wf)
        finally:
            has_wd_wf.working_directory.delete()
