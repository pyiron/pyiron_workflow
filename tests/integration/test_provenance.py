import unittest
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from pyiron_workflow.workflow import Workflow


class TestProvenance(unittest.TestCase):
    """
    Verify that the post-facto provenance record works, even under complex conditions
    like nested composites and executors.
    """

    def setUp(self) -> None:
        @Workflow.wrap.as_function_node
        def Slow(t):
            sleep(t)
            return t

        @Workflow.wrap.as_macro_node
        def Provenance(self, t):
            self.fast = Workflow.create.standard.UserInput(t)
            self.slow = Slow(t)
            self.double = self.fast + self.slow
            return self.double

        wf = Workflow("provenance")
        wf.time = Workflow.create.standard.UserInput(2)
        wf.prov = Provenance(t=wf.time)
        wf.post = wf.prov + 2
        self.wf = wf
        self.expected_post = {
            wf.post.scoped_label: (2 * wf.time.inputs.user_input.value) + 2
        }

    def test_executed_provenance(self):
        with ThreadPoolExecutor() as exe:
            self.wf.prov.executor = exe
            out = self.wf()

        self.assertDictEqual(
            self.expected_post, out, msg="Sanity check that the graph is executing ok"
        )

        self.assertListEqual(
            ["time", "prov", "post"],
            self.wf.provenance_by_execution,
            msg="Even with a child running on an executor, provenance should log",
        )

        self.assertListEqual(
            self.wf.provenance_by_execution,
            self.wf.provenance_by_completion,
            msg="The workflow itself is serial and these should be identical.",
        )

        self.assertListEqual(
            ["t", "slow", "fast", "double"],
            self.wf.prov.provenance_by_execution,
            msg="Later connections get priority over earlier connections, so we expect "
                "the t-node to trigger 'slow' before 'fast'",
        )

        self.assertListEqual(
            self.wf.prov.provenance_by_execution,
            self.wf.prov.provenance_by_completion,
            msg="The macro is running on an executor, but its children are in serial,"
                "so completion and execution order should be the same",
        )

    def test_execution_vs_completion(self):
        with ThreadPoolExecutor(max_workers=2) as exe:
            self.wf.prov.fast.executor = exe
            self.wf.prov.slow.executor = exe
            out = self.wf()

        self.assertDictEqual(
            self.expected_post, out, msg="Sanity check that the graph is executing ok"
        )

        self.assertListEqual(
            ["t", "slow", "fast", "double"],
            self.wf.prov.provenance_by_execution,
            msg="Later connections get priority over earlier connections, so we expect "
                "the t-node to trigger 'slow' before 'fast'",
        )

        self.assertListEqual(
            ["t", "fast", "slow", "double"],
            self.wf.prov.provenance_by_completion,
            msg="Since 'slow' is slow it shouldn't _finish_ until after 'fast' (but "
                "still before 'double' since 'double' depends on 'slow')",
        )

        self.assertListEqual(
            self.wf.provenance_by_execution,
            self.wf.provenance_by_completion,
            msg="The workflow itself is serial and these should be identical.",
        )


if __name__ == "__main__":
    unittest.main()
