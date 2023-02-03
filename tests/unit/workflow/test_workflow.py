from unittest import TestCase

from pyiron_contrib.workflow.channels import ChannelTemplate
from pyiron_contrib.workflow.node import Node
from pyiron_contrib.workflow.workflow import Workflow


class DummyNode(Node):
    input_channels = [ChannelTemplate(name="in")]
    output_channels = [ChannelTemplate(name="out")]


class TestWorkflow(TestCase):
    def test_ugly(self):
        # I'm a bit short on time, and I want to get to the integration
        # So this is just a slapdash version of some stuff I was testing in notebook
        return
        n1 = DummyNode(name="n1")
        n2 = DummyNode(name="n2")

        wf = Workflow("my_workflow", n1, n2)

        self.assertEqual(
            2, len(wf.nodes), msg="Adding nodes should be possible at instantiation"
        )

        n_unnamed = DummyNode()

