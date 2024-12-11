import pickle
import unittest

from pyiron_workflow import as_macro_node
from pyiron_workflow import standard_nodes as std


@as_macro_node("greater")
def AddWhileLessThan(self, a, b, cap):
    """
    Add :param:`b` to :param:`a` while the sum is less than or equal to :param:`cap`.

    A simple but complete demonstrator for how to construct cyclic flows, including
    logging key outputs during the loop.
    """
    # Bespoke logic
    self.body = std.Add(obj=a, other=b)
    self.body.inputs.obj = self.body.outputs.add  # Higher priority connection
    # The output is NOT_DATA on the first pass and `a` gets used,
    # But after that the node will find and use its own output
    self.condition = std.LessThan(self.body, cap)

    # Universal logic
    self.switch = std.If()
    self.switch.inputs.condition = self.condition

    self.starting_nodes = [self.body]
    self.body >> self.condition >> self.switch
    self.switch.signals.output.true >> self.body

    # Bespoke logging
    self.history = std.AppendToList()
    self.history.inputs.existing = self.history
    self.history.inputs.new_element = self.body
    self.body >> self.history

    # Returns are pretty universal for single-value body nodes,
    # assuming a log of the history is not desired as output,
    # but in general return values are also bespoke
    return self.body


class TestWhileLoop(unittest.TestCase):
    def test_while_loop(self):
        a, b, cap = 0, 2, 5
        n = AddWhileLessThan(a, b, cap, autorun=True)
        self.assertGreaterEqual(
            6,
            n.outputs.greater.value,
            msg="Verify output"
        )
        self.assertListEqual(
            [2, 4, 6],
            n.history.outputs.list.value,
            msg="Verify loop history logging"
        )
        self.assertListEqual(
            [
                'body',
                'history',
                'condition',
                'switch',
                'body',
                'history',
                'condition',
                'switch',
                'body',
                'history',
                'condition',
                'switch'
            ],
            n.provenance_by_execution,
            msg="Verify execution order -- the same nodes get run repeatedly in acyclic"
        )
        reloaded = pickle.loads(pickle.dumps(n))
        self.assertListEqual(
            reloaded.history.outputs.list.value,
            n.history.outputs.list.value,
            msg="Should be able to save and re-load cyclic graphs just like usual"
        )
