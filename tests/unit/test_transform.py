import pickle
import unittest

from pyiron_workflow.transform import (
    Transformer,
    inputs_to_list,
    list_to_outputs,
)


class TestTransformer(unittest.TestCase):
    def test_pickle(self):
        n = inputs_to_list(3, "a", "b", "c", run_after_init=True)
        self.assertListEqual(
            ["a", "b", "c"],
            n.outputs.list.value,
            msg="Sanity check"
        )
        reloaded = pickle.loads(pickle.dumps(n))
        self.assertListEqual(
            n.outputs.list.value,
            reloaded.outputs.list.value,
            msg="Transformer nodes should be (un)pickleable"
        )
        self.assertIsInstance(reloaded, Transformer)

    def test_inputs_to_list(self):
        n = inputs_to_list(3, "a", "b", "c", run_after_init=True)
        self.assertListEqual(["a", "b", "c"], n.outputs.list.value)

    def test_list_to_outputs(self):
        l = ["a", "b", "c", "d", "e"]
        n = list_to_outputs(5, l, run_after_init=True)
        self.assertEqual(l, n.outputs.to_list())


if __name__ == '__main__':
    unittest.main()
