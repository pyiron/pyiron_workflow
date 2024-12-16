import pickle
import unittest

from pyiron_workflow.nodes.transform import (
    inputs_to_list,
    inputs_to_list_factory,
    list_to_outputs,
    list_to_outputs_factory,
)


class TestTransform(unittest.TestCase):
    def test_list(self):
        n = 3
        inp = inputs_to_list(n, *list(range(n)), label="inp")
        out = list_to_outputs(n, inp, label="out")
        out()
        self.assertListEqual(
            list(range(3)),
            out.outputs.to_list(),
            msg="Expected behaviour here is an autoencoder"
        )

        inp_class = inputs_to_list_factory(n)
        out_class = list_to_outputs_factory(n)

        self.assertIs(
            inp_class,
            inp.__class__,
            msg="Regardless of origin, we expect to be constructing the exact same "
                "class"
        )
        self.assertIs(out_class, out.__class__)

        reloaded = pickle.loads(pickle.dumps(out))
        self.assertEqual(
            out.label,
            reloaded.label,
            msg="Transformers should be pickleable"
        )
        self.assertDictEqual(
            out.outputs.to_value_dict(),
            reloaded.outputs.to_value_dict(),
            msg="Transformers should be pickleable"
        )
