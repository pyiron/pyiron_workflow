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
            msg="Expected behaviour here is an autoencoder",
        )

        inp_class = inputs_to_list_factory(n, inp.__class__.__name__)
        out_class = list_to_outputs_factory(n, out.__class__.__name__)

        self.assertIs(
            inp_class,
            inp.__class__,
            msg="We can recover the constructed class from the factory",
        )
        self.assertIs(out_class, out.__class__)

        reloaded = pickle.loads(pickle.dumps(out))
        self.assertEqual(
            out.label, reloaded.label, msg="Transformers should be pickleable"
        )
        self.assertDictEqual(
            out.outputs.to_value_dict(),
            reloaded.outputs.to_value_dict(),
            msg="Transformers should be pickleable",
        )
