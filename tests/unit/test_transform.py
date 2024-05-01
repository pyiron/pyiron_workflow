import pickle
import unittest

from pyiron_workflow.transform import (
    Transformer,
    inputs_to_dict,
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

    def test_inputs_to_dict(self):
        with self.subTest("List specification"):
            d = {"c1": 4, "c2": 5}
            n = inputs_to_dict(list(d.keys()), **d, run_after_init=True)
            self.assertDictEqual(
                d,
                n.outputs.dict.value,
                msg="Verify structure and ability to pass kwargs"
            )

        with self.subTest("Dict specification"):
            d = {"c1": 4, "c2": 5}
            default = 42
            hint = int
            spec = {k: (int, default) for k in d.keys()}
            n = inputs_to_dict(spec, run_after_init=True)
            self.assertIs(
                n.inputs[list(d.keys())[0]].type_hint,
                hint,
                msg="Spot check hint recognition"
            )
            self.assertDictEqual(
                {k: default for k in d.keys()},
                n.outputs.dict.value,
                msg="Verify structure and ability to pass defaults"
            )

        with self.subTest("Explicit suffix"):
            suffix = "MyName"
            n = inputs_to_dict(["c1", "c2"], class_name_suffix="MyName")
            self.assertTrue(
                n.__class__.__name__.endswith(suffix)
            )

        with self.subTest("Only hashable"):
            unhashable_spec = {"c1": (list, ["an item"])}
            with self.assertRaises(
                ValueError,
                msg="List instances are not hashable, we should not be able to auto-"
                    "generate a class name from this."
            ):
                inputs_to_dict(unhashable_spec)

            n = inputs_to_dict(unhashable_spec, class_name_suffix="Bypass")
            self.assertListEqual(n.inputs.labels, list(unhashable_spec.keys()))
            key = list(unhashable_spec.keys())[0]
            self.assertIs(unhashable_spec[key][0], n.inputs[key].type_hint)
            self.assertListEqual(unhashable_spec[key][1], n.inputs[key].value)


if __name__ == '__main__':
    unittest.main()
