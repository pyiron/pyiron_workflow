from dataclasses import dataclass, field, is_dataclass
import pickle
import random
import unittest

from pandas import DataFrame

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.transform import (
    Transformer,
    as_dataclass_node,
    dataclass_node,
    inputs_to_dataframe,
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

    def test_inputs_to_dataframe(self):
        l = 3
        n = inputs_to_dataframe(l)
        for i in range(l):
            n.inputs[f"row_{i}"] = {"x": i, "xsq": i*i}
        n()
        self.assertIsInstance(
            n.outputs.df.value,
            DataFrame,
            msg="Confirm output type"
        )
        self.assertListEqual(
            [i*i for i in range(3)],
            n.outputs.df.value["xsq"].to_list(),
            msg="Spot check values"
        )

        d1 = {"a": 1, "b": 1}
        d2 = {"a": 1, "c": 2}
        with self.assertRaises(
            KeyError,
            msg="If the input rows don't have commensurate keys, we expect to get the "
                "relevant pandas error"
        ):
            n(row_0=d1, row_1=d1, row_2=d2)

        n = inputs_to_dataframe(l)  # Freshly instantiate to remove failed status
        d3 = {"a": 1}
        with self.assertRaises(
            ValueError,
            msg="If the input rows don't have commensurate length, we expect to get "
                "the relevant pandas error"
        ):
            n(row_0=d1, row_1=d3, row_2=d1)

    def test_dataclass_node(self):
        # Note: We'd need to declare the generator and classes outside the <locals> of
        # this test function if we wanted them to be pickleable, but we test the
        # pickleability of transformers elsewhere so just keep stuff tidy by declaring
        # locally for this test

        def some_generator():
            return [1, 2, 3]

        with self.subTest("From instantiator"):
            @dataclass
            class DC:
                necessary: str
                with_default: int = 42
                with_factory: list = field(default_factory=some_generator)

            n = dataclass_node(DC, label="direct_instance")
            self.assertIs(
                n.dataclass,
                DC,
                msg="Underlying dataclass should be accessible"
            )
            self.assertListEqual(
                list(DC.__dataclass_fields__.keys()),
                n.inputs.labels,
                msg="Inputs should correspond exactly to fields"
            )
            self.assertIs(
                DC,
                n.outputs.dataclass.type_hint,
                msg="Output type hint should get automatically set"
            )
            key = random.choice(n.inputs.labels)
            self.assertIs(
                DC.__dataclass_fields__[key].type,
                n.inputs[key].type_hint,
                msg="Spot-check input type hints are pulled from dataclass fields"
            )
            self.assertFalse(
                n.inputs.necessary.ready,
                msg="Fields with no default and no default factory should not be ready"
            )
            self.assertTrue(
                n.inputs.with_default.ready,
                msg="Fields with default should be ready"
            )
            self.assertTrue(
                n.inputs.with_factory.ready,
                msg="Fields with default factory should be ready"
            )
            self.assertListEqual(
                n.inputs.with_factory.value,
                some_generator(),
                msg="Verify the generator is being used to set the intial value"
            )
            out = n(necessary="something")
            self.assertIsInstance(
                out,
                DC,
                msg="Node should output an instance of the dataclass"
            )

        with self.subTest("From decorator"):
            @as_dataclass_node
            @dataclass
            class DecoratedDC:
                necessary: str
                with_default: int = 42
                with_factory: list = field(default_factory=some_generator)

            n_cls = DecoratedDC(label="decorated_instance")

            self.assertTrue(
                is_dataclass(n_cls.dataclass),
                msg="Underlying dataclass should be available on node class"
            )
            prev = n_cls.preview_inputs()
            key = random.choice(list(prev.keys()))
            self.assertIs(
                n_cls._dataclass_fields[key].type,
                prev[key][0],
                msg="Spot-check input type hints are pulled from dataclass fields"
            )
            self.assertIs(
                prev["necessary"][1],
                NOT_DATA,
                msg="Field has no default"
            )
            self.assertEqual(
                n_cls._dataclass_fields["with_default"].default,
                prev["with_default"][1],
                msg="Fields with default should get scraped"
            )
            self.assertIs(
                prev["with_factory"][1],
                NOT_DATA,
                msg="Fields with default factory won't see their default until "
                    "instantiation"
            )



if __name__ == '__main__':
    unittest.main()
