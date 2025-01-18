import pickle
import random
import unittest
from dataclasses import dataclass, field, is_dataclass

from pandas import DataFrame

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.transform import (
    Transformer,
    as_dataclass_node,
    dataclass_node,
    inputs_to_dataframe,
    inputs_to_dict,
    inputs_to_list,
    list_to_outputs,
)


@as_dataclass_node
class MyData:
    stuff: bool = False


@as_function_node
def Downstream(x: MyData.dataclass):
    x.stuff = True
    return x


class TestTransformer(unittest.TestCase):
    def test_pickle(self):
        n = inputs_to_list(3, "a", "b", "c", autorun=True)
        self.assertListEqual(["a", "b", "c"], n.outputs.list.value, msg="Sanity check")
        reloaded = pickle.loads(pickle.dumps(n))
        self.assertListEqual(
            n.outputs.list.value,
            reloaded.outputs.list.value,
            msg="Transformer nodes should be (un)pickleable",
        )
        self.assertIsInstance(reloaded, Transformer)

    def test_inputs_to_list(self):
        n = inputs_to_list(3, "a", "b", "c", autorun=True)
        self.assertListEqual(["a", "b", "c"], n.outputs.list.value)

    def test_list_to_outputs(self):
        lst = ["a", "b", "c", "d", "e"]
        n = list_to_outputs(len(lst), lst, autorun=True)
        self.assertEqual(lst, n.outputs.to_list())

    def test_inputs_to_dict(self):
        with self.subTest("List specification"):
            d = {"c1": 4, "c2": 5}
            n = inputs_to_dict(list(d.keys()), **d, autorun=True)
            self.assertDictEqual(
                d,
                n.outputs.dict.value,
                msg="Verify structure and ability to pass kwargs",
            )

        with self.subTest("Dict specification"):
            d = {"c1": 4, "c2": 5}
            default = 42
            hint = int
            spec = {k: (int, default) for k in d}
            n = inputs_to_dict(spec, autorun=True)
            self.assertIs(
                n.inputs[list(d.keys())[0]].type_hint,
                hint,
                msg="Spot check hint recognition",
            )
            self.assertDictEqual(
                {k: default for k in d},
                n.outputs.dict.value,
                msg="Verify structure and ability to pass defaults",
            )

        with self.subTest("Explicit suffix"):
            suffix = "MyName"
            n = inputs_to_dict(["c1", "c2"], class_name_suffix="MyName")
            self.assertTrue(n.__class__.__name__.endswith(suffix))

        with self.subTest("Only hashable"):
            unhashable_spec = {"c1": (list, ["an item"])}
            with self.assertRaises(
                ValueError,
                msg="List instances are not hashable, we should not be able to auto-"
                "generate a class name from this.",
            ):
                inputs_to_dict(unhashable_spec)

            n = inputs_to_dict(unhashable_spec, class_name_suffix="Bypass")
            self.assertListEqual(n.inputs.labels, list(unhashable_spec.keys()))
            key = list(unhashable_spec.keys())[0]
            self.assertIs(unhashable_spec[key][0], n.inputs[key].type_hint)
            self.assertListEqual(unhashable_spec[key][1], n.inputs[key].value)

    def test_inputs_to_dataframe(self):
        length = 3
        n = inputs_to_dataframe(length)
        n.recovery = None  # Some tests intentionally fail, and we don't want a file
        for i in range(length):
            n.inputs[f"row_{i}"] = {"x": i, "xsq": i * i}
        n()
        self.assertIsInstance(n.outputs.df.value, DataFrame, msg="Confirm output type")
        self.assertListEqual(
            [i * i for i in range(length)],
            n.outputs.df.value["xsq"].to_list(),
            msg="Spot check values",
        )

        d1 = {"a": 1, "b": 1}
        d2 = {"a": 1, "c": 2}
        with self.assertRaises(
            KeyError,
            msg="If the input rows don't have commensurate keys, we expect to get the "
            "relevant pandas error",
        ):
            n(row_0=d1, row_1=d1, row_2=d2)

        n = inputs_to_dataframe(length)  # Freshly instantiate to remove failed status
        n.recovery = None  # Next test intentionally fails, and we don't want a file
        d3 = {"a": 1}
        with self.assertRaises(
            ValueError,
            msg="If the input rows don't have commensurate length, we expect to get "
            "the relevant pandas error",
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

            class DC:
                """Doesn't even have to be an actual dataclass, just dataclass-like"""

                necessary: str
                with_default: int = 42
                with_factory: list = field(default_factory=some_generator)

            n = dataclass_node(DC, label="direct_instance")
            self.assertIs(
                n.dataclass, DC, msg="Underlying dataclass should be accessible"
            )
            self.assertTrue(
                is_dataclass(n.dataclass),
                msg="Underlying dataclass should be a real dataclass",
            )
            self.assertTrue(
                is_dataclass(DC),
                msg="Note that passing the underlying dataclass variable through the "
                "`dataclasses.dataclass` operator actually transforms it, so it "
                "too is now a real dataclass, even though it wasn't defined as "
                "one! This is just a side effect. I don't see it being harmful, "
                "but in case it gives some future reader trouble, I want to "
                "explicitly note the side effect here in the tests.",
            )
            self.assertListEqual(
                list(DC.__dataclass_fields__.keys()),
                n.inputs.labels,
                msg="Inputs should correspond exactly to fields",
            )
            self.assertIs(
                DC,
                n.outputs.dataclass.type_hint,
                msg="Output type hint should get automatically set",
            )
            key = random.choice(n.inputs.labels)
            self.assertIs(
                DC.__dataclass_fields__[key].type,
                n.inputs[key].type_hint,
                msg="Spot-check input type hints are pulled from dataclass fields",
            )
            self.assertFalse(
                n.inputs.necessary.ready,
                msg="Fields with no default and no default factory should not be ready",
            )
            self.assertTrue(
                n.inputs.with_default.ready, msg="Fields with default should be ready"
            )
            self.assertTrue(
                n.inputs.with_factory.ready,
                msg="Fields with default factory should be ready",
            )
            self.assertListEqual(
                n.inputs.with_factory.value,
                some_generator(),
                msg="Verify the generator is being used to set the intial value",
            )
            out = n(necessary="something")
            self.assertIsInstance(
                out, DC, msg="Node should output an instance of the dataclass"
            )

        with self.subTest("From decorator"):

            @as_dataclass_node
            @dataclass
            class DecoratedDC:
                necessary: str
                with_factory: list = field(default_factory=some_generator)
                with_default: int = 42

            @as_dataclass_node
            class DecoratedDCLike:
                necessary: str
                with_factory: list = field(default_factory=some_generator)
                with_default: int = 42

            for n_cls, style in zip(
                [DecoratedDC(label="dcinst"), DecoratedDCLike(label="dcinst")],
                ["Actual dataclass", "Dataclass-like class"],
                strict=False,
            ):
                with self.subTest(style):
                    self.assertTrue(
                        is_dataclass(n_cls.dataclass),
                        msg="Underlying dataclass should be available on node class",
                    )
                    prev = n_cls.preview_inputs()
                    key = random.choice(list(prev.keys()))
                    self.assertIs(
                        n_cls._dataclass_fields()[key].type,
                        prev[key][0],
                        msg="Spot-check input type hints are pulled from dataclass fields",
                    )
                    self.assertIs(
                        prev["necessary"][1], NOT_DATA, msg="Field has no default"
                    )
                    self.assertEqual(
                        n_cls._dataclass_fields()["with_default"].default,
                        prev["with_default"][1],
                        msg="Fields with default should get scraped",
                    )
                    self.assertIs(
                        prev["with_factory"][1],
                        NOT_DATA,
                        msg="Fields with default factory won't see their default until "
                        "instantiation",
                    )

    def test_dataclass_typing_and_storage(self):
        md = MyData()

        with self.assertRaises(TypeError, msg="Wrongly typed input should not connect"):
            Downstream(5)

        ds = Downstream(md)
        out = ds.pull()
        self.assertTrue(out.stuff, msg="Sanity check")

        rmd = pickle.loads(pickle.dumps(md))
        self.assertIs(
            rmd.outputs.dataclass.type_hint,
            MyData.dataclass,
            msg="Type hint should be findable on the scope of the node decorating it",
        )
        ds2 = Downstream(rmd)
        out = ds2.pull()
        self.assertTrue(
            out.stuff, msg="Flow should be able to survive (de)serialization"
        )


if __name__ == "__main__":
    unittest.main()
