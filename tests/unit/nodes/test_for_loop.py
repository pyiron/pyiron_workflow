from concurrent.futures import ThreadPoolExecutor
from itertools import product
import pickle
from time import perf_counter
import unittest

from pandas import DataFrame

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.nodes.for_loop import (
    dictionary_to_index_maps,
    for_node,
    UnmappedConflictError,
    MapsToNonexistentOutputError
)
from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.macro import as_macro_node
from pyiron_workflow.nodes.standard import Add, Sleep
from pyiron_workflow.nodes.transform import inputs_to_list


class TestDictionaryToIndexMaps(unittest.TestCase):

    def test_no_keys(self):
        data = {"key": 5}
        with self.assertRaises(ValueError):
            dictionary_to_index_maps(data)

    def test_empty_nested_keys(self):
        data = {"key1": [1, 2, 3], "key2": [4, 5, 6]}
        with self.assertRaises(ValueError):
            dictionary_to_index_maps(data, nested_keys=())

    def test_empty_zipped_keys(self):
        data = {"key1": [1, 2, 3], "key2": [4, 5, 6]}
        with self.assertRaises(ValueError):
            dictionary_to_index_maps(data, zipped_keys=())

    def test_nested_non_iterable_data(self):
        data = {"key1": [1, 2, 3], "key2": 5}
        with self.assertRaises(TypeError):
            dictionary_to_index_maps(data, nested_keys=("key1", "key2"))

    def test_zipped_non_iterable_data(self):
        data = {"key1": [1, 2, 3], "key2": 5}
        with self.assertRaises(TypeError):
            dictionary_to_index_maps(data, zipped_keys=("key1", "key2"))

    def test_valid_data_nested_only(self):
        data = {"key1": [1, 2, 3], "key2": [4, 5]}
        nested_keys = ("key1", "key2")
        expected_maps = tuple(
            {nested_keys[i]: idx for i, idx in enumerate(indices)}
            for indices in product(range(len(data["key1"])), range(len(data["key2"])))
        )
        self.assertEqual(
            expected_maps,
            dictionary_to_index_maps(data, nested_keys=nested_keys),
        )

    def test_valid_data_zipped_only(self):
        data = {"key1": [1, 2, 3], "key2": [4, 5]}
        zipped_keys = ("key1", "key2")
        expected_maps = tuple(
            {key: idx for key in zipped_keys}
            for idx in range(min(len(data["key1"]), len(data["key2"])))
        )
        self.assertEqual(
            expected_maps,
            dictionary_to_index_maps(data, zipped_keys=zipped_keys),
        )

    def test_valid_data_nested_and_zipped(self):
        data = {
            "nested1": [2, 3],
            "nested2": [4, 5, 6],
            "zipped1": [7, 8, 9, 10],
            "zipped2": [11, 12, 13, 14, 15]
        }
        nested_keys = ("nested1", "nested2")
        zipped_keys = ("zipped1", "zipped2")
        expected_maps = tuple(
            {
                nested_keys[0]: n_idx,
                nested_keys[1]: n_idx2,
                zipped_keys[0]: z_idx,
                zipped_keys[1]: z_idx2
            }
            for n_idx, n_idx2 in product(
                range(len(data["nested1"])),
                range(len(data["nested2"]))
            )
            for z_idx, z_idx2 in zip(
                range(len(data["zipped1"])),
                range(len(data["zipped2"]))
            )
        )
        self.assertEqual(
            expected_maps,
            dictionary_to_index_maps(data, nested_keys=nested_keys, zipped_keys=zipped_keys),
        )


@as_function_node("together")
def FiveTogether(
    a: int = 0,
    b: int = 1,
    c: int = 2,
    d: int = 3,
    e: str = "foobar",
):
    return (a, b, c, d, e,),


class TestForNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        ensure_tests_in_python_path()
        from static.demo_nodes import AddThree
        cls.AddThree = AddThree

    def test_iter_only(self):
        for_instance = for_node(
            FiveTogether,
            iter_on=("a", "b",),
            a=[42, 43, 44],
            b=[13, 14],
        )
        out = for_instance(e="iter")
        self.assertIsInstance(out.df, DataFrame,)
        self.assertEqual(
            len(out.df),
            3 * 2,
            msg="Expect nested loops"
        )
        self.assertListEqual(
            out.df.columns.tolist(),
            ["a", "b", "together"],
            msg="Dataframe should only hold output and _looped_ input"
        )
        self.assertTupleEqual(
            out.df["together"][1],
            ((42, 14, 2, 3, "iter"),),
            msg="Iter should get nested, broadcast broadcast, else take default"
        )

    def test_zip_only(self):
        for_instance = for_node(
            FiveTogether,
            zip_on=("c", "d",),
            e="zip"
        )
        out = for_instance(c=[100, 101], d=[-1, -2, -3])
        self.assertEqual(
            len(out.df),
            2,
            msg="Expect zipping with the python convention of truncating to shortest"
        )
        self.assertListEqual(
            out.df.columns.tolist(),
            ["c", "d", "together"],
            msg="Dataframe should only hold output and _looped_ input"
        )
        self.assertTupleEqual(
            out.df["together"][1],
            ((0, 1, 101, -2, "zip"),),
            msg="Zipped should get zipped, broadcast broadcast, else take default"
        )

    def test_iter_and_zip(self):
        for_instance = for_node(
            FiveTogether,
            iter_on=("a", "b",),
            a=[42, 43, 44],
            b=[13, 14],
            zip_on=("c", "d",),
            e="both"
        )
        out = for_instance(c=[100, 101], d=[-1, -2, -3])
        self.assertEqual(
            len(out.df),
            3 * 2 * 2,
            msg="Zipped stuff is nested with the individually nested fields"
        )
        self.assertListEqual(
            out.df.columns.tolist(),
            ["a", "b", "c", "d", "together"],
            msg="Dataframe should only hold output and _looped_ input"
        )
        # We don't actually care if the order of nesting changes, but make sure the
        # iters are getting nested and zipped stay together
        self.assertTupleEqual(
            out.df["together"][0],
            ((42, 13, 100, -1, "both"),),
            msg="All start"
        )
        self.assertTupleEqual(
            out.df["together"][1],
            ((42, 13, 101, -2, "both"),),
            msg="Bump zipped together"
        )
        self.assertTupleEqual(
            out.df["together"][2],
            ((42, 14, 100, -1, "both"),),
            msg="Back to start of zipped, bump _one_ iter"
        )

    def test_dynamic_length(self):
        for_instance = for_node(
            FiveTogether,
            iter_on=("a", "b",),
            a=[42, 43, 44],
            b=[13, 14],
            zip_on=("c", "d",),
            c=[100, 101],
            d=[-1, -2, -3]
        )
        self.assertEqual(
            3 * 2 * 2,
            len(for_instance().df),
            msg="Sanity check"
        )
        self.assertEqual(
            1,
            len(for_instance(a=[0], b=[1], c=[2]).df),
            msg="Should be able to re-run with different input lengths"
        )

    def test_column_mapping(self):
        @as_function_node
        def FiveApart(
            a: int = 0,
            b: int = 1,
            c: int = 2,
            d: int = 3,
            e: str = "foobar",
        ):
            return a, b, c, d, e,

        with self.subTest("Successful map"):
            for_instance = for_node(
                FiveApart,
                iter_on=("a", "b"),
                zip_on=("c", "d"),
                a=[1, 2],
                b=[3, 4, 5],
                c=[7, 8],
                d=[9, 10, 11],
                e="e",
                output_column_map={
                    "a": "out_a",
                    "b": "out_b",
                    "c": "out_c",
                    "d": "out_d"
                }
            )
            self.assertEqual(
                4 + 5,  # loop inputs + outputs
                len(for_instance().df.columns),
                msg="When all conflicting names are remapped, we should have no trouble"
            )

        with self.subTest("Insufficient map"):
            with self.assertRaises(
                UnmappedConflictError,
                msg="Leaving conflicting channels unmapped should raise an error"
            ):
                for_node(
                    FiveApart,
                    iter_on=("a", "b"),
                    zip_on=("c", "d"),
                    a=[1, 2],
                    b=[3, 4, 5],
                    c=[7, 8],
                    d=[9, 10, 11],
                    e="e",
                    output_column_map={
                        # "a": "out_a",
                        "b": "out_b",
                        "c": "out_c",
                        "d": "out_d"
                    }
                )

        with self.subTest("Excessive map"):
            with self.assertRaises(
                MapsToNonexistentOutputError,
                msg="Trying to map something that isn't there should raise an error"
            ):
                for_node(
                    FiveApart,
                    iter_on=("a", "b"),
                    zip_on=("c", "d"),
                    a=[1, 2],
                    b=[3, 4, 5],
                    c=[7, 8],
                    d=[9, 10, 11],
                    e="e",
                    output_column_map={
                        "a": "out_a",
                        "b": "out_b",
                        "c": "out_c",
                        "d": "out_d",
                        "not_a_key_on_the_body_node_outputs": "anything"
                    }
                )

    def test_body_node_executor(self):
        t_sleep = 2
        for_parallel = for_node(
            Sleep,
            iter_on=("t",)
        )
        t_start = perf_counter()
        n_procs = 4
        with ThreadPoolExecutor(max_workers=n_procs) as exe:
            for_parallel.body_node_executor = exe
            for_parallel(t=n_procs*[t_sleep])
        dt = perf_counter() - t_start
        grace = 1.25
        self.assertLess(
            dt,
            grace * t_sleep,
            msg=f"Parallelization over children should result in faster completion. "
                f"Expected limit {grace} x {t_sleep} = {grace * t_sleep} -- got {dt}"
        )

        reloaded = pickle.loads(pickle.dumps(for_parallel))
        self.assertIsNone(
            reloaded.body_node_executor,
            msg="Just like regular nodes, until executors can be delayed creators "
                "instead of actual executor nodes, we need to purge executors from "
                "nodes on serialization or the thread lock/queue objects hit us"
        )

    def test_with_connections(self):
        length_y = 3

        @as_macro_node
        def LoopInside(self, x: list, y: int):
            self.to_list = inputs_to_list(
                length_y, y, y, y
            )
            self.loop = for_node(
                Add,
                iter_on=("obj", "other",),
                obj=x,
                other=self.to_list
            )
            return self.loop

        x, y = [1], 2
        li = LoopInside([1], 2)
        df = li().loop
        self.assertIsInstance(df, DataFrame)
        self.assertEqual(length_y * len(x), len(df))
        self.assertEqual(
            x[0] + y,
            df["add"][0],
            msg="Just make sure the loop is actually running"
        )
        x, y = [2, 3], 4
        df = li(x, y).loop
        self.assertEqual(length_y * len(x), len(df))
        self.assertEqual(
            x[-1] + y,
            df["add"][len(df) - 1],
            msg="And make sure that we can vary the length still"
        )

    def test_node_access_points(self):
        n = FiveTogether(1, 2, e="instance")

        with self.subTest("Iter"):
            df = n.iter(c=[3, 4], d=[5, 6])
            self.assertIsInstance(df, DataFrame)
            self.assertEqual(2 * 2, len(df))
            self.assertTupleEqual(df["together"][1][0], (1, 2, 3, 6, "instance",))

        with self.subTest("Zip"):
            df = n.zip(c=[3, 4], d=[5, 6])
            self.assertIsInstance(df, DataFrame)
            self.assertEqual(2, len(df))
            self.assertTupleEqual(df["together"][1][0], (1, 2, 4, 6, "instance",))

    def test_macro_body(self):
        n = for_node(
            body_node_class=self.AddThree,
            iter_on=("x",),
            x=[1, 2, 3]
        )
        n()
        pickle.loads(pickle.dumps(n))


if __name__ == "__main__":
    unittest.main()
