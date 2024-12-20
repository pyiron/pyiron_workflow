import pickle
import unittest
from concurrent.futures import ThreadPoolExecutor
from itertools import product
from time import perf_counter

from pandas import DataFrame
from pyiron_snippets.dotdict import DotDict

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.nodes.for_loop import (
    MapsToNonexistentOutputError,
    UnmappedConflictError,
    dictionary_to_index_maps,
    for_node,
)
from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.macro import as_macro_node
from pyiron_workflow.nodes.standard import Add, Multiply, Sleep
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
                range(len(data["zipped2"])), strict=False
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

    @staticmethod
    def _get_column(
        output: DotDict,
        output_as_dataframe: bool,
        column_name: str ="together"
    ):
        """
        Facilitate testing different output types

        Args:
            output (DotDict): The result of running a for node.
            as_dataframe (bool): Whether the output is formatted as DF or list.
            column_name (str): The name of the output column you're looking for.
            (Default is "together" to work with the locally defined FiveTogether node.)

        Returns:

        """
        if output_as_dataframe:
            return output.df[column_name].to_list()
        else:  # output as lists
            return output[column_name]

    def test_iter_only(self):
        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                for_instance = for_node(
                    FiveTogether,
                    iter_on=("a", "b",),
                    a=[42, 43, 44],
                    b=[13, 14],
                    output_as_dataframe=output_as_dataframe,
                )
                self.assertIsNone(
                    for_instance.nrows,
                    msg="Haven't run yet, so there is no size"
                )
                self.assertIsNone(
                    for_instance.ncols,
                    msg="Haven't run yet, so there is no size"
                )
                out = for_instance(e="iter")
                self.assertIsInstance(
                    out[list(out.keys())[0]],
                    DataFrame if output_as_dataframe else list,
                    msg="Expected output type to correspond to boolean request"
                )
                self.assertEqual(
                    for_instance.nrows,
                    3 * 2,
                    msg="Expect nested loops"
                )
                self.assertEqual(
                    for_instance.ncols,
                    1 + 2,
                    msg="Dataframe should only hold output and _looped_ input"
                )
                self.assertTupleEqual(
                    self._get_column(out, output_as_dataframe=output_as_dataframe)[1],
                    ((42, 14, 2, 3, "iter"),),
                    msg="Iter should get nested, broadcast broadcast, else take default"
                )

    def test_zip_only(self):
        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                for_instance = for_node(
                    FiveTogether,
                    zip_on=("c", "d",),
                    e="zip",
                    output_as_dataframe=output_as_dataframe,
                )
                out = for_instance(c=[100, 101], d=[-1, -2, -3])
                self.assertEqual(
                    for_instance.nrows,
                    2,
                    msg="Expect zipping with the python convention of truncating to "
                        "shortest"
                )
                self.assertEqual(
                    for_instance.ncols,
                    1 + 2,
                    msg="Dataframe should only hold output and _looped_ input"
                )
                self.assertTupleEqual(
                    self._get_column(out, output_as_dataframe)[1],
                    ((0, 1, 101, -2, "zip"),),
                    msg="Zipped should get zipped, broadcast broadcast, else take default"
                )

    def test_iter_and_zip(self):
        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                for_instance = for_node(
                    FiveTogether,
                    iter_on=("a", "b",),
                    a=[42, 43, 44],
                    b=[13, 14],
                    zip_on=("c", "d",),
                    e="both",
                    output_as_dataframe=output_as_dataframe,
                )
                out = for_instance(c=[100, 101], d=[-1, -2, -3])
                self.assertEqual(
                    for_instance.nrows,
                    3 * 2 * 2,
                    msg="Zipped stuff is nested with the individually nested fields"
                )
                self.assertEqual(
                    for_instance.ncols,
                    1 + 4,
                    msg="Dataframe should only hold output and _looped_ input"
                )
                # We don't actually care if the order of nesting changes, but make sure the
                # iters are getting nested and zipped stay together
                self.assertTupleEqual(
                    self._get_column(out, output_as_dataframe)[0],
                    ((42, 13, 100, -1, "both"),),
                    msg="All start"
                )
                self.assertTupleEqual(
                    self._get_column(out, output_as_dataframe)[1],
                    ((42, 13, 101, -2, "both"),),
                    msg="Bump zipped together"
                )
                self.assertTupleEqual(
                    self._get_column(out, output_as_dataframe)[2],
                    ((42, 14, 100, -1, "both"),),
                    msg="Back to start of zipped, bump _one_ iter"
                )

    def test_dynamic_length(self):
        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                for_instance = for_node(
                    FiveTogether,
                    iter_on=("a", "b",),
                    a=[42, 43, 44],
                    b=[13, 14],
                    zip_on=("c", "d",),
                    c=[100, 101],
                    d=[-1, -2, -3],
                    output_as_dataframe=output_as_dataframe,
                )
                for_instance()
                self.assertEqual(
                    for_instance.nrows,
                    3 * 2 * 2,
                    msg="Sanity check"
                )
                for_instance(a=[0], b=[1], c=[2])
                self.assertEqual(
                    for_instance.nrows,
                    1,
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

        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
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
                        },
                        output_as_dataframe=output_as_dataframe,
                    )
                    for_instance()
                    self.assertEqual(
                        for_instance.ncols,
                        4 + 5,  # loop inputs + outputs
                        msg="When all conflicting names are remapped, we should have no "
                            "trouble"
                    )

                with self.subTest("Insufficient map"), self.assertRaises(
                        UnmappedConflictError,
                        msg="Leaving conflicting channels unmapped should raise an error"):
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
                        },
                        output_as_dataframe=output_as_dataframe,
                    )

                with self.subTest("Excessive map"), self.assertRaises(
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
                        },
                        output_as_dataframe=output_as_dataframe,
                    )

    def test_body_node_executor(self):
        t_sleep = 2

        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                for_parallel = for_node(
                    Sleep,
                    iter_on="t",
                    output_as_dataframe=output_as_dataframe,
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
                    msg=f"Parallelization over children should result in faster "
                        f"completion. Expected limit {grace} x {t_sleep} = "
                        f"{grace * t_sleep} -- got {dt}"
                )

                reloaded = pickle.loads(pickle.dumps(for_parallel))
                self.assertIsNone(
                    reloaded.body_node_executor,
                    msg="Just like regular nodes, until executors can be delayed creators "
                        "instead of actual executor nodes, we need to purge executors from "
                        "nodes on serialization or the thread lock/queue objects hit us"
                )

    def test_with_connections_dataframe(self):
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
                other=self.to_list,
                output_as_dataframe=True,
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

    def test_with_connections_list(self):
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
                other=self.to_list,
                output_as_dataframe=False,
            )
            out = self.loop.outputs.add
            return out

        x, y = [1], 2
        li = LoopInside([1], 2)
        li_out = li().out
        print(li)
        self.assertIsInstance(li_out, list)
        self.assertEqual(length_y * len(x), len(li_out))
        self.assertEqual(
            x[0] + y,
            li_out[0],
            msg="Just make sure the loop is actually running"
        )
        x, y = [2, 3], 4
        li_out = li(x, y).out
        self.assertEqual(length_y * len(x), len(li_out))
        self.assertEqual(
            x[-1] + y,
            li_out[len(li_out) - 1],
            msg="And make sure that we can vary the length still"
        )

    def test_node_access_points(self):
        n = FiveTogether(1, 2, e="instance")

        with self.subTest("Iter"):
            df = n.iter(c=[3, 4], d=[5, 6])
            self.assertIsInstance(df, DataFrame)
            self.assertEqual(2 * 2, len(df))
            self.assertTupleEqual(
                df["together"][1][0],
                (1, 2, 3, 6, "instance",)
            )

        with self.subTest("Zip"):
            df = n.zip(c=[3, 4], d=[5, 6])
            self.assertIsInstance(df, DataFrame)
            self.assertEqual(2, len(df))
            self.assertTupleEqual(
                df["together"][1][0],
                (1, 2, 4, 6, "instance",)
            )

    def test_shortcut(self):
        loop1 = Add.for_node(
            iter_on="other",
            obj=1,
            other=[1, 2],
            output_as_dataframe=False,
        )
        loop2 = Multiply.for_node(
            zip_on=("obj", "other"),
            obj=loop1.outputs.add,
            other=[1, 2],
            output_as_dataframe=False,
        )
        out = loop2()
        self.assertListEqual(
            out.mul,
            [(1+1)*1, (1+2)*2],
            msg="We should be able to call for_node right from node classes to bypass "
                "needing to provide the `body_node_class` argument"
        )

    def test_macro_body(self):
        for output_as_dataframe in [True, False]:
            with self.subTest(f"output_as_dataframe {output_as_dataframe}"):
                n = for_node(
                    body_node_class=self.AddThree,
                    iter_on="x",
                    x=[1, 2, 3],
                    output_as_dataframe=output_as_dataframe,
                )
                print(n.preview_io())
                n()
                pickle.loads(pickle.dumps(n))

    def test_repeated_creation(self):
        n1 = for_node(
            body_node_class=FiveTogether,
            iter_on="a",
            a=[1, 2],
            output_as_dataframe=True,
        )
        n2 = for_node(
            body_node_class=FiveTogether,
            iter_on="a",
            a=[1, 2],
            output_as_dataframe=False,
        )
        n3 = for_node(
            body_node_class=FiveTogether,
            iter_on="a",
            a=[1, 2],
            output_as_dataframe=True,
        )
        self.assertTrue(n1._output_as_dataframe)
        self.assertFalse(n2._output_as_dataframe)
        self.assertTrue(n3._output_as_dataframe)

    def test_executor_deserialization(self):

        for title, executor, expected in [
            ("Instance", ThreadPoolExecutor(), None),
            ("Instructions", (ThreadPoolExecutor, (), {}), (ThreadPoolExecutor, (), {}))
        ]:
            with self.subTest(title):
                n = for_node(
                    body_node_class=FiveTogether,
                    iter_on="a",
                    label=title,
                )
                n.body_node_executor = executor

                try:
                    n.save(backend="pickle")
                    n.load(backend="pickle")
                    self.assertEqual(
                        n.body_node_executor,
                        expected,
                        msg="Executor instances should get removed on "
                            "(de)serialization, but instructions on how to build one "
                            "should not."
                    )
                finally:
                    n.delete_storage()


if __name__ == "__main__":
    unittest.main()
