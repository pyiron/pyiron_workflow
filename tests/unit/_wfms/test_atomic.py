from __future__ import annotations

import dataclasses
import unittest

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow._wfms import atomic, execution, transformers
from tests.unit._wfms import _fixtures

# --------------------------------------------------------------------------- #
# Module-level helpers (must be importable by flowrep / VersionInfo).         #
# --------------------------------------------------------------------------- #


def with_default(x, y=10):
    """Tiny callable with a parameter default — used for the default-fallback case."""
    return x + y


def add_no_default(x, y):
    """Variant of `add` without a default — used for the missing-value case."""
    return x + y


def func_with_metadata(x):
    """Stand-in for a flowrep-decorated function carrying `_semantikon_metadata`."""
    return x


# Sentinel attached at module load — the value is matched by identity in tests.
_METADATA_SENTINEL = object()
func_with_metadata._semantikon_metadata = _METADATA_SENTINEL  # type: ignore[attr-defined]
# Brittly depends on semantikon attaching metadata to this particular attribute


@dataclasses.dataclass
class _Pair:
    """Two-field dataclass used for the `UnpackMode.DATACLASS` case."""

    a: int
    b: int


def _make_pair() -> _Pair:
    """Returns a `_Pair` instance — module-level so flowrep can resolve it."""
    return _Pair(a=0, b=0)


# --------------------------------------------------------------------------- #
# Tiny recipe builders.                                                       #
# --------------------------------------------------------------------------- #


def _atomic_recipe(
    func,
    *,
    inputs: list[str],
    outputs: list[str],
    restricted: dict[str, fr.schemas.RestrictedParamKind] | None = None,
    inputs_with_defaults: list[str] | None = None,
) -> fr.schemas.AtomicRecipe:
    reference = fr.schemas.PythonReference(
        info=versions.VersionInfo.of(func),
        inputs_with_defaults=inputs_with_defaults or [],
        restricted_input_kinds=restricted or {},
    )
    return fr.schemas.AtomicRecipe(
        reference=reference,
        inputs=inputs,
        outputs=outputs,
    )


# --------------------------------------------------------------------------- #
# Atomic.__init__ / function_metadata / _result_type / evaluate               #
# --------------------------------------------------------------------------- #


class TestAtomicInit(unittest.TestCase):
    def test_function_metadata_none_when_absent(self) -> None:
        node = _fixtures.atomic_add_node()
        self.assertIsNone(node.function_metadata)

    def test_function_metadata_captures_semantikon_sentinel(self) -> None:
        recipe = _atomic_recipe(
            func_with_metadata,
            inputs=["x"],
            outputs=["out"],
        )
        node = atomic.Atomic(recipe, "lbl")
        self.assertIs(node.function_metadata, _METADATA_SENTINEL)


class TestAtomicResultType(unittest.TestCase):
    def test_result_type_is_live_atomic(self) -> None:
        self.assertIs(atomic.Atomic._result_type(), fr.schemas.AtomicData)


class TestAtomicEvaluate(unittest.TestCase):
    def test_evaluate_runs_add_to_finished(self) -> None:
        node = _fixtures.atomic_add_node()
        run = node.run(x=1, y=2)
        self.assertEqual(run.status, execution.RunStatus.FINISHED)
        # `add`'s single output is named `output_0` by flowrep.
        (only_name,) = run.outputs.keys()
        self.assertEqual(run.outputs[only_name], 3)


# --------------------------------------------------------------------------- #
# _call_atomic                                                                #
# --------------------------------------------------------------------------- #


class TestCallAtomic(unittest.TestCase):
    def test_positional_only_routing(self) -> None:
        # `Transform1toN` marks `items` as POSITIONAL_ONLY, so the value
        # must be routed through `*positional` rather than `**keyword`.
        node = transformers.Transform1toN(2).node("split")
        live = node.generate_flowrep_live_node()
        live.input_ports["items"].value = [1, 2]
        result = atomic._call_atomic(live)
        self.assertEqual(result, (1, 2))

    def test_keyword_routing(self) -> None:
        node = _fixtures.atomic_add_node()
        live = node.generate_flowrep_live_node()
        live.input_ports["x"].value = 1
        live.input_ports["y"].value = 2
        self.assertEqual(atomic._call_atomic(live), 3)

    def test_default_fallback(self) -> None:
        recipe = _atomic_recipe(
            with_default,
            inputs=["x", "y"],
            outputs=["out"],
            inputs_with_defaults=["y"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        live.input_ports["x"].value = 1
        # `y` is left as NotData; the recipe carries a default of 10.
        self.assertEqual(atomic._call_atomic(live), 11)

    def test_missing_value_raises_value_error(self) -> None:
        recipe = _atomic_recipe(
            add_no_default,
            inputs=["x", "y"],
            outputs=["out"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        live.input_ports["x"].value = 1
        # `y` has neither value nor default.
        with self.assertRaises(ValueError) as ctx:
            atomic._call_atomic(live)
        self.assertIn("'y'", str(ctx.exception))


# --------------------------------------------------------------------------- #
# _store_atomic_outputs                                                       #
# --------------------------------------------------------------------------- #


class TestStoreAtomicOutputs(unittest.TestCase):
    def test_unpack_none_writes_whole_result_to_single_port(self) -> None:
        recipe = _atomic_recipe(
            with_default,
            inputs=["x"],
            outputs=["out"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        payload = [1, 2, 3]
        atomic._store_atomic_outputs(live, payload)
        self.assertEqual(live.output_ports["out"].value, payload)

    def test_unpack_tuple_single_output_writes_whole_tuple(self) -> None:
        recipe = _atomic_recipe(
            with_default,
            inputs=["x"],
            outputs=["out"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        payload = (1, 2, 3)
        atomic._store_atomic_outputs(live, payload)
        self.assertEqual(live.output_ports["out"].value, payload)

    def test_unpack_tuple_multi_output_distributes_elements(self) -> None:
        recipe = _atomic_recipe(
            with_default,
            inputs=["x"],
            outputs=["a", "b", "c"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        atomic._store_atomic_outputs(live, (1, 2, 3))
        self.assertEqual(live.output_ports["a"].value, 1)
        self.assertEqual(live.output_ports["b"].value, 2)
        self.assertEqual(live.output_ports["c"].value, 3)

    def test_unpack_tuple_multi_output_length_mismatch_raises(self) -> None:
        recipe = _atomic_recipe(
            with_default,
            inputs=["x"],
            outputs=["a", "b", "c"],
        )
        live = fr.schemas.AtomicData.from_recipe(recipe)
        with self.assertRaises(ValueError):
            atomic._store_atomic_outputs(live, (1, 2))


if __name__ == "__main__":
    unittest.main()
