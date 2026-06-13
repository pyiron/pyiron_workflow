"""
A standard library of flowrep recipes built around fundamental python operations.
"""

import operator
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import flowrep as fr
from pyiron_snippets import versions

abs = fr.schemas.LabeledRecipe(
    label="abs",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.abs),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["absolute"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

add = fr.schemas.LabeledRecipe(
    label="add",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.add),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["added"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

index = fr.schemas.LabeledRecipe(
    label="index",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.index),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["index"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

inv = fr.schemas.LabeledRecipe(
    label="inv",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.inv),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["inverted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

invert = fr.schemas.LabeledRecipe(
    label="invert",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.invert),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["inverted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

neg = fr.schemas.LabeledRecipe(
    label="neg",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.neg),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["negative"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

pos = fr.schemas.LabeledRecipe(
    label="pos",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.pos),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["positive"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

not_ = fr.schemas.LabeledRecipe(
    label="not_",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.not_),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["negated"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

truth = fr.schemas.LabeledRecipe(
    label="truth",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.truth),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a"],
        outputs=["truth"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

length_hint = fr.schemas.LabeledRecipe(
    label="length_hint",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.length_hint),
            restricted_input_kinds={
                "obj": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["obj"],
        outputs=["length"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

sub = fr.schemas.LabeledRecipe(
    label="sub",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.sub),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["difference"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

isub = fr.schemas.LabeledRecipe(
    label="isub",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.isub),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["difference"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

iadd = fr.schemas.LabeledRecipe(
    label="iadd",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.iadd),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["added"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

mul = fr.schemas.LabeledRecipe(
    label="mul",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.mul),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["product"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

imul = fr.schemas.LabeledRecipe(
    label="imul",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.imul),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["product"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

floordiv = fr.schemas.LabeledRecipe(
    label="floordiv",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.floordiv),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["quotient"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ifloordiv = fr.schemas.LabeledRecipe(
    label="ifloordiv",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ifloordiv),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["quotient"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

truediv = fr.schemas.LabeledRecipe(
    label="truediv",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.truediv),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["quotient"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

itruediv = fr.schemas.LabeledRecipe(
    label="itruediv",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.itruediv),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["quotient"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

mod = fr.schemas.LabeledRecipe(
    label="mod",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.mod),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["remainder"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

imod = fr.schemas.LabeledRecipe(
    label="imod",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.imod),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["remainder"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

pow = fr.schemas.LabeledRecipe(
    label="pow",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.pow),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["power"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ipow = fr.schemas.LabeledRecipe(
    label="ipow",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ipow),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["power"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

and_ = fr.schemas.LabeledRecipe(
    label="and_",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.and_),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["conjunction"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

iand = fr.schemas.LabeledRecipe(
    label="iand",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.iand),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["conjunction"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

or_ = fr.schemas.LabeledRecipe(
    label="or_",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.or_),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["disjunction"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ior = fr.schemas.LabeledRecipe(
    label="ior",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ior),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["disjunction"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

xor = fr.schemas.LabeledRecipe(
    label="xor",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.xor),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["exclusive_or"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ixor = fr.schemas.LabeledRecipe(
    label="ixor",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ixor),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["exclusive_or"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

lshift = fr.schemas.LabeledRecipe(
    label="lshift",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.lshift),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["left_shifted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ilshift = fr.schemas.LabeledRecipe(
    label="ilshift",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ilshift),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["left_shifted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

rshift = fr.schemas.LabeledRecipe(
    label="rshift",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.rshift),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["right_shifted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

irshift = fr.schemas.LabeledRecipe(
    label="irshift",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.irshift),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["right_shifted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

matmul = fr.schemas.LabeledRecipe(
    label="matmul",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.matmul),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["matrix_product"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

imatmul = fr.schemas.LabeledRecipe(
    label="imatmul",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.imatmul),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["matrix_product"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

eq = fr.schemas.LabeledRecipe(
    label="eq",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.eq),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["equal"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ne = fr.schemas.LabeledRecipe(
    label="ne",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ne),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["not_equal"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

lt = fr.schemas.LabeledRecipe(
    label="lt",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.lt),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["less"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

le = fr.schemas.LabeledRecipe(
    label="le",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.le),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["less_equal"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

gt = fr.schemas.LabeledRecipe(
    label="gt",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.gt),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["greater"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

ge = fr.schemas.LabeledRecipe(
    label="ge",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.ge),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["greater_equal"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

is_ = fr.schemas.LabeledRecipe(
    label="is_",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.is_),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["identical"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

is_not = fr.schemas.LabeledRecipe(
    label="is_not",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.is_not),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["not_identical"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

contains = fr.schemas.LabeledRecipe(
    label="contains",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.contains),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["contains"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

countOf = fr.schemas.LabeledRecipe(
    label="countOf",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.countOf),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["count"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

indexOf = fr.schemas.LabeledRecipe(
    label="indexOf",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.indexOf),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["index"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

concat = fr.schemas.LabeledRecipe(
    label="concat",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.concat),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["concatenated"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

iconcat = fr.schemas.LabeledRecipe(
    label="iconcat",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.iconcat),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["concatenated"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

getitem = fr.schemas.LabeledRecipe(
    label="getitem",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.getitem),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["item"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

setitem = fr.schemas.LabeledRecipe(
    label="setitem",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.setitem),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "c": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b", "c"],
        outputs=["set"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

delitem = fr.schemas.LabeledRecipe(
    label="delitem",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.delitem),
            restricted_input_kinds={
                "a": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "b": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["a", "b"],
        outputs=["deleted"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

attrgetter = fr.schemas.LabeledRecipe(
    label="attrgetter",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.attrgetter),
            restricted_input_kinds={
                "attr": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["attr"],
        outputs=["getter"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

itemgetter = fr.schemas.LabeledRecipe(
    label="itemgetter",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.itemgetter),
            restricted_input_kinds={
                "item": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["item"],
        outputs=["getter"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)

methodcaller = fr.schemas.LabeledRecipe(
    label="methodcaller",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(operator.methodcaller),
            restricted_input_kinds={
                "name": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
            },
        ),
        inputs=["name"],
        outputs=["caller"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)


def _call_wrapper(
    obj: Callable,
    args_: Iterable[Any] | None = None,
    kwargs_: Mapping[str, Any] | None = None,
):
    args_ = () if args_ is None else args_
    kwargs_ = {} if kwargs_ is None else kwargs_
    return obj(*args_, **kwargs_)


call = fr.schemas.LabeledRecipe(
    label="call",
    node=fr.schemas.AtomicRecipe(
        reference=fr.schemas.PythonReference(
            info=versions.VersionInfo.of(_call_wrapper),
            restricted_input_kinds={
                "obj": fr.schemas.RestrictedParamKind.POSITIONAL_ONLY,
                "args_": fr.schemas.RestrictedParamKind.KEYWORD_ONLY,
                "kwargs_": fr.schemas.RestrictedParamKind.KEYWORD_ONLY,
            },
        ),
        inputs=["obj", "args_", "kwargs_"],
        outputs=["result"],
        unpack_mode=fr.schemas.UnpackMode.NONE,
    ),
)
