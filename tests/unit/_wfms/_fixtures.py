"""
Shared test fixtures for `_wfms` unit tests.

These are kept in a single module so individual test files do not redefine — and
possibly drift from — the same flowrep-decorated functions. Note that flowrep parses
the *source* of a decorated function via `inspect.getsource`, so these functions
must live in a real `.py` file (not in a `python -c` string or test scope).

Usage:
>>>    from tests.unit._wfms import _fixtures
>>>    n = _fixtures.atomic_add_node()
>>>    run = n.run(x=1, y=2)
"""

from __future__ import annotations

import flowrep as fr
import semantikon
from flowrep.api import schemas as frs
from pyiron_snippets import versions

from pyiron_workflow._wfms import api as wfms
from pyiron_workflow._wfms import transformers

# --------------------------------------------------------------------------- #
# Atomic recipes                                                              #
# --------------------------------------------------------------------------- #


@fr.atomic
def add(x, y):
    return x + y


@fr.atomic
def sub(x, y):
    return x - y


@fr.atomic
def identity(x):
    return x


@fr.atomic
def negate(x):
    return -x


@fr.atomic
def is_positive(n):
    return n > 0


@fr.atomic
def is_negative(n):
    return n < 0


@fr.atomic
def multiply_with_defaults(x=1, y=2):
    return x * y


# --------------------------------------------------------------------------- #
# Macro recipes                                                               #
# --------------------------------------------------------------------------- #


@fr.workflow
def macro(x, y, z):
    a = add(x, y)
    s = sub(a, z)
    return a, s


@fr.workflow
@semantikon.meta(uri="This is decorated")
def annotated_macro(x, y, z):
    a = add(x, y)
    s = sub(a, z)
    return a, s


@fr.workflow
def nested_macro(x, y):
    z = add(x, y)
    a, s = macro(x, y, z)
    return a, s


@fr.workflow
def passthrough(x, y):
    """
    Macro that wires a parent input directly to a parent output.

    Exercises the `InputSource` branch of :func:`dag.populate_outputs`.
    """
    s = add(x, y)
    return x, s


@fr.workflow
def container():
    m = multiply_with_defaults()
    return m


# --------------------------------------------------------------------------- #
# Autoencoder (round-trips TransformNto1 -> Transform1toN)                    #
# --------------------------------------------------------------------------- #

_COMPRESS = transformers.TransformNto1(3)
_EXPAND = transformers.Transform1toN(3)


@fr.workflow
def autoencoder(a, b, c):
    listy = _COMPRESS.recipe(item_0=a, item_1=b, item_2=c)
    x, y, z = _EXPAND.recipe(items=listy)
    return x, y, z


# --------------------------------------------------------------------------- #
# If-flow workflow                                                            #
# --------------------------------------------------------------------------- #


@fr.workflow
def if_abs(x):
    if is_positive(x):  # noqa: SIM108  (parsed as IfNode; ternary would collapse it)
        y = identity(x)
    else:
        y = negate(x)
    return y


def if_recipe() -> frs.IfRecipe:
    """
    Minimal `IfNode` whose condition/body both wrap `add`.

    Promoted here from `test_constructors.py` so the if-flow tests share a
    single source of truth for the canonical recipe shape.
    """
    add_recipe = add.flowrep_recipe
    return frs.IfRecipe(
        inputs=["x", "y"],
        outputs=["out"],
        cases=[
            frs.ConditionalCase(
                condition=frs.LabeledRecipe(label="cond", node=add_recipe),
                body=frs.LabeledRecipe(label="body", node=add_recipe),
            )
        ],
        input_edges={
            frs.TargetHandle(node="cond", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="cond", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="body", port="y"): frs.InputSource(port="y"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="out"): [
                frs.SourceHandle(node="body", port="output_0")
            ],
        },
    )


# --------------------------------------------------------------------------- #
# While-flow workflow                                                         #
# --------------------------------------------------------------------------- #


@fr.atomic
def decrement(x):
    return x - 1


@fr.workflow
def while_countdown(n):
    while is_positive(n):
        n = decrement(n)
    return n


# --------------------------------------------------------------------------- #
# For-each workflow                                                           #
# --------------------------------------------------------------------------- #


@fr.workflow
def for_wf(xs, ys, z):
    sums = []
    x_used = []
    y_used = []
    for x in xs:
        for y in ys:
            _, s = macro(x, y, z)
            sums.append(s)
            x_used.append(x)
            y_used.append(y)
    return x_used, y_used, sums


# --------------------------------------------------------------------------- #
# Constructor helpers                                                         #
# --------------------------------------------------------------------------- #


def atomic_add_node(label: str = "add"):
    """Return a fresh `Atomic` wrapping `add`."""
    return wfms.function2node(add, label)


def atomic_sub_node(label: str = "sub"):
    """Return a fresh `Atomic` wrapping `sub`."""
    return wfms.function2node(sub, label)


def macro_node(label: str = "my_macro"):
    """Return a fresh `Macro` wrapping `macro`."""
    return wfms.function2node(macro, label)


def annotated_macro_node(label: str = "my_annotated_macro"):
    """Return a fresh `Macro` wrapping `annotated_macro`."""
    return wfms.function2node(annotated_macro, label)


def nested_macro_node(label: str = "my_nested_macro"):
    """Return a fresh `Macro` wrapping `nested_macro`."""
    return wfms.function2node(nested_macro, label)


def passthrough_node(label: str = "my_passthrough"):
    """Return a fresh `Macro` wrapping `passthrough`."""
    return wfms.function2node(passthrough, label)


def container_node(label: str = "container"):
    """Return a fresh `Macro` wrapping `container`."""
    return wfms.function2node(container, label)


def autoencoder_node(label: str = "autoencoder"):
    """Return a fresh `Macro` wrapping `autoencoder`."""
    return wfms.function2node(autoencoder, label)


def for_wf_node(label: str = "for_wf"):
    """Return a fresh `Macro` wrapping `for_wf`."""
    return wfms.function2node(for_wf, label)


def if_abs_node(label: str = "if_abs"):
    """Return a fresh `Macro` wrapping `if_abs`."""
    return wfms.function2node(if_abs, label)


def while_countdown_node(label: str = "while_countdown"):
    """Return a fresh `Macro` wrapping `while_countdown`."""
    return wfms.function2node(while_countdown, label)


def try_safe_divide_node(label: str = "try_safe_divide"):
    """Return a fresh `Macro` wrapping `try_safe_divide`."""
    return wfms.function2node(try_safe_divide, label)


# --------------------------------------------------------------------------- #
# Try-flow workflow                                                           #
# --------------------------------------------------------------------------- #


@fr.atomic
def divide(x, y):
    return x / y


@fr.workflow
def try_safe_divide(x, y):
    try:
        z = divide(x, y)
    except ZeroDivisionError:
        z = identity(x)
    return z


def try_recipe() -> frs.TryRecipe:
    """
    Programmatically-built `TryNode` matching `try_safe_divide`: divides `x` by `y`,
    falling back to `identity(x)` on `ZeroDivisionError`.
    """
    return frs.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=frs.LabeledRecipe(label="try_body", node=divide.flowrep_recipe),
        exception_cases=[
            frs.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=frs.LabeledRecipe(
                    label="except_body_0", node=identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            frs.TargetHandle(node="try_body", port="x"): frs.InputSource(port="x"),
            frs.TargetHandle(node="try_body", port="y"): frs.InputSource(port="y"),
            frs.TargetHandle(node="except_body_0", port="x"): frs.InputSource(port="x"),
        },
        prospective_output_edges={
            frs.OutputTarget(port="z"): [
                frs.SourceHandle(node="try_body", port="output_0"),
                frs.SourceHandle(node="except_body_0", port="x"),
            ],
        },
    )


def while_recipe() -> frs.WhileRecipe:
    """
    Programmatically-built `WhileNode` matching `while_countdown`.

    Decrements `n` while `is_positive(n)`. Mirrors `if_recipe()` so tests
    can construct a `While` directly without going through the parser.
    """
    body = frs.WorkflowRecipe(
        inputs=["n"],
        outputs=["n"],
        nodes={"decrement_0": decrement.flowrep_recipe},
        input_edges={
            frs.TargetHandle(node="decrement_0", port="x"): frs.InputSource(port="n"),
        },
        edges={},
        output_edges={
            frs.OutputTarget(port="n"): frs.SourceHandle(
                node="decrement_0", port="output_0"
            ),
        },
    )
    return frs.WhileRecipe(
        inputs=["n"],
        outputs=["n"],
        case=frs.ConditionalCase(
            condition=frs.LabeledRecipe(
                label="condition", node=is_positive.flowrep_recipe
            ),
            body=frs.LabeledRecipe(label="body", node=body),
        ),
        input_edges={
            frs.TargetHandle(node="condition", port="n"): frs.InputSource(port="n"),
            frs.TargetHandle(node="body", port="n"): frs.InputSource(port="n"),
        },
        output_edges={
            frs.OutputTarget(port="n"): frs.SourceHandle(node="body", port="n"),
        },
    )


# --------------------------------------------------------------------------- #
# Attribute-sugar collisions                                                  #
# --------------------------------------------------------------------------- #


def attr_sugar_recipe() -> frs.WorkflowRecipe:
    """
    Programmatic `WorkflowRecipe` whose node labels collide with graph
    attributes: `executor` and `nodes` shadow real attributes, `plain` does
    not. Every node wraps `multiply_with_defaults`, so no edges are needed.
    """
    return frs.WorkflowRecipe(
        inputs=[],
        outputs=[],
        nodes={
            "executor": multiply_with_defaults.flowrep_recipe,
            "nodes": multiply_with_defaults.flowrep_recipe,
            "plain": multiply_with_defaults.flowrep_recipe,
        },
        input_edges={},
        edges={},
        output_edges={},
    )


def attr_sugar_macro_node(label: str = "attr_sugar_macro"):
    """Return a fresh `Macro` whose node labels collide with graph attributes."""
    return wfms.Macro(label, attr_sugar_recipe())
