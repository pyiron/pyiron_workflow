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

import dataclasses
from typing import Annotated

import flowrep as fr
import rdflib
import semantikon
from pyiron_snippets import versions

from pyiron_workflow._wfms import api as wfms

# --------------------------------------------------------------------------- #
# Plain functions                                                             #
# --------------------------------------------------------------------------- #


def plain_increment(x):
    return x + 1


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


@fr.atomic
def typed_int(x: int) -> int:
    return x + 0


@fr.atomic
def typed_float(x: float) -> float:
    return x + 0.0


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


@fr.workflow
def annotated_wf(
    w, x: int, y: int = 100, z: Annotated[float, ("units", "meters")] = 1.0
) -> tuple[int, Annotated[float, ("units", "centimeters")]]:
    m2cm = multiply_with_defaults(x=y, y=z)
    return x, m2cm


# --------------------------------------------------------------------------- #
# Autoencoder (round-trips TransformNto1 -> Transform1toN)                    #
# --------------------------------------------------------------------------- #

_COMPRESS = wfms.schemas.TransformNto1(3)
_EXPAND = wfms.schemas.Transform1toN(3)


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


def if_recipe() -> fr.schemas.IfRecipe:
    """
    Minimal `IfNode` whose condition/body both wrap `add`.

    Promoted here from `test_constructors.py` so the if-flow tests share a
    single source of truth for the canonical recipe shape.
    """
    add_recipe = add.flowrep_recipe
    return fr.schemas.IfRecipe(
        inputs=["x", "y"],
        outputs=["out"],
        cases=[
            fr.schemas.ConditionalCase(
                condition=fr.schemas.LabeledRecipe(label="cond", node=add_recipe),
                body=fr.schemas.LabeledRecipe(label="body", node=add_recipe),
            )
        ],
        input_edges={
            fr.schemas.TargetHandle(node="cond", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="cond", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="out"): [
                fr.schemas.SourceHandle(node="body", port="output_0")
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
    return wfms.tools.function2node(add, label)


def atomic_sub_node(label: str = "sub"):
    """Return a fresh `Atomic` wrapping `sub`."""
    return wfms.tools.function2node(sub, label)


def macro_node(label: str = "my_macro"):
    """Return a fresh `Macro` wrapping `macro`."""
    return wfms.tools.function2node(macro, label)


def annotated_macro_node(label: str = "my_annotated_macro"):
    """Return a fresh `Macro` wrapping `annotated_macro`."""
    return wfms.tools.function2node(annotated_macro, label)


def nested_macro_node(label: str = "my_nested_macro"):
    """Return a fresh `Macro` wrapping `nested_macro`."""
    return wfms.tools.function2node(nested_macro, label)


def passthrough_node(label: str = "my_passthrough"):
    """Return a fresh `Macro` wrapping `passthrough`."""
    return wfms.tools.function2node(passthrough, label)


def container_node(label: str = "container"):
    """Return a fresh `Macro` wrapping `container`."""
    return wfms.tools.function2node(container, label)


def autoencoder_node(label: str = "autoencoder"):
    """Return a fresh `Macro` wrapping `autoencoder`."""
    return wfms.tools.function2node(autoencoder, label)


def multiply_with_defaults_node(label: str = "multiply_with_defaults"):
    """Return a fresh `Atomic` wrapping `multiply_with_defaults`."""
    return wfms.tools.function2node(multiply_with_defaults, label)


def typed_int_node(label: str = "typed_int"):
    """Return a fresh `Atomic` wrapping `typed_int` (input/output hinted `int`)."""
    return wfms.tools.function2node(typed_int, label)


def typed_float_node(label: str = "typed_float"):
    """Return a fresh `Atomic` wrapping `typed_float` (input/output hinted `float`)."""
    return wfms.tools.function2node(typed_float, label)


# --------------------------------------------------------------------------- #
# Workflow builder                                                             #
# --------------------------------------------------------------------------- #

_MACRO_WF_EDGES = [
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="x"),
        fr.schemas.TargetHandle(node="add_0", port="x"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="y"),
        fr.schemas.TargetHandle(node="add_0", port="y"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="z"),
        fr.schemas.TargetHandle(node="sub_0", port="y"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.SourceHandle(node="add_0", port="output_0"),
        fr.schemas.TargetHandle(node="sub_0", port="x"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.SourceHandle(node="add_0", port="output_0"),
        fr.schemas.OutputTarget(port="a"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.SourceHandle(node="sub_0", port="output_0"),
        fr.schemas.OutputTarget(port="s"),
    ),
]


def build_workflow(inputs=(), outputs=(), node_specs=None, edges=(), label="wf"):
    """
    Build a Workflow for testing.

    node_specs: dict mapping node label -> factory function, e.g.
        {"add_0": atomic_add_node, "sub_0": atomic_sub_node}
    edges: iterable of EdgeTuple
    """
    if node_specs is None:
        node_specs = {}
    wf = wfms.Workflow(label)
    for name in inputs:
        wf.create_input(name)
    for name in outputs:
        wf.create_output(name)
    for node_label, factory in node_specs.items():
        wf.add_node(factory(node_label))
    for edge in edges:
        wf.add_edge(edge)
    return wf


def for_wf_node(label: str = "for_wf"):
    """Return a fresh `Macro` wrapping `for_wf`."""
    return wfms.tools.function2node(for_wf, label)


def foreach_node(label: str = "fe"):
    """Return a fresh `ForEach` flow-control node (a `NotParseable` to the type
    validator) wrapping `add(x, y)` with `x` nested and `y` broadcast."""
    body = fr.schemas.LabeledRecipe(label="body", node=add.flowrep_recipe)
    recipe = fr.schemas.ForEachRecipe(
        inputs=["xs", "y"],
        outputs=["sums"],
        body_node=body,
        input_edges={
            fr.schemas.TargetHandle(node="body", port="x"): fr.schemas.InputSource(
                port="xs"
            ),
            fr.schemas.TargetHandle(node="body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="sums"): fr.schemas.SourceHandle(
                node="body", port="output_0"
            ),
        },
        nested_ports=["x"],
        zipped_ports=[],
    )
    return wfms.schemas.ForEach(label, recipe)


def if_abs_node(label: str = "if_abs"):
    """Return a fresh `Macro` wrapping `if_abs`."""
    return wfms.tools.function2node(if_abs, label)


def while_countdown_node(label: str = "while_countdown"):
    """Return a fresh `Macro` wrapping `while_countdown`."""
    return wfms.tools.function2node(while_countdown, label)


def try_safe_divide_node(label: str = "try_safe_divide"):
    """Return a fresh `Macro` wrapping `try_safe_divide`."""
    return wfms.tools.function2node(try_safe_divide, label)


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


def try_recipe() -> fr.schemas.TryRecipe:
    """
    Programmatically-built `TryNode` matching `try_safe_divide`: divides `x` by `y`,
    falling back to `identity(x)` on `ZeroDivisionError`.
    """
    return fr.schemas.TryRecipe(
        inputs=["x", "y"],
        outputs=["z"],
        try_node=fr.schemas.LabeledRecipe(label="try_body", node=divide.flowrep_recipe),
        exception_cases=[
            fr.schemas.ExceptionCase(
                exceptions=[versions.VersionInfo.of(ZeroDivisionError)],
                body=fr.schemas.LabeledRecipe(
                    label="except_body_0", node=identity.flowrep_recipe
                ),
            ),
        ],
        input_edges={
            fr.schemas.TargetHandle(node="try_body", port="x"): fr.schemas.InputSource(
                port="x"
            ),
            fr.schemas.TargetHandle(node="try_body", port="y"): fr.schemas.InputSource(
                port="y"
            ),
            fr.schemas.TargetHandle(
                node="except_body_0", port="x"
            ): fr.schemas.InputSource(port="x"),
        },
        prospective_output_edges={
            fr.schemas.OutputTarget(port="z"): [
                fr.schemas.SourceHandle(node="try_body", port="output_0"),
                fr.schemas.SourceHandle(node="except_body_0", port="x"),
            ],
        },
    )


def while_recipe() -> fr.schemas.WhileRecipe:
    """
    Programmatically-built `WhileNode` matching `while_countdown`.

    Decrements `n` while `is_positive(n)`. Mirrors `if_recipe()` so tests
    can construct a `While` directly without going through the parser.
    """
    body = fr.schemas.WorkflowRecipe(
        inputs=["n"],
        outputs=["n"],
        nodes={"decrement_0": decrement.flowrep_recipe},
        input_edges={
            fr.schemas.TargetHandle(
                node="decrement_0", port="x"
            ): fr.schemas.InputSource(port="n"),
        },
        edges={},
        output_edges={
            fr.schemas.OutputTarget(port="n"): fr.schemas.SourceHandle(
                node="decrement_0", port="output_0"
            ),
        },
    )
    return fr.schemas.WhileRecipe(
        inputs=["n"],
        outputs=["n"],
        case=fr.schemas.ConditionalCase(
            condition=fr.schemas.LabeledRecipe(
                label="condition", node=is_positive.flowrep_recipe
            ),
            body=fr.schemas.LabeledRecipe(label="body", node=body),
        ),
        input_edges={
            fr.schemas.TargetHandle(node="condition", port="n"): fr.schemas.InputSource(
                port="n"
            ),
            fr.schemas.TargetHandle(node="body", port="n"): fr.schemas.InputSource(
                port="n"
            ),
        },
        output_edges={
            fr.schemas.OutputTarget(port="n"): fr.schemas.SourceHandle(
                node="body", port="n"
            ),
        },
    )


# --------------------------------------------------------------------------- #
# Attribute-sugar collisions                                                  #
# --------------------------------------------------------------------------- #


def attr_sugar_recipe() -> fr.schemas.WorkflowRecipe:
    """
    Programmatic `WorkflowRecipe` whose node labels collide with graph
    attributes: `executor` and `nodes` shadow real attributes, `plain` does
    not. Every node wraps `multiply_with_defaults`, so no edges are needed.
    """
    return fr.schemas.WorkflowRecipe(
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
    return wfms.schemas.Macro(label, attr_sugar_recipe())


# --------------------------------------------------------------------------- #
# Grouping fixtures                                                            #
# --------------------------------------------------------------------------- #


def grouping_wf_node_specs():
    """Three siblings whose connectivity exercises every cross-boundary case."""
    return {
        "add_0": atomic_add_node,
        "sub_0": atomic_sub_node,
        "mul_0": multiply_with_defaults_node,
    }


_GROUPING_WF_EDGES = [
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="x"),
        fr.schemas.TargetHandle(node="add_0", port="x"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="y"),
        fr.schemas.TargetHandle(node="add_0", port="y"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.SourceHandle(node="add_0", port="output_0"),
        fr.schemas.TargetHandle(node="sub_0", port="x"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.InputSource(port="z"),
        fr.schemas.TargetHandle(node="sub_0", port="y"),
    ),
    wfms.schemas.EdgeTuple(
        fr.schemas.SourceHandle(node="sub_0", port="output_0"),
        fr.schemas.OutputTarget(port="diff"),
    ),
]


def grouping_wf(label: str = "grouping_wf"):
    """Parent containing `add_0`, `sub_0`, `mul_0` plus the edge set above."""
    return build_workflow(
        inputs=["x", "y", "z"],
        outputs=["diff"],
        node_specs=grouping_wf_node_specs(),
        edges=_GROUPING_WF_EDGES,
        label=label,
    )


def passthrough_subgraph_wf(label: str = "passthrough_subgraph"):
    """Subgraph whose only inner edge is `InputSource('a') -> OutputTarget('b')`."""
    sub = wfms.Workflow(label)
    sub.create_input("a")
    sub.create_output("b")
    sub.add_edge(
        wfms.schemas.EdgeTuple(
            fr.schemas.InputSource(port="a"), fr.schemas.OutputTarget(port="b")
        )
    )
    return sub


# --------------------------------------------------------------------------- #
# Ontology fixtures (clothes domain)                                          #
#                                                                             #
# A minimal semantikon-annotated pipeline. `sell` requires the clothes to     #
# carry BOTH the `cleaned` and `color` properties (OWL restrictions).         #
# `my_correct_macro` dyes then washes before selling (valid); the            #
# `my_incorrect_macro` skips washing, so `cleaned` is missing (invalid).      #
# Must live at module scope: flowrep parses these from source.                #
# --------------------------------------------------------------------------- #

EX = rdflib.Namespace("http://www.example.org/")
uri_cleaned = semantikon.SemantikonURI(EX.cleaned)
uri_color = semantikon.SemantikonURI(EX.color)


class Clothes:
    pass


@wfms.atomic
def wash(clothes: semantikon.u(Clothes, uri=EX.Clothes)) -> semantikon.u(
    Clothes,
    uri=EX.Clothes,
    triples=(EX.hasProperty, uri_cleaned),
    derived_from="inputs.clothes",
):
    ...
    return clothes


@wfms.atomic
def dye(clothes: semantikon.u(Clothes, uri=EX.Clothes), color="blue") -> semantikon.u(
    Clothes,
    uri=EX.Clothes,
    triples=(EX.hasProperty, uri_color),
    derived_from="inputs.clothes",
):
    ...
    return clothes


@wfms.atomic
def sell(
    clothes: semantikon.u(
        Clothes,
        uri=EX.Clothes,
        restrictions=(
            (
                (rdflib.OWL.onProperty, EX.hasProperty),
                (rdflib.OWL.someValuesFrom, EX.cleaned),
            ),
            (
                (rdflib.OWL.onProperty, EX.hasProperty),
                (rdflib.OWL.someValuesFrom, EX.color),
            ),
        ),
    ),
) -> int:
    price = 10
    return price


@wfms.workflow
def my_correct_macro(clothes: Clothes):
    dyed_clothes = dye(clothes)
    washed_clothes = wash(dyed_clothes)
    money = sell(washed_clothes)
    return money


@wfms.workflow
def my_incorrect_macro(clothes: Clothes):
    dyed_clothes = dye(clothes)
    # Not washed! `cleaned` property is missing, so `sell` is unsatisfied.
    money = sell(dyed_clothes)
    return money


def clothes_correct_macro_node(label: str = "my_correct_macro"):
    """Return a fresh `Macro` for the valid clothes pipeline (dye -> wash -> sell)."""
    return my_correct_macro.pwf.node(label)


def clothes_incorrect_macro_node(label: str = "my_incorrect_macro"):
    """Return a fresh `Macro` for the invalid clothes pipeline (dye -> sell)."""
    return my_incorrect_macro.pwf.node(label)


# --------------------------------------------------------------------------- #
# Decorator-tool fixtures (wfms.atomic / wfms.workflow)                        #
# --------------------------------------------------------------------------- #


@wfms.atomic
def wfms_add(x, y):
    return x + y


@wfms.atomic("relabelled_sum")
def wfms_add_relabelled(x, y):
    return x + y


@wfms.workflow
def wfms_macro(x, y, z):
    a = add(x, y)
    s = sub(a, z)
    return a, s


# --------------------------------------------------------------------------- #
# Dataclass fixtures (wfms.dataclass)                                          #
# --------------------------------------------------------------------------- #


@wfms.dataclass
class PlainPoint:
    x: float
    y: float


@wfms.dataclass
class WithDefaults:
    a: int
    b: int = 5
    c: list = dataclasses.field(default_factory=list)


@wfms.dataclass(frozen=True, kw_only=True)
class FrozenKw:
    nova: float
    foo: int = 42
    not_a_field = 13  # unannotated -> NOT a dataclass field


@wfms.dataclass
class WithInitFalse:
    a: int
    c: int = dataclasses.field(init=False, default=7)


@wfms.dataclass
class WithInitVar:
    a: "int"  # noqa: UP037
    d: dataclasses.InitVar[int] = 3
    b: int = 5
