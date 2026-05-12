"""Shared test fixtures for ``_wfms`` unit tests.

These mirror the recipes in ``_minimal_integration_examples.py``. They are kept
in a single module so individual test files do not redefine — and possibly drift
from — the same flowrep-decorated functions. Note that flowrep parses the
*source* of a decorated function via ``inspect.getsource``, so these functions
must live in a real ``.py`` file (not in a ``python -c`` string or test scope).

Usage::

    from tests.unit._wfms import _fixtures
    n = _fixtures.atomic_add_node()
    run = n.run(x=1, y=2)
"""

from __future__ import annotations

import flowrep as fr

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


# --------------------------------------------------------------------------- #
# Macro recipes                                                               #
# --------------------------------------------------------------------------- #


@fr.workflow
def macro(x, y, z):
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

    Exercises the ``InputSource`` branch of :func:`dag.populate_outputs`.
    """
    s = add(x, y)
    return x, s


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
    """Return a fresh ``Atomic`` wrapping ``add``."""
    return wfms.node(add, label)


def atomic_sub_node(label: str = "sub"):
    """Return a fresh ``Atomic`` wrapping ``sub``."""
    return wfms.node(sub, label)


def macro_node(label: str = "my_macro"):
    """Return a fresh ``Macro`` wrapping ``macro``."""
    return wfms.node(macro, label)


def nested_macro_node(label: str = "my_nested_macro"):
    """Return a fresh ``Macro`` wrapping ``nested_macro``."""
    return wfms.node(nested_macro, label)


def passthrough_node(label: str = "my_passthrough"):
    """Return a fresh ``Macro`` wrapping ``passthrough``."""
    return wfms.node(passthrough, label)


def autoencoder_node(label: str = "autoencoder"):
    """Return a fresh ``Macro`` wrapping ``autoencoder``."""
    return wfms.node(autoencoder, label)


def for_wf_node(label: str = "for_wf"):
    """Return a fresh ``Macro`` wrapping ``for_wf``."""
    return wfms.node(for_wf, label)
