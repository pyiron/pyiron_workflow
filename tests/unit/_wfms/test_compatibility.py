from __future__ import annotations

import pickle
import unittest

from pyiron_workflow._wfms import compatibility, dag
from pyiron_workflow.nodes.multiple_distpatch import MultipleDispatchError

# --------------------------------------------------------------------------- #
# Compatibility fixtures                                                      #
# --------------------------------------------------------------------------- #


@compatibility.as_function_node("z")
def add(x, y):
    z = x + y
    return z


class NestedScope:
    @staticmethod
    @compatibility.as_function_node
    def add(x, y):
        z = x + y
        return z


@compatibility.as_function_node("renamed")
def add_renamed(x, y):
    z = x + y
    return z


def _plain(x):
    """An undecorated function, used to provoke the multiple-dispatch error."""
    return x


@compatibility.as_function_node("d")
def double(a):
    d = a * 2
    return d


@compatibility.as_function_node("z")
def add_with_default(x, y=10):
    z = x + y
    return z


@compatibility.as_function_node("p", "q")
def two_outputs(a, b):
    return a, b


@compatibility.as_macro_node("summed")
def macro_args(self, x, y):
    """Child built with positional args; returns a single-output node."""
    self.s = add(x, y)
    return self.s


@compatibility.as_macro_node("summed")
def macro_kwargs(self, x, y):
    """Child built with keyword args."""
    self.s = add(x=x, y=y)
    return self.s


@compatibility.as_macro_node("doubled_sum")
def macro_node_input(self, x, y):
    """A single-output child *node* is passed as input to the next child."""
    self.s = add(x, y)
    self.d = double(self.s)
    return self.d


@compatibility.as_macro_node("doubled_sum")
def macro_port_input(self, x, y):
    """A specific child output *port* is passed as input to the next child."""
    self.s = add(x, y)
    self.d = double(self.s.outputs["z"])
    return self.d


@compatibility.as_macro_node("summed")
def macro_port_return(self, x, y):
    """A specific child port, bound to a local, is returned."""
    self.s = add(x, y)
    summed = self.s.outputs["z"]
    return summed


@compatibility.as_macro_node("summed", "doubled")
def macro_multi_return(self, x, y):
    """Mixed multi-return: a specific child port and a single-output node."""
    self.s = add(x, y)
    self.d = double(self.s)
    summed = self.s.outputs["z"]
    return summed, self.d


@compatibility.as_macro_node
def macro_scraped(self, x, y):
    """No explicit labels: the output label is scraped from the return name."""
    self.s = add(x, y)
    return self.s


@compatibility.as_macro_node("result")
def macro_child_default(self, x):
    """The child's own default (``y=10``) must survive the round trip."""
    self.a = add_with_default(x)
    return self.a


@compatibility.as_macro_node("echoed", "summed")
def macro_passthrough(self, x, y):
    """A parent input wired straight to a parent output."""
    self.s = add(x, y)
    return x, self.s


@compatibility.as_macro_node("total")
def macro_outer_args(self, x, y):
    """Macro child built with positional args; its ports feed a function node."""
    self.inner = macro_multi_return(x, y)
    self.combined = add(self.inner.outputs["summed"], self.inner.outputs["doubled"])
    return self.combined


@compatibility.as_macro_node("total")
def macro_outer_kwargs(self, x, y):
    """Macro child built with kwargs; the single-output macro is passed as input."""
    self.inner = macro_args(x=x, y=y)
    self.combined = double(self.inner)
    return self.combined


@compatibility.as_macro_node
def dotted_return(self, x, y):
    self.s = add(x, y)
    return self.s.outputs["z"]


def too_few_labels(self, x, y):
    self.s = add(x, y)
    self.t = add(y, x)
    return self.s, self.t


@compatibility.as_macro_node("both")
def returns_multi_output_node(self, a, b):
    self.t = two_outputs(a, b)
    return self.t


class TestAsFunctionNodeBehaviour(unittest.TestCase):
    """The decorated function should behave as a legacy-style node factory."""

    def test_naked_decorator_builds_a_factory(self) -> None:
        self.assertIsInstance(add, compatibility._AtomicFactory)

    def test_calling_factory_builds_a_runnable_node(self) -> None:
        node = add()
        self.assertEqual(node.label, add.decorated.__name__)
        run = node.run(x=-1, y=43)
        self.assertEqual(run.outputs["z"].value, 42)

    def test_nested_scope_works(self) -> None:
        run = NestedScope.add().run(x=2, y=3)
        self.assertEqual(run.outputs["z"].value, 5)

    def test_locals_fail_at_decoration(self) -> None:
        # A `<locals>` function can never be instantiated (it is unimportable), so we
        # force the failure to the decoration call rather than the later `node(...)`.
        with self.assertRaisesRegex(ImportError, "contains '<locals>'"):

            @compatibility.as_function_node
            def local_add(x, y):
                z = x + y
                return z

    def test_explicit_forbid_locals_false_is_overridden(self) -> None:
        # We raise on any kwargs, because we can't support old kwargs for new objects
        with self.assertRaisesRegex(ValueError, "are not meaningful."):

            @compatibility.as_function_node(forbid_locals=False)
            def local_add(x, y):
                z = x + y
                return z

    def test_output_label_argument_renames_the_output(self) -> None:
        run = add_renamed().run(x=2, y=3)
        self.assertEqual(
            list(run.outputs.keys()),
            ["renamed"],
            msg="A positional string argument should override the scraped label.",
        )
        self.assertEqual(run.outputs["renamed"].value, 5)


class TestMultipleDispatch(unittest.TestCase):
    def test_callable_with_extra_arguments_is_an_error(self) -> None:
        with self.assertRaises(MultipleDispatchError):
            compatibility.as_function_node(_plain, "extra")


class TestQualnameMangling(unittest.TestCase):
    """
    The shadowing of the module name breaks pickle-by-reference; we repair it by
    redirecting both the function object and its recipe to `<name>.decorated`.
    """

    def test_function_qualname_points_at_decorated_attribute(self) -> None:
        self.assertEqual(add.decorated.__qualname__, "add.decorated")

    def test_module_is_left_untouched(self) -> None:
        self.assertEqual(add.decorated.__module__, __name__)

    def test_recipe_reference_matches_the_mangled_qualname(self) -> None:
        info = add.decorated.flowrep_recipe.reference.info
        self.assertEqual(info.qualname, "add.decorated")
        self.assertEqual(info.module, __name__)

    def test_function_pickles_by_reference(self) -> None:
        # The fast stand-in for out-of-process execution: pickling the function
        # resolves `<module>.add.decorated` and round-trips to the *same* object.
        restored = pickle.loads(pickle.dumps(add.decorated))
        self.assertIs(restored, add.decorated)


class TestAsMacroNodeFactory(unittest.TestCase):
    def test_factory_is_a_simple_factory(self) -> None:
        self.assertIsInstance(macro_args, compatibility._MacroFactory)

    def test_calling_factory_builds_a_macro(self) -> None:
        self.assertIsInstance(macro_args(), dag.Macro)

    def test_macro_label_is_the_function_name(self) -> None:
        self.assertEqual(macro_args().label, "macro_args")


class TestFlatMacroConstruction(unittest.TestCase):
    """Children built only from `as_function_node`-decorated calls."""

    def test_positional_child_construction(self) -> None:
        run = macro_args().run(x=1, y=2)
        self.assertEqual(run.outputs["summed"].value, 3)

    def test_keyword_child_construction(self) -> None:
        run = macro_kwargs().run(x=1, y=2)
        self.assertEqual(run.outputs["summed"].value, 3)

    def test_single_output_node_as_input(self) -> None:
        # double(self.s) -- the whole single-output node feeds the next child.
        run = macro_node_input().run(x=1, y=2)
        self.assertEqual(run.outputs["doubled_sum"].value, 6)

    def test_specific_port_as_input(self) -> None:
        # double(self.s.outputs["z"]) -- a named child port feeds the next child.
        run = macro_port_input().run(x=1, y=2)
        self.assertEqual(run.outputs["doubled_sum"].value, 6)


class TestMacroReturns(unittest.TestCase):
    def test_single_output_node_return(self) -> None:
        run = macro_args().run(x=1, y=2)
        self.assertEqual(list(run.outputs.keys()), ["summed"])
        self.assertEqual(run.outputs["summed"].value, 3)

    def test_specific_port_return(self) -> None:
        run = macro_port_return().run(x=1, y=2)
        self.assertEqual(run.outputs["summed"].value, 3)

    def test_mixed_multi_return(self) -> None:
        run = macro_multi_return().run(x=1, y=2)
        self.assertEqual(run.outputs["summed"].value, 3)
        self.assertEqual(run.outputs["doubled"].value, 6)

    def test_parent_input_passthrough(self) -> None:
        # `return x, self.s` wires a parent input straight to a parent output.
        run = macro_passthrough().run(x=4, y=5)
        self.assertEqual(run.outputs["echoed"].value, 4)
        self.assertEqual(run.outputs["summed"].value, 9)


class TestMacroOutputLabels(unittest.TestCase):
    def test_explicit_label_names_the_output(self) -> None:
        self.assertEqual(list(macro_args().run(x=1, y=2).outputs.keys()), ["summed"])

    def test_scraped_label_from_return_name(self) -> None:
        # No decorator argument: the label is scraped from `return self.s`.
        run = macro_scraped().run(x=1, y=2)
        self.assertEqual(list(run.outputs.keys()), ["s"])
        self.assertEqual(run.outputs["s"].value, 3)

    def test_incommensurate_explicit_labels_raise(self) -> None:
        factory = compatibility.as_macro_node("only_one")(too_few_labels)
        # Can't declare this locally because it hits the "no locals" error first,
        # so instead in-line the decorator function

        with self.assertRaisesRegex(
            ValueError,
            "Found 2 return values, but got an incommensurate number of labels",
        ):
            # To fail early, we'd need to explicitly parse the number of return values
            # from the function. It's just not worth re-running that infrastructure
            # just to fail earlier, so live with failing only at node-usage time.
            factory()

    def test_dotted_inline_return_without_local_binding_gives_default(self) -> None:
        n = dotted_return()
        self.assertIn("output_0", n.outputs)


class TestMacroDefaultCapture(unittest.TestCase):
    """The `flowrep2python` round trip must preserve child default values."""

    def test_child_default_survives_in_the_recipe(self) -> None:
        node = macro_child_default()
        (child,) = node.recipe.nodes.values()
        self.assertEqual(child.reference.inputs_with_defaults, ["y"])

    def test_child_default_is_used_at_run_time(self) -> None:
        # Only `x` is supplied; the child's `y=10` default is applied: 5 + 10.
        run = macro_child_default().run(x=5)
        self.assertEqual(run.outputs["result"].value, 15)


class TestNestedMacroConstruction(unittest.TestCase):
    """Children are themselves `as_macro_node`-decorated calls."""

    def test_nested_macro_with_positional_child(self) -> None:
        # Sum of the inner macro's two outputs: three plus six.
        run = macro_outer_args().run(x=1, y=2)
        self.assertEqual(run.outputs["total"].value, 9)

    def test_nested_macro_with_keyword_child(self) -> None:
        # The inner macro's single output (three), doubled.
        run = macro_outer_kwargs().run(x=1, y=2)
        self.assertEqual(run.outputs["total"].value, 6)


class TestUnsupportedReturns(unittest.TestCase):
    def test_multi_output_node_returned_directly_is_unsupported(self) -> None:
        # A node with more than one output port cannot be collapsed to a single
        # returned value -- the conversion raises rather than guessing.
        with self.assertRaisesRegex(ValueError, "please choose individual ports."):
            returns_multi_output_node()
            # Again, it would be nice to fail earlier, but it's just not worth the
            # extra work


if __name__ == "__main__":
    unittest.main()
