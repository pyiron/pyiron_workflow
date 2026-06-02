from __future__ import annotations

import pickle
import unittest

from pyiron_workflow._wfms import compatibility
from pyiron_workflow.nodes.multiple_distpatch import MultipleDispatchError

# --------------------------------------------------------------------------- #
# Module-level decorated functions.                                           #
#                                                                             #
# These MUST live at module scope so the shadowed real function is reachable  #
# (and pickleable) at `<module>.<name>.decorated`.                            #
# --------------------------------------------------------------------------- #


@compatibility.as_function_node
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


@compatibility.as_function_node(unpack_mode="none")
def two_returns(a, b):
    return a, b


def _plain(x):
    """An undecorated function, used to provoke the multiple-dispatch error."""
    return x


class TestAsFunctionNodeBehaviour(unittest.TestCase):
    """The decorated function should behave as a legacy-style node factory."""

    def test_naked_decorator_builds_a_factory(self) -> None:
        self.assertIsInstance(add, compatibility.SimpleFactory)

    def test_calling_factory_builds_a_runnable_node(self) -> None:
        node = add("my_label")
        self.assertEqual(node.label, "my_label")
        run = node.run(x=-1, y=43)
        self.assertEqual(run.outputs["z"].value, 42)

    def test_nested_scope_works(self) -> None:
        run = NestedScope.add("lbl").run(x=2, y=3)
        self.assertEqual(run.outputs["z"].value, 5)

    def test_locals_fail_at_decoration(self) -> None:
        # A `<locals>` function can never be instantiated (it is unimportable), so we
        # force the failure to the decoration call rather than the later `node(...)`.
        with self.assertRaises(ValueError):

            @compatibility.as_function_node
            def local_add(x, y):
                z = x + y
                return z

    def test_explicit_forbid_locals_false_is_overridden(self) -> None:
        # We pin `forbid_locals=True`, so even an explicit opt-out still fails fast.
        with self.assertRaisesRegex(ValueError, "You got clobbered."):

            @compatibility.as_function_node(forbid_locals=False)
            def local_add(x, y):
                z = x + y
                return z

    def test_output_label_argument_renames_the_output(self) -> None:
        run = add_renamed("lbl").run(x=2, y=3)
        self.assertEqual(
            list(run.outputs.keys()),
            ["renamed"],
            msg="A positional string argument should override the scraped label.",
        )
        self.assertEqual(run.outputs["renamed"].value, 5)

    def test_flowrep_kwargs_flow_through_to_the_recipe(self) -> None:
        # `unpack_mode="none"` should keep the tuple return as a *single* output port
        # rather than unpacking it into two -- proving the kwarg reached flowrep.
        run = two_returns("lbl").run(a=1, b=2)
        self.assertEqual(list(run.outputs.keys()), ["output_0"])
        self.assertEqual(run.outputs["output_0"].value, (1, 2))


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


if __name__ == "__main__":
    unittest.main()
