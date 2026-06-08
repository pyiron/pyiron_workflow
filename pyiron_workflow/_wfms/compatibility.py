from flowrep.api import schemas as frs
from flowrep.api import tools as frt
from pyiron_snippets import versions

from pyiron_workflow._wfms import decorators
from pyiron_workflow.nodes import multiple_distpatch


class SimpleFactory:
    """
    Wraps a :mod:`flowrep`-decorated function so that *calling* it produces a node
    instance, recapturing the legacy :mod:`pyiron_workflow` decorator philosophy.

    The decorated function is shadowed in its module namespace by this factory, so the
    real function is reachable only at ``<module>.<name>.decorated``. We rewrite both the
    recipe's :class:`~flowrep.api.schemas.PythonReference` *and* the function object's
    own ``__qualname__`` to point at that location, so the function remains pickleable
    by reference (e.g. for out-of-process execution).
    """

    def __init__(self, func):
        self.decorated = func
        original_ref = func.flowrep_recipe.reference
        new_qualname = original_ref.info.qualname + ".decorated"
        self.decorated.flowrep_recipe.reference = frs.PythonReference(
            info=versions.VersionInfo(
                module=original_ref.info.module,
                qualname=new_qualname,
                version=original_ref.info.version,
            ),
            inputs_with_defaults=original_ref.inputs_with_defaults,
            restricted_input_kinds=original_ref.restricted_input_kinds,
        )
        self.decorated.__qualname__ = new_qualname

    def __call__(self, *args, **kwargs):
        return self.decorated.pwf.node(self.decorated.__name__, *args, **kwargs)


@multiple_distpatch.dispatch_output_labels
def as_function_node(*output_labels, **kwargs):
    if not kwargs.get("forbid_locals", True):
        raise ValueError(
            "Nodes built from `<locals>` functions cannot be instantiated -- the "
            "underlying function is unimportable, so `generate_flowrep_live_node` "
            "would fail. We pin flowrep's `forbid_locals` on (clobbering any user "
            "value). You got clobbered."
        )
    kwargs["forbid_locals"] = True

    def decorator(func):
        decorated = frt.atomic(*output_labels, **kwargs)(func)
        decorated.pwf = decorators.AtomicTools(decorated)
        return SimpleFactory(decorated)

    return decorator


as_function_node.__doc__ = """
This is a compatibility tool for the legacy implementation of
:mod:`pyiron_workflow`. The signature matches the modern :mod:`flowrep` decorators
(which overlap with the legacy PWF decorator in taking output labels as variadic args,
and additionally accept the `flowrep` keyword arguments) but follows the legacy pattern
of converting the decorated function from a plain function to a node-creator.

I.e. the decorator returns a `Callable[..., Atomic]` node factory, such that the 
decorated function has its return modified to `Atomic`. This is here to maximize 
_syntactic_ compatibility with node definitions in legacy .py code -- the object you 
are actually going to get back is the new-style node.

Unlike the underlying `flowrep` decorator, this forces `forbid_locals=True`: a function
defined inside another function (`<locals>` in its qualname) cannot be re-imported, so
the resulting node could never be instantiated. Any `forbid_locals` value the caller
passes is ignored, and such functions raise a `ValueError` at decoration time.

`pyiron_workflow` decorator docstring:

""" + (decorators.atomic.__doc__ or "")
