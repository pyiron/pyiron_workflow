import abc
import inspect
import itertools
import re
import types

import flowrep as fr
import semantikon
from pyiron_snippets import versions

from pyiron_workflow import output_parser
from pyiron_workflow._wfms import datatypes, decorators, workflow
from pyiron_workflow.nodes import multiple_distpatch


class _CombatibilityFactory(abc.ABC):
    """
    Wraps a :mod:`flowrep`-decorated function so that *calling* it produces a node
    instance, recapturing the legacy :mod:`pyiron_workflow` decorator philosophy.

    The decorated function is shadowed in its module namespace by this factory, so the
    real function is reachable only at ``<module>.<name>.decorated``. We rewrite both the
    recipe's :class:`~flowrep.api.schemas.PythonReference` *and* the function object's
    own ``__qualname__`` to point at that location, so the function remains pickleable
    by reference (e.g. for out-of-process execution).
    """

    def __init__(self, func, *output_labels: str):
        self._received_function = func
        self._output_labels = output_labels
        self._decorated = None

    @staticmethod
    @abc.abstractmethod
    def _to_flowrep(
        func: types.FunctionType,
        *output_labels: str,
    ) -> types.FunctionType:
        """
        Macro nodes compile new python functions, so to avoid circular import errors
        from partially-initialized modules, we must delay this compilation and _not_
        do it at decoration time, but only later when the factory is actually used.

        This is managed by making the ``decorated`` function a property, and here we
        give an abstract interface for macros and function nodes to cast themselves
        differently.
        """

    def __call__(self, *args, **kwargs):
        return self.decorated.pwf.node(self.decorated.__name__, *args, **kwargs)

    @property
    def decorated(self):
        if self._decorated is None:
            flowrep_func = self._to_flowrep(
                self._received_function,
                *self._output_labels,
            )
            self._decorated = self._override_metadata(flowrep_func)
        return self._decorated

    @staticmethod
    def _override_metadata(func):
        original_ref = func.flowrep_recipe.reference
        new_qualname = func.__qualname__ + ".decorated"

        func_info = versions.VersionInfo.of(func)
        replacement_info = versions.VersionInfo(
            module=func_info.module,
            qualname=new_qualname,
            version=func_info.version,
        )

        func.flowrep_recipe.reference = fr.schemas.PythonReference(
            info=replacement_info,
            inputs_with_defaults=original_ref.inputs_with_defaults,
            restricted_input_kinds=original_ref.restricted_input_kinds,
        )
        func.__qualname__ = new_qualname
        return func


class AtomicFactory(_CombatibilityFactory):
    @staticmethod
    def _to_flowrep(
        func: types.FunctionType,
        *output_labels: str,
    ) -> types.FunctionType:
        decorated = fr.tools.atomic(func, *output_labels)
        decorated.pwf = decorators.AtomicTools(decorated)
        return decorated


class MacroFactory(_CombatibilityFactory):
    @staticmethod
    def _to_flowrep(
        func: types.FunctionType, *output_labels: str
    ) -> types.FunctionType:
        wf = _legacy_as_macro_node2workflow(func, *output_labels)
        full_signature = inspect.signature(func)
        selfless_signature = full_signature.replace(
            parameters=list(full_signature.parameters.values())[1:]
        )
        rendered = fr.tools.flowrep2python(
            wf.recipe,
            signature=selfless_signature,
            function_name=func.__name__,
            _workflow_decorator=(
                "pyiron_workflow._wfms.api",  # Recovering proximate modules is tough,
                # so just hard-code the proximate API reference
                "workflow",
            ),
        )
        new_form = rendered.build()
        new_form.__module__ = func.__module__
        new_form.__qualname__ = func.__qualname__
        return new_form


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
        return AtomicFactory(func, *output_labels)

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


@multiple_distpatch.dispatch_output_labels
def as_macro_node(*output_labels, **kwargs):
    def decorator(func):
        return MacroFactory(func, *output_labels, **kwargs)

    return decorator


def _legacy_as_macro_node2workflow(func, *output_labels) -> workflow.Workflow:
    wf = workflow.Workflow(func.__name__)
    sig = inspect.signature(func)
    ports_to_pass = _build_inputs_and_collect_input_ports(wf, sig)
    returns: (
        datatypes.Node | datatypes.Port | tuple[datatypes.Node | datatypes.Port]
    ) = func(wf, **ports_to_pass)

    # Ensure tuple-structure from single-return functions
    returned_ports = (returns,) if not isinstance(returns, tuple) else returns
    n_outputs = len(returned_ports)

    default_labels = _parse_legacy_output_labels(func, n_outputs)
    output_port_labels = _get_output_labels(
        n_outputs,
        output_labels,
        default_labels,
    )
    _convert_returns_to_outputs_and_edges(wf, returned_ports, output_port_labels)
    return wf


def _build_inputs_and_collect_input_ports(
    wf: workflow.Workflow, sig: inspect.Signature
) -> dict[str, datatypes.Port]:
    kwargs: dict[str, datatypes.Port] = {}
    for port_name, param in itertools.islice(
        sig.parameters.items(), 1, None
    ):  # skip self
        if param.annotation is not inspect.Signature.empty:
            type_hint = semantikon.annotation_to_type_hint(param.annotation)
            type_metadata = semantikon.annotation_to_type_metadata(param.annotation)
        else:
            type_hint, type_metadata = None, None
        wf.create_input(port_name, type_hint=type_hint, type_metadata=type_metadata)
        kwargs[port_name] = wf.inputs[port_name]
    return kwargs


def _parse_legacy_output_labels(func, n_outputs: int) -> tuple[str, ...]:
    scraped_labels = output_parser.ParseOutput(func).output
    sig = inspect.signature(func)
    self_argument = next(iter(sig.parameters))
    if scraped_labels is not None:
        # Strip off the first argument, e.g. self.foo just becomes foo
        stripped_labels = tuple(
            re.sub(r"^" + re.escape(f"{self_argument}."), "", label)
            for label in scraped_labels
        )
        cleaned_labels = tuple(
            f"output_{i}" if "." in label else label
            for i, label in enumerate(stripped_labels)
        )
        return cleaned_labels
    else:
        return tuple(f"output_{i}" for i in range(n_outputs))


def _get_output_labels(
    n_expected, output_labels: tuple[str, ...], default_labels: tuple[str, ...]
):
    if len(output_labels) == 0:
        return default_labels
    elif len(output_labels) == n_expected:
        return output_labels
    else:
        raise ValueError(
            f"Found {n_expected} return values, but got an incommensurate number of "
            f"labels: {output_labels!r}."
        )


def _convert_returns_to_outputs_and_edges(
    wf: workflow.Workflow,
    returned_ports: tuple[datatypes.Node | datatypes.Port],
    output_port_labels: tuple[str, ...],
) -> None:
    for label, obj in zip(output_port_labels, returned_ports, strict=True):
        if isinstance(obj, datatypes.Port):
            if obj.owner is wf:
                source = fr.schemas.InputSource(port=obj.label)
            else:
                source = fr.schemas.SourceHandle(node=obj.owner.label, port=obj.label)
        elif isinstance(obj, datatypes.Node) and len(obj.outputs) == 1:
            port: datatypes.Port = next(iter(obj.outputs.values()))
            source = fr.schemas.SourceHandle(node=port.owner.label, port=port.label)
        else:
            raise NotImplementedError()

        wf.create_output(label)
        target = fr.schemas.OutputTarget(port=label)
        wf.add_edge(datatypes.EdgeTuple(source, target))
