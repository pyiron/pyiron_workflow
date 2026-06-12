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


class _CompatibilityFactory(abc.ABC):
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


class AtomicFactory(_CompatibilityFactory):
    @staticmethod
    def _to_flowrep(
        func: types.FunctionType,
        *output_labels: str,
    ) -> types.FunctionType:
        decorated = fr.tools.atomic(func, *output_labels)
        decorated.pwf = decorators.AtomicTools(decorated)
        return decorated


class MacroFactory(_CompatibilityFactory):
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


def _kwargs_error(func, **kwargs) -> str:
    return (
        "Compatibility decorators take legacy-decorated functions, and turn them into "
        "factories for new node classes. In this context, arguments (other than output "
        "labels) to the decorator are not meaningful. Can't parse "
        f"{func.__qualname__!r} as a compatiblity factory because it received kwargs "
        f"{kwargs!r}"
    )


@multiple_distpatch.dispatch_output_labels
def as_function_node(*output_labels, **kwargs):
    """
    This is a compatibility decorator so that legacy ``.py`` files with decorated
    functions continue to work, but return new-style nodes. I.e. this object will
    return a ``flowrep``-based node.

    In the case of decorated atomic nodes, no changes need to be made to the function
    definition to move from `@as_function_node` to `@atomic` -- just update the
    decorator, and note that modern decorated function _stay functions_ and do not
    become node factories.
    """
    if kwargs:
        raise ValueError(_kwargs_error(as_function_node, **kwargs))

    def decorator(func):
        return AtomicFactory(func, *output_labels)

    return decorator


@multiple_distpatch.dispatch_output_labels
def as_macro_node(*output_labels, **kwargs):
    if kwargs:
        raise ValueError(_kwargs_error(as_function_node, **kwargs))

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
