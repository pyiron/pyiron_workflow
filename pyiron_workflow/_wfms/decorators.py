from __future__ import annotations

import abc
import dataclasses
import functools
import inspect
import types
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

import flowrep as fr
from pyiron_snippets import versions

from pyiron_workflow._wfms import (
    atomic as atomic_mod,
)
from pyiron_workflow._wfms import dag, execution, validation

if TYPE_CHECKING:
    import rdflib


_DecoratedType = TypeVar("_DecoratedType", types.FunctionType, type)
_RecipeType = TypeVar("_RecipeType", fr.schemas.AtomicRecipe, fr.schemas.WorkflowRecipe)
_NodeType = TypeVar("_NodeType", atomic_mod.Atomic, dag.Macro)


class _PwfTools(Generic[_DecoratedType, _RecipeType, _NodeType], abc.ABC):
    assign_to: ClassVar[str]
    _node_type: type[_NodeType]

    def __init__(self, wrapped: _DecoratedType):
        self._disallow_locals(wrapped)
        self._decorated_object: _DecoratedType = wrapped

    @property
    @abc.abstractmethod
    def recipe(self) -> _RecipeType: ...

    @abc.abstractmethod
    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label: ...

    def node(
        self, label: fr.schemas.Label | None = None, /, *positional, **keyword
    ) -> _NodeType:
        return self._node_type(self._label(label), self.recipe, *positional, **keyword)

    def run(self, config: execution.RunConfig | None = None, **input_data):
        return self.node().run(config, **input_data)

    @staticmethod
    def _disallow_locals(func: types.FunctionType | type):
        if "<locals>" in func.__qualname__:
            raise ImportError(
                "To turn decorated functions into nodes, pyiron_workflow needs to be "
                "able to import the underlying decorated function; "
                f"but {func.__qualname__!r} contains '<locals>'."
            )


class _DecoratedFunction(
    _PwfTools[types.FunctionType, _RecipeType, _NodeType],
    Generic[_RecipeType, _NodeType],
    abc.ABC,
):
    assign_to: ClassVar[str] = "pwf"

    @property
    def recipe(self) -> _RecipeType:
        return self._decorated_object.flowrep_recipe  # type: ignore[attr-defined]

    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label:
        return self._decorated_object.__name__ if label is None else label


class DecoratedAtomic(_DecoratedFunction[fr.schemas.AtomicRecipe, atomic_mod.Atomic]):
    _node_type = atomic_mod.Atomic


class DecoratedMacro(_DecoratedFunction[fr.schemas.WorkflowRecipe, dag.Macro]):
    _node_type = dag.Macro

    def validate(
        self,
        do_types: bool = True,
        do_ontology: bool = True,
        extra_knowledge: rdflib.Graph | None = None,
    ) -> validation.CombinedValidationReport:
        return self.node().validate(
            do_types=do_types,
            do_ontology=do_ontology,
            extra_knowledge=extra_knowledge,
        )


_assigned = tuple(
    a
    for a in functools.WRAPPER_ASSIGNMENTS
    if a not in {"__name__", "__module__", "__qualname__"}
)  # Let the wrapping functions keep their identity


@functools.wraps(fr.tools.atomic, assigned=_assigned)
def atomic(*args, **kwargs):
    wrapped = fr.tools.atomic(*args, **kwargs)
    return _double_wrap_if_decorator_got_args(
        args[0], wrapped, DecoratedAtomic, "@atomic"
    )


atomic.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.schemas.Atomic` node instance or create a dynamic node  
instance and run it.

Base `flowrep` documentation:

""" + (fr.tools.atomic.__doc__ or "")


def _double_wrap_if_decorator_got_args(
    arg0: str | types.FunctionType,
    wrapped: types.FunctionType,
    tool: type[DecoratedAtomic] | type[DecoratedMacro],
    decorator_name: str,
):
    if isinstance(arg0, types.FunctionType):
        if not hasattr(wrapped, "flowrep_recipe"):  # pragma: no cover
            raise ValueError(
                f"The {decorator_name} decorator must be applied to a function "
                f"decorated with flowrep, as evidenced by the presence of a "
                f"`flowrep_recipe` attribute. This is an internal error and likely flags "
                f"a development bug. dir({wrapped!r}) = {dir(wrapped)}."
            )
        setattr(
            wrapped,
            tool.assign_to,
            tool(wrapped),
        )
        return wrapped
    elif isinstance(arg0, str):

        def wrapped_decorator(func):
            double_wrapped = wrapped(func)
            setattr(
                double_wrapped,
                tool.assign_to,
                tool(double_wrapped),
            )
            return double_wrapped

        return wrapped_decorator
    else:
        raise TypeError(
            f"{decorator_name} can only decorate functions, got {type(arg0).__name__}"
        )


@functools.wraps(fr.tools.workflow, assigned=_assigned)
def workflow(*args, **kwargs):
    wrapped = fr.tools.workflow(*args, **kwargs)
    return _double_wrap_if_decorator_got_args(
        args[0], wrapped, DecoratedMacro, "@workflow"
    )


workflow.__doc__ = """
A powered-up version of the `flowrep` decorator of the same name.

Additionally adds a `.pwf` attribute holding methods to instantiate the function as a 
`pyiron_workflow._wfms.api.schemas.Macro` node instance, validate the underlying graph, 
or create a dynamic node instance and run it.

Base `flowrep` documentation:

""" + (fr.tools.workflow.__doc__ or "")


class _DecoratedDataclass(
    _PwfTools[type, fr.schemas.AtomicRecipe, atomic_mod.Atomic], abc.ABC
):
    _root_function: ClassVar[staticmethod[..., Any]]
    _unpack_mode: ClassVar[fr.schemas.UnpackMode]

    _node_type = atomic_mod.Atomic

    def __init__(
        self,
        wrapped,
        version_scraping: versions.VersionScrapingMap | None = None,
        forbid_main: bool = False,
        forbid_locals: bool = False,
        forbid_lambda: bool = False,
        require_version: bool = False,
    ):
        super().__init__(wrapped)

        inputs, inputs_with_defaults, restricted_input_kinds, outputs, func_sig = (
            self._parse_dataclass(wrapped)
        )

        self._function = functools.partial(self._root_function, *self._partials())
        self._function.__name__ = wrapped.__name__  # type: ignore[attr-defined]
        self._function.__signature__ = func_sig  # type: ignore[attr-defined]
        self._function.__annotations__ = {  # type: ignore[attr-defined]
            name: p.annotation
            for name, p in func_sig.parameters.items()
            if p.annotation is not inspect.Parameter.empty
        }
        self._function.__annotations__["return"] = wrapped
        # Point __wrapped__ at the dataclass __init__ so that get_type_hints()
        # can resolve forward-reference annotations (e.g. dataclasses.InitVar[int])
        # using the dataclass module's globals rather than _root_function's globals.
        self._function.__wrapped__ = wrapped.__init__  # type: ignore[attr-defined]

        dc_version = versions.VersionInfo.of(
            wrapped,
            version_scraping=version_scraping,
            forbid_main=forbid_main,
            forbid_locals=forbid_locals,
            forbid_lambda=forbid_lambda,
            require_version=require_version,
        )
        self._recipe = fr.schemas.AtomicRecipe(
            inputs=inputs,
            outputs=outputs,
            reference=fr.schemas.PythonReference(
                info=versions.VersionInfo(
                    module=dc_version.module,
                    qualname=dc_version.qualname + f".{self.assign_to}._function",
                    version=dc_version.version,
                ),
                inputs_with_defaults=inputs_with_defaults,
                restricted_input_kinds=restricted_input_kinds,
            ),
            unpack_mode=self._unpack_mode,
        )

    @abc.abstractmethod
    def _parse_dataclass(self, cls) -> tuple[
        list[str],
        list[str],
        dict[str, fr.schemas.RestrictedParamKind],
        list[str],
        inspect.Signature,
    ]: ...

    @abc.abstractmethod
    def _partials(self) -> tuple: ...

    @property
    def recipe(self) -> fr.schemas.AtomicRecipe:
        return self._recipe


def _inputs_to_dataclass(cls, *args, **kwargs):
    return cls(*args, **kwargs)


class Inputs2Dataclass(_DecoratedDataclass):
    assign_to: ClassVar[str] = "pwf_inputs2dc"
    _root_function = staticmethod(_inputs_to_dataclass)
    _unpack_mode = fr.schemas.UnpackMode.NONE

    def _parse_dataclass(self, cls) -> tuple[
        list[str],
        list[str],
        dict[str, fr.schemas.RestrictedParamKind],
        list[str],
        inspect.Signature,
    ]:
        dc_sig = inspect.signature(cls)
        params = dc_sig.parameters

        inputs = list(params)
        inputs_with_defaults = [
            name
            for name, p in params.items()
            if p.default is not inspect.Parameter.empty
        ]
        restricted_input_kinds = {
            name: fr.schemas.RestrictedParamKind.KEYWORD_ONLY
            for name, p in params.items()
            if p.kind == inspect.Parameter.KEYWORD_ONLY
        }

        output_name = "dataclass"

        func_sig = dc_sig.replace(return_annotation=cls)

        return (
            inputs,
            inputs_with_defaults,
            restricted_input_kinds,
            [output_name],
            func_sig,
        )

    def _partials(self) -> tuple:
        return (self._decorated_object,)

    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label:
        return f"Inputs2{self._decorated_object.__name__}" if label is None else label


def _dataclass_to_outputs(dataclass):
    return dataclass


class Dataclass2Outputs(_DecoratedDataclass):
    assign_to: ClassVar[str] = "pwf_dc2outputs"
    _root_function = staticmethod(_dataclass_to_outputs)
    _unpack_mode = fr.schemas.UnpackMode.DATACLASS

    def _parse_dataclass(self, cls) -> tuple[
        list[str],
        list[str],
        dict[str, fr.schemas.RestrictedParamKind],
        list[str],
        inspect.Signature,
    ]:
        outputs = [f.name for f in dataclasses.fields(cls)]

        input_name = "dataclass"
        func_sig = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    input_name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=cls,
                )
            ],
            return_annotation=cls,
        )

        return (
            [input_name],
            [],
            {},
            outputs,
            func_sig,
        )

    def _partials(self) -> tuple:
        return ()

    def _label(self, label: fr.schemas.Label | None = None) -> fr.schemas.Label:
        return (
            f"Dataclass2{self._decorated_object.__name__}" if label is None else label
        )


_RESERVED_DATACLASS_PORT = "dataclass"


def _check_reserved_fields(dcls: type) -> None:
    for field in dataclasses.fields(dcls):
        if field.name == _RESERVED_DATACLASS_PORT:
            raise ValueError(
                f"@dataclass attaches node tools that reserve the port name "
                f"{_RESERVED_DATACLASS_PORT!r}, but dataclass "
                f"{dcls.__qualname__!r} declares a field named "
                f"{_RESERVED_DATACLASS_PORT!r}. Rename that field."
            )


@functools.wraps(dataclasses.dataclass, assigned=_assigned)
def dataclass(
    cls=None,
    /,
    version_scraping: versions.VersionScrapingMap | None = None,
    forbid_main: bool = False,
    forbid_locals: bool = False,
    require_version: bool = False,
    **dataclasses_dataclass_kwargs,
):
    def wrap(cls):
        dcls = dataclasses.dataclass(**dataclasses_dataclass_kwargs)(cls)
        _check_reserved_fields(dcls)
        setattr(
            dcls,
            Inputs2Dataclass.assign_to,
            Inputs2Dataclass(
                dcls,
                version_scraping=version_scraping,
                forbid_main=forbid_main,
                forbid_locals=forbid_locals,
                require_version=require_version,
            ),
        )
        setattr(
            dcls,
            Dataclass2Outputs.assign_to,
            Dataclass2Outputs(
                dcls,
                version_scraping=version_scraping,
                forbid_main=forbid_main,
                forbid_locals=forbid_locals,
                require_version=require_version,
            ),
        )
        return dcls

    # See if we're being called as @dataclass or @dataclass().
    if cls is None:
        # We're called with parens.
        return wrap

    # We're called as @dataclass without parens.
    return wrap(cls)


dataclass.__doc__ = """
A powered-up version of the standard `dataclasses` decorator of the same name.

Additionally adds a `.pwf_dc2outputs` and `.pwf_inputs2dc` attributes which host methods 
to instantiate atomic nodes converting between multiple graph IO and a single dataclass 
instance. The decorated dataclass is still usable as a regular dataclass, but don't 
use fields which conflict with these two new attribute names.

Base `dataclass` documentation:

""" + (dataclasses.dataclass.__doc__ or "")
