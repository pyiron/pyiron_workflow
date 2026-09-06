"""
Stubs to raise useful messages for users trying to import pre-flowrep (<=0.17.0) tools.
"""

from typing import NoReturn

from . import messages

_REPO_URL = "https://github.com/pyiron/flowrep"
_USER_GUIDE = (
    "https://flowrep.readthedocs.io/en/latest/source/notebooks/user-guide.html"
)


class RemovedFeatureError(ImportError):
    """A pyiron_workflow object from <=0.17.0 is no longer available here."""


class _TopLevel:
    """
    From the 0.17.0 top-level __init__.py
    """

    RELOCATED: dict[str, str] = {
        "NodeSlurmExecutor": "Use `pyiron_workflow.tools.NodeSlurmExecutor` instead.",
    }

    NOT_AVAILABLE: dict[str, str] = {
        "as_dataclass_node": (
            "Decorate your class with `@flowrep.dataclass`, then convert it to a node "
            "with `pyiron_workflow.node`; e.g. "
            "`wf.dc_node = pyiron_workflow.node(MyDC, x=1, y=2)`. `@flowrep.dataclass` "
            "also makes the inverse operation (a node taking a dataclass and returning "
            "one output port per dataclass field) available under "
            "`.flowrep_recipe_unpacking`."
        ),
        # "Workflow": new functionality exists under the same name
        # "as_function_node": available via compatibility wrapper
        # "as_macro_node": available via compatibility wrapper
        "dataclass_node": (
            "Use the `@flowrep.dataclass` to wrap your class into a dataclass, and "
            "convert it to a node with `pyiron_workflow.node`; e.g. "
            "`wf.dc_node = pyiron_workflow.node(flowrep.dataclass(MyDC), x=1, y=2)`."
        ),
        "for_node": (
            "In the context of parsing a decorated context, you can write for-loops "
            "with constrained but native python code -- see the flowrep user guide. "
            "Further, it is now always allowed to directly write a flowrep for-loop "
            "recipe with `flowrep.schemas.ForEachRecipe` and turn that into a "
            "pyiron_workflow node, e.g. "
            "`wf.for_node = pyiron_workflow.node(my_foreach_recipe)`."
        ),
        "function_node": (
            "Simply cast the function to a node before adding it to a workflow, e.g. "
            "`wf.function_node = pyiron_workflow.node(some_function)`. "
            "For tricky/unparseable functions, you have the power to define how the "
            "function will be executed and how its IO will be exposed by writing a "
            "flowrep atomic recipe directly using `flowrep.schemas.AtomicRecipe`."
        ),
        "macro_node": (
            "For functions written to be parsed by legacy pyiron_workflow, use the "
            "`@pyiron_workflow.as_macro_node` decorator on the function definition, or "
            "rewrite the function to be compliant with `flowrep.workflow` parsing and "
            "simply use "
            "`wf.macro_node = pyiron_workflow.node(flowrep.workflow(some_flowrep_macro_definition))`."
        ),
        "std": (
            "Use `flowrep.std` instead, and convert standard recipes to nodes when "
            "adding them to a workflow, e.g. "
            "`wf.add = pyiron_workflow.node(flowrep.std.add)`."
        ),
        "to_function_node": (
            "Flowrep-based nodes don't have subclasses for each individual node; the "
            "closest analogy is to look at the recipe of a parsed function, e.g. "
            "`my_recipe = flowrep.atomic(some_function).flowrep_recipe`. Note that "
            "these recipes are callable (e.g., `my_recipe(1, 2)`) and can be used as "
            "nodes in a pyiron_workflow workflow (e.g., "
            "`wf.my_node = pwf.node(my_recipe, y=2)`)."
        ),
        "while_node": (
            "In the context of parsing a decorated context, you can write while-loops "
            "with constrained but native python code -- see the flowrep user guide. "
            "Further, it is always allowed to directly write a flowrep while-loop "
            "recipe with `flowrep.schemas.WhileRecipe` and turn that into a "
            "pyiron_workflow node, e.g. "
            "`wf.while_node = pyiron_workflow.node(my_while_recipe)`."
        ),
    }


_TRANSFORMER_MESSAGE = (
    "There is no special pyiron_workflow or flowrep tool for this, but the flowrep "
    "user guide outlines how power-users can create recipes for data transformation "
    "that reference very flexible functions, including those which accept variadic "
    "input."
)
_STORAGE_MESSAGE = (
    "Flowrep-based pyiron_workflow does not enforce a particular storage paradigm; "
    "recipes are save-able as plain-text JSON by virtue of all flowrep recipes being "
    "pydantic models -- just write the `some_recipe.model_dump_json(indent=2)` string "
    "to file however you please. For storing completed `Run` output or "
    "`flowrep.schemas.NodeData` output, we recommend using bagofholding; "
    "`flowrep.tools.LexicalBagBrowser` provides a convenient widget for browsing and "
    "reloading `NodeData` stored in H5 bags."
)


class _ApiSubmodule:
    """
    From the 0.17.0 api.py submodule
    """

    RELOCATED: dict[str, str] = {
        "NOT_DATA": "Use `flowrep.schemas.NOT_DATA` instead.",
        "NodeSlurmExecutor": _TopLevel.RELOCATED["NodeSlurmExecutor"],
    }

    NOT_AVAILABLE: dict[str, str] = {
        "CloudpickleProcessPoolExecutor": (
            "The pyiron_workflow infrastructure itself works fine with a regular "
            "`concurrent.futures.ProcessPoolExecutor`; if you have _data_ moving "
            "through your graph that will not pickle, finding an executor to work with "
            "it is outside the scope of pyiron_workflow. Cf. also "
            "`pyiron_workflow.tools.NodeSingleExecutor` and `.NodeSlurmExecutor` for "
            "convenience wrappers to Executorlib executors."
        ),
        "logger": (
            "There is no custom logger; use the `pyiron_workflow.RunConfig`'s "
            "`progress_hook` and `exception_hook` fields to configure graph-based "
            "activity reporting on a per-run basis."
        ),
        "std": _TopLevel.NOT_AVAILABLE["std"],
        "FailedChildError": (
            "Child errors are raised as-is, or grouped in an exception group and "
            "raised together."
        ),
        "For": _TopLevel.NOT_AVAILABLE["for_node"],
        "for_node": _TopLevel.NOT_AVAILABLE["for_node"],
        "for_node_factory": _TopLevel.NOT_AVAILABLE["for_node"],
        "Function": _TopLevel.NOT_AVAILABLE["to_function_node"],
        # "as_function_node": available via compatibility wrapper
        "function_node": _TopLevel.NOT_AVAILABLE["function_node"],
        "to_function_node": _TopLevel.NOT_AVAILABLE["to_function_node"],
        "Macro": (
            "Flowrep-based nodes don't have subclasses for each individual node; the "
            "closest analogy is to look at the recipe of a parsed function, e.g. "
            "`my_macro_recipe = flowrep.workflow(some_macro_function).flowrep_recipe`, "
            "where `some_macro_function` is written in the constrained subset of "
            "python syntax that flowrep knows how to parse as a macro (cf. the flowrep "
            "docs). Note that these recipes are callable (e.g., `my_recipe(1, 2)`) and "
            "can be used as nodes in a pyiron_workflow workflow (e.g., "
            "`wf.my_subgraph = pwf.node(my_macro_recipe, y=2)`)."
        ),
        # "as_macro_node": available via compatibility wrapper
        "macro_node": _TopLevel.NOT_AVAILABLE["macro_node"],
        "as_dataclass_node": _TopLevel.NOT_AVAILABLE["as_dataclass_node"],
        "dataclass_node": _TopLevel.NOT_AVAILABLE["dataclass_node"],
        "inputs_to_dataframe": _TRANSFORMER_MESSAGE,
        "inputs_to_dict": _TRANSFORMER_MESSAGE,
        "inputs_to_list": _TRANSFORMER_MESSAGE,
        "list_to_outputs": _TRANSFORMER_MESSAGE,
        "While": _TopLevel.NOT_AVAILABLE["while_node"],
        "while_node": _TopLevel.NOT_AVAILABLE["while_node"],
        "while_node_factory": _TopLevel.NOT_AVAILABLE["while_node"],
        "PickleStorage": _STORAGE_MESSAGE,
        "StorageInterface": _STORAGE_MESSAGE,
        "TypeNotFoundError": _STORAGE_MESSAGE,
        "available_backends": _STORAGE_MESSAGE,
        # "Workflow": new functionality exists under the same name
    }


def _getattr_or_raise(
    scope: type[_TopLevel] | type[_ApiSubmodule],
    module_name: str,
    name: str,
) -> NoReturn:
    if (addendum := scope.RELOCATED.get(name)) is not None:
        raise RemovedFeatureError(
            f"{name} is available at a different location in this version of "
            f"pyiron_workflow. {addendum} Note that while they share the same role "
            f"and name, technical differences may exist between the new and old "
            f"objects. {messages.DOWNGRADE}",
            name=name,
        )
    if (addendum := scope.NOT_AVAILABLE.get(name)) is not None:
        raise RemovedFeatureError(
            f"{name} is not available in this version of pyiron_workflow. The most "
            f"recent version retaining this object is pyiron_workflow-0.17.0. For "
            f"the new, flowrep-based implementation: {addendum} {messages.DOWNGRADE} "
            f"For more on flowrep see {_REPO_URL} or the user guide at {_USER_GUIDE}.",
            name=name,
        )
    raise AttributeError(f"module {module_name!r} has no attribute {name!r}")


def get_top_level_stub_or_raise(name: str) -> NoReturn:
    _getattr_or_raise(
        scope=_TopLevel,
        module_name="pyiron_workflow",
        name=name,
    )


def get_api_submodule_stub_or_raise(name: str) -> NoReturn:
    _getattr_or_raise(
        scope=_ApiSubmodule,
        module_name="pyiron_workflow.api",
        name=name,
    )
