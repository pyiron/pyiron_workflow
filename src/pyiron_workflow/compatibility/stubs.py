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

    RELOCATED: dict[str, str] = {}

    NOT_AVAILABLE: dict[str, str] = {}


class _ApiSubmodule:
    """
    From the 0.17.0 api.py submodule
    """

    RELOCATED: dict[str, str] = {}

    NOT_AVAILABLE: dict[str, str] = {}


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
