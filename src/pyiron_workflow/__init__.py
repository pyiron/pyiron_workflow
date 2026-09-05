import importlib.metadata

try:
    # Installed package will find its version
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # Repository clones will register an unknown version
    __version__ = "0.0.0+unknown"

# Public API
from pyiron_workflow.api import (
    ExecutorInstructions as ExecutorInstructions,
)
from pyiron_workflow.api import ProgressHook as ProgressHook
from pyiron_workflow.api import RunConfig as RunConfig
from pyiron_workflow.api import Workflow as Workflow
from pyiron_workflow.api import as_function_node as as_function_node
from pyiron_workflow.api import as_macro_node as as_macro_node
from pyiron_workflow.api import node as node
from pyiron_workflow.api import run as run
from pyiron_workflow.api import schemas as schemas
from pyiron_workflow.api import tools as tools
from pyiron_workflow.compatibility import get_top_level_stub_or_raise as _top_level_stub


def __getattr__(name):
    return _top_level_stub(name)
