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
from pyiron_workflow.api import node as node
from pyiron_workflow.api import run as run
from pyiron_workflow.api import schemas as schemas
from pyiron_workflow.api import tools as tools
