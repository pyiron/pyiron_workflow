import importlib.metadata

try:
    # Installed package will find its version
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    # Repository clones will register an unknown version
    __version__ = "0.0.0+unknown"

# Public API
from pyiron_workflow.api import compatibility as compatibility
from pyiron_workflow.api import schemas as schemas
from pyiron_workflow.api import tools as tools
from pyiron_workflow.api.schemas import (
    ExecutorInstructions as ExecutorInstructions,
)
from pyiron_workflow.api.schemas import ProgressHook as ProgressHook
from pyiron_workflow.api.schemas import RunConfig as RunConfig
from pyiron_workflow.api.schemas import Workflow as Workflow
from pyiron_workflow.api.tools import atomic as atomic
from pyiron_workflow.api.tools import dataclass as dataclass
from pyiron_workflow.api.tools import node as node
from pyiron_workflow.api.tools import run as run
from pyiron_workflow.api.tools import workflow as workflow
