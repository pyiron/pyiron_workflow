from pyiron_workflow.api import schemas as schemas
from pyiron_workflow.api import tools as tools
from pyiron_workflow.api.compatibility import as_function_node as as_function_node
from pyiron_workflow.api.compatibility import as_macro_node as as_macro_node
from pyiron_workflow.api.schemas import (
    ExecutorInstructions as ExecutorInstructions,
)
from pyiron_workflow.api.schemas import ProgressHook as ProgressHook
from pyiron_workflow.api.schemas import RunConfig as RunConfig
from pyiron_workflow.api.schemas import Workflow as Workflow
from pyiron_workflow.api.tools import node as node
from pyiron_workflow.api.tools import run as run
from pyiron_workflow.compatibility import get_api_submodule_stub_or_raise as _api_stub


def __getattr__(name):
    return _api_stub(name)
