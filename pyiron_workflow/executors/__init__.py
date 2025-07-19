"""
This module holds customized children of :class:`concurrent.futures.Executor`.
"""

from pyiron_workflow.executors.cloudpickleprocesspool import (
    CloudpickleProcessPoolExecutor,
)
from pyiron_workflow.executors.wrapped_executorlib import NodeSlurmExecutor
