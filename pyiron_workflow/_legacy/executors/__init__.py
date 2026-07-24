"""
This module holds customized children of :class:`concurrent.futures.Executor`.
"""

from pyiron_workflow._legacy.executors.cloudpickleprocesspool import (
    CloudpickleProcessPoolExecutor as CloudpickleProcessPoolExecutor,
)
from pyiron_workflow._legacy.executors.wrapped_executorlib import (
    NodeSlurmExecutor as NodeSlurmExecutor,
)
