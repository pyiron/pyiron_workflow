"""
Wrapper for running a node as a pyiron base job.

Two approaches are provided while we work out which is more convenient and how some
edge cases may be handled differently:
- `NodeOutputJob(PythonFunctionContainerJob)`, which gets serialized like any other
  wrapped python code.
- `StoredNodeJob(TemplateJob)`, which uses the node's underlying storage capabilities.

The intent of this module is to provide immediate access to pyiron's queue submission
functionality, while in the long run this should be integrated more directly with the
workflows. E.g., this solution doesn't permit individual nodes in a workflow to be
submitted to the queue, but only standalone nodes/macros, or entire workflows.

Parallel processing inside node job will not be possible until the node executor can
take values _other_ than an actual executor instance (which don't serialize). The
infrastructure is in place for this in :meth:`Node._parse_executor`, but it is not yet
leveraged.
"""

from __future__ import annotations

import base64
import inspect
import os
import sys

import cloudpickle

from pyiron_base import TemplateJob, JOB_CLASS_DICT
from pyiron_base.jobs.flex.pythonfunctioncontainer import (
    PythonFunctionContainerJob,
    get_function_parameter_dict,
)
from pyiron_workflow.node import Node
from h5io._h5io import _import_class


def _node_out(node):
    node.run()
    return node.outputs.to_value_dict()


class NodeOutputJob(PythonFunctionContainerJob):
    """
    A `PythonFunctionContainer` node that explicitly runs a node input and returns its
    output value dictionary.

    Examples:
        >>> from pyiron_base import Project
        >>> from pyiron_workflow import Workflow
        >>> import pyiron_workflow.job  # To get the job registered in JOB_CLASS_DICT
        >>>
        >>> wf = Workflow("pyiron_node", overwrite_save=True)
        >>> wf.answer = Workflow.create.standard.UserInput(42)  # Or your nodes
        >>>
        >>> pr = Project("test")
        >>>
        >>> nj = pr.create.job.NodeOutputJob("my_node")
        >>> nj.input["node"] = wf
        >>> nj.run()  # doctest:+ELLIPSIS
        The job ...
        >>> print(nj.output)
        DataContainer({'answer__user_input': 42})

        >>> lj = pr.load(nj.job_name)
        >>> print(nj.output)
        DataContainer({'answer__user_input': 42})

        >>> pr.remove_jobs(recursive=True, silently=True)
        >>> pr.remove(enable=True)

    Warnings:
        All submitted nodes must be importable from their module at load time, or
        loading will fail. This means node definitions can't be nested inside another
        object, and any nodes defined in `__main__` (e.g. in a jupyter notebook) must
        be redefined. If node definitions get changed between saving and loading, all
        bets are off.
    """

    def __init__(self, project, job_name):
        if sys.version_info < (3, 11):
            raise NotImplementedError("Node jobs are only available in python 3.11+")
        super().__init__(project, job_name)
        self._function = _node_out
        self.input.update(get_function_parameter_dict(funct=_node_out))
        self._executor_type = None

    @property
    def python_function(self):
        return self._function

    @python_function.setter
    def python_function(self, funct):
        raise NotImplementedError(
            f"{self.__class__.__name__}'s python function is to run the node and get "
            f"its output values, and this may not be overridden."
        )

    def validate_ready_to_run(self):
        if not isinstance(self.input["node"], Node):
            raise TypeError(f"'node' input must be of type {Node.__name__}")
        elif not self.input["node"].ready:
            nl = "\n"
            raise ValueError(
                f"Node not ready:{nl}{self.input['node'].readiness_report}"
            )

    def save(self):
        # DataContainer can't handle custom reconstructors, so convert the node to
        # bytestream
        self.input["node"] = base64.b64encode(
            cloudpickle.dumps(self.input["node"])
        ).decode("utf-8")
        super().save()

    def run_static(self):
        # Overrides the parent method
        # Copy and paste except for the output update, which makes sure the output is
        # flat and not tested beneath "result"

        # Unpack the node
        input_dict = self.input.to_builtin()
        input_dict["node"] = cloudpickle.loads(base64.b64decode(self.input["node"]))

        if (
            self._executor_type is not None
            and "executor" in inspect.signature(self._function).parameters.keys()
        ):
            del input_dict["executor"]
            output = self._function(
                **input_dict, executor=self._get_executor(max_workers=self.server.cores)
            )
        else:
            output = self._function(**input_dict)
        self.output.update(output)  # DIFFERS FROM PARENT METHOD
        self.to_hdf()
        self.status.finished = True

    def get_input_node(self):
        """
        On saving, we turn the input node into a bytestream so that the DataContainer
        can store it. You might want to look at it again though, so you can use this
        to unpack it

        Returns:
            (Node): The input node as a node again
        """
        if isinstance(self.input["node"], Node):
            return self.input["node"]
        else:
            return cloudpickle.loads(base64.b64decode(self.input["node"]))


JOB_CLASS_DICT[NodeOutputJob.__name__] = NodeOutputJob.__module__


class NodeJob(NodeOutputJob):
    # Just to expose it under the simpler name per @JNmpi's request
    # This is a temporary change ahead of the 2024 DPG conference,
    # in the long run these job names are likely to take on different meanings
    pass


JOB_CLASS_DICT[NodeJob.__name__] = NodeJob.__module__


class StoredNodeJob(TemplateJob):
    """
    This job is an intermediate feature for accessing pyiron's queue submission
    infrastructure for nodes (function nodes, macros, or entire workflows).

    It leans directly on the storage capabilities of the node itself, except for
    the node class and name, and the storage backend mode, all of which are held in the
    traditional job input. (WARNING: This might be fragile to adjusting the storage
    backend on the node _after_ the node has been assign to the job.)

    The job provides direct access to its owned node (as both input and output) on the
    :attr:`node` attribute. The only requirement is that the node have an untouched
    working directory (so we can make sure its files get stored _inside_ the job's
    directory tree), and that it be saveable (not all objects work with the "h5io"
    storage backend, e.g. `ase.Calculator` objects may break it).

    Examples:
        >>> from pyiron_base import Project
        >>> from pyiron_workflow import Workflow
        >>> import pyiron_workflow.job  # To get the job registered in JOB_CLASS_DICT
        >>>
        >>> wf = Workflow("pyiron_node", overwrite_save=True)
        >>> wf.answer = Workflow.create.standard.UserInput(42)  # Or your nodes
        >>>
        >>> pr = Project("test")
        >>>
        >>> nj = pr.create.job.StoredNodeJob("my_node")
        >>> nj.node = wf
        >>> nj.run()  # doctest:+ELLIPSIS
        The job my_node was saved and received the ID: ...
        >>> print(nj.node.outputs.to_value_dict())
        {'answer__user_input': 42}

        >>> lj = pr.load(nj.job_name)
        >>> print(nj.node.outputs.to_value_dict())
        {'answer__user_input': 42}

        >>> pr.remove_jobs(recursive=True, silently=True)
        >>> pr.remove(enable=True)


    Warnings:
        Node jobs rely on storing the node to file, which means these are also only
        available for python >= 3.11.

        The job can be run with `run_mode="non_modal"`, but _only_ if all the nodes
        being run are defined in an importable file location -- i.e. copying and
        pasting the example above into a jupyter notebook works fine in modal mode, but
        will throw an exception if you try to run it non-modally.

        This hasn't been tested for running on a remote queue. It should work, but it's
        _possible_ the same requirement from non-modal mode (importable nodes) will
        apply.
    """

    def __init__(self, project, job_name):
        if sys.version_info < (3, 11):
            raise NotImplementedError("Node jobs are only available in python 3.11+")

        super().__init__(project, job_name)
        self._python_only_job = True
        self._write_work_dir_warnings = False
        self._node = None
        self.input._label = None
        self.input._class_type = None
        self.input._storage_backend = None

    @property
    def node(self) -> Node:
        if self._node is None and self.status.finished:
            self._load_node()
        return self._node

    @node.setter
    def node(self, new_node: Node):
        if self._node is not None:
            raise ValueError("Node already set, make a new job")
        elif self._node_working_directory_already_there(new_node):
            self.raise_working_directory_error()
        else:
            self._node = new_node
            self.input._class_type = (
                f"{new_node.__class__.__module__}." f"{new_node.__class__.__name__}"
            )
            self.input._label = new_node.label
            self.input._storage_backend = new_node.storage_backend

    @staticmethod
    def _node_working_directory_already_there(node):
        return node._working_directory is not None

    @staticmethod
    def raise_working_directory_error():
        raise ValueError("Only nodes with un-touched working directories!")

    def _save_node(self):
        here = os.getcwd()
        os.makedirs(self.working_directory, exist_ok=True)
        os.chdir(self.working_directory)
        self.node.save()
        os.chdir(here)

    def _load_node(self):
        here = os.getcwd()
        os.chdir(self.working_directory)
        self._node = _import_class(self.input._class_type)(
            label=self.input._label,
            storage_backend=self.input._storage_backend,
        )
        os.chdir(here)

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(hdf=hdf, group_name=group_name)
        self._save_node()

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(hdf=hdf, group_name=group_name)
        self._load_node()

    def validate_ready_to_run(self):
        if self._node_working_directory_already_there(self.node):
            self.raise_working_directory_error()

    def run_static(self):
        self.status.running = True
        self.node.run()
        self.to_hdf()
        self.status.finished = True


JOB_CLASS_DICT[StoredNodeJob.__name__] = StoredNodeJob.__module__
