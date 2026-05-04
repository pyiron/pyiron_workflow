from __future__ import annotations

from pyiron_workflow._wfms.datatypes import FlowControl, Node


class ForEach(FlowControl):  # Not implemented
    body_node: Node


class If(FlowControl): ...  # Not implemented


class Try(FlowControl): ...  # Not implemented


class While(FlowControl): ...  # Not implemented
