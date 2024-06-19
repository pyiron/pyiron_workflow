"""
We want to be able to operate freely on (single-output) nodes and channels as though
they were regular objects, but still retain our graph paradigm.

To accomplish this, we overload :class:`pyiron_workflow.channel.OutputData` to be able
to inject new nodes into the graph dynamically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, TYPE_CHECKING

from pyiron_workflow.channels import OutputData, NOT_DATA
from pyiron_workflow.mixin.has_interface_mixins import HasChannel
from pyiron_workflow.io import Outputs, HasIO

if TYPE_CHECKING:
    from pyiron_workflow.node import Node


class OutputDataWithInjection(OutputData):
    """
    Output data that must have a :class:`pyiron_workflow.node.Node` for its
    :attr:`owner`, and which is able to inject new nodes into that owner's graph, e.g.
    to accomplish operations on the channel.

    This class facilitates many (but not all) python operators by injecting a new
    node to perform that operation. Where the operator is not supported, we try to
    support using the operator's dunder name as a method, e.g. `==` gives us trouble
    with hashing, but this exploits the dunder method `.__eq__(other)`, so you can call
    `.eq(other)` on output data.
    These new nodes are instructed to run at the end of instantiation, but this fails
    cleanly in case they are not ready. This is intended to accommodate two likely
    scenarios: if you're injecting a node on top of an existing result you probably
    want the injection result to also be immediately available, but if you're injecting
    it at the end of something that hasn't run yet you don't want to see an error.
    """

    def __init__(
        self,
        label: str,
        owner: Node,
        default: Optional[Any] = NOT_DATA,
        type_hint: Optional[Any] = None,
        strict_hints: bool = True,
        value_receiver: Optional[OutputData] = None,
    ):
        # Override parent method to give the new owner type hint
        super().__init__(
            label=label,
            owner=owner,
            default=default,
            type_hint=type_hint,
            strict_hints=strict_hints,
            value_receiver=value_receiver,
        )

    @staticmethod
    def _other_label(other):
        return (
            other.channel.scoped_label if isinstance(other, HasChannel) else str(other)
        )

    def _get_injection_label(self, injection_class, *args):
        other_labels = "_".join(self._other_label(other) for other in args)
        suffix = f"_{other_labels}" if len(args) > 0 else ""
        nominal_label = f"{self.scoped_label}_{injection_class.__name__}{suffix}"
        hashed = str(hash(nominal_label)).replace("-", "m")
        return f"injected_{injection_class.__name__}_{hashed}"

    def _node_injection(self, injection_class, *args, inject_self=True):
        """
        Create a new node with the same parent as this channel's node, and feed it
        arguments, or load such a node if it already exists on the parent (based on a
        name dynamically generated from the injected node class and arguments).

        Args:
            injection_class (type[Node]): The new node class to instantiate
            *args: Any arguments for that function node
            inject_self (bool): Whether to pre-pend the args with self. (Default is
                True.)

        Returns:
            (Node): The instantiated or loaded node.
        """
        label = self._get_injection_label(injection_class, *args)
        try:
            # First check if the node already exists
            return self.owner.parent.children[label]
        except (AttributeError, KeyError):
            # Fall back on creating a new node in case parent is None or node nexists
            node_args = (self, *args) if inject_self else args
            return injection_class(
                *node_args, parent=self.owner.parent, label=label, run_after_init=True
            )

    # We don't wrap __all__ the operators, because you might really want the string or
    # hash or whatever of the actual channel. But we do wrap all the dunder methods
    # that should be unambiguously referring to an operation on values

    def __getattr__(self, name):
        from pyiron_workflow.nodes.standard import GetAttr

        if name == "to_hdf":
            raise AttributeError(
                "This is just a failsafe to protect us against other elements of the "
                "pyiron ecosystem (pyiron_base's DataContainer) running a "
                "`hasattr('to_hdf')` check on us and accidentally injecting a new "
                "getattr node."
            )
        if name.startswith("_"):
            raise AttributeError(
                f"{self.full_label} ({OutputDataWithInjection.__name__}) tried to "
                f"inject on the attribute {name}, but injecting on private attributes "
                f"is forbidden -- if you really need it create a {GetAttr.__name__} "
                f"node manually."
            )
        return self._node_injection(GetAttr, name)

    def __getitem__(self, item):
        # Break slices into deeper injections, if any slice arguments are channel-like
        if isinstance(item, slice) and any(
            isinstance(slice_input, HasChannel)
            for slice_input in [item.start, item.stop, item.step]
        ):
            from pyiron_workflow.nodes.standard import Slice

            item = self._node_injection(
                Slice, item.start, item.stop, item.step, inject_self=False
            )

        from pyiron_workflow.nodes.standard import GetItem

        return self._node_injection(GetItem, item)

    def __lt__(self, other):
        from pyiron_workflow.nodes.standard import LessThan

        return self._node_injection(LessThan, other)

    def __le__(self, other):
        from pyiron_workflow.nodes.standard import LessThanEquals

        return self._node_injection(LessThanEquals, other)

    def eq(self, other):
        from pyiron_workflow.nodes.standard import Equals

        return self._node_injection(Equals, other)

    def __ne__(self, other):
        from pyiron_workflow.nodes.standard import NotEquals

        return self._node_injection(NotEquals, other)

    def __gt__(self, other):
        from pyiron_workflow.nodes.standard import GreaterThan

        return self._node_injection(GreaterThan, other)

    def __ge__(self, other):
        from pyiron_workflow.nodes.standard import GreaterThanEquals

        return self._node_injection(GreaterThanEquals, other)

    def bool(self):
        from pyiron_workflow.nodes.standard import Bool

        return self._node_injection(Bool)

    def len(self):
        from pyiron_workflow.nodes.standard import Length

        return self._node_injection(Length)

    def contains(self, other):
        from pyiron_workflow.nodes.standard import Contains

        return self._node_injection(Contains, other)

    def __add__(self, other):
        from pyiron_workflow.nodes.standard import Add

        return self._node_injection(Add, other)

    def __sub__(self, other):
        from pyiron_workflow.nodes.standard import Subtract

        return self._node_injection(Subtract, other)

    def __mul__(self, other):
        from pyiron_workflow.nodes.standard import Multiply

        return self._node_injection(Multiply, other)

    def __rmul__(self, other):
        from pyiron_workflow.nodes.standard import RightMultiply

        return self._node_injection(RightMultiply, other)

    def __matmul__(self, other):
        from pyiron_workflow.nodes.standard import MatrixMultiply

        return self._node_injection(MatrixMultiply, other)

    def __truediv__(self, other):
        from pyiron_workflow.nodes.standard import Divide

        return self._node_injection(Divide, other)

    def __floordiv__(self, other):
        from pyiron_workflow.nodes.standard import FloorDivide

        return self._node_injection(FloorDivide, other)

    def __mod__(self, other):
        from pyiron_workflow.nodes.standard import Modulo

        return self._node_injection(Modulo, other)

    def __pow__(self, other):
        from pyiron_workflow.nodes.standard import Power

        return self._node_injection(Power, other)

    def __and__(self, other):
        from pyiron_workflow.nodes.standard import And

        return self._node_injection(And, other)

    def __xor__(self, other):
        from pyiron_workflow.nodes.standard import XOr

        return self._node_injection(XOr, other)

    def __or__(self, other):
        from pyiron_workflow.nodes.standard import Or

        return self._node_injection(Or, other)

    def __neg__(self):
        from pyiron_workflow.nodes.standard import Negative

        return self._node_injection(Negative)

    def __pos__(self):
        from pyiron_workflow.nodes.standard import Positive

        return self._node_injection(Positive)

    def __abs__(self):
        from pyiron_workflow.nodes.standard import Absolute

        return self._node_injection(Absolute)

    def __invert__(self):
        from pyiron_workflow.nodes.standard import Invert

        return self._node_injection(Invert)

    def int(self):
        from pyiron_workflow.nodes.standard import Int

        return self._node_injection(Int)

    def float(self):
        from pyiron_workflow.nodes.standard import Float

        return self._node_injection(Float)

    def __round__(self):
        from pyiron_workflow.nodes.standard import Round

        return self._node_injection(Round)


class OutputsWithInjection(Outputs):
    @property
    def _channel_class(self) -> type(OutputDataWithInjection):
        return OutputDataWithInjection


class HasIOWithInjection(HasIO, ABC):
    @property
    @abstractmethod
    def outputs(self) -> OutputsWithInjection:
        pass
