from __future__ import annotations

import inspect
from functools import partialmethod
from typing import get_args, get_type_hints, Optional, TYPE_CHECKING

from pyiron_contrib.workflow.channels import (
    InputData, OutputData, InputSignal, OutputSignal
)
from pyiron_contrib.workflow.has_to_dict import HasToDict
from pyiron_contrib.workflow.io import Inputs, Outputs, Signals

if TYPE_CHECKING:
    from pyiron_contrib.workflow.workflow import Workflow


class Node(HasToDict):
    """
    Nodes have input and output channels that interface with the outside world, and
    a callable that determines what they actually compute. After running, their output
    channels are updated with the results of the node's computation, which
    triggers downstream node updates if those output channels are connected to other
    input channels.

    Nodes can be forced to run, or more gently "updated", which will trigger a
    calculation only if all of the input is ready.

    Nodes won't update themselves while setting inputs to initial values, but can
    optionally update themselves at the end instantiation.

    Nodes must be instantiated with a callable to deterimine their function, and a tuple
    of strings to name each returned value of that callable.

    The node label (unless otherwise provided), IO types, and input defaults for the
    _automatically_ from introspection of the node function.
    Additional properties like storage priority (present but doesn't do anything yet)
    and ontological type (not yet present) can be set using kwarg dictionaries with
    keys corresponding to the channel labels (i.e. the node arguments of the node
    function, or the output labels provided).

    Actual node instances can either be instances of the base node class, in which case
    the callable node function and output labels *must* be provided, in addition to
    other data, OR they can be instances of children of this class.
    Those children may define some or all of the node behaviour at the class level, and
    modify their signature accordingly so this is not available for alteration by the
    user, e.g. the node function and output labels may be hard-wired.

    Args:
        node_function (callable): The function determining the behaviour of the node.
        output_labels (tuple[str]): A name for each return value of the node function.
        label (str): The node's label. (Defaults to the node function's name.)
        run_automatically (bool): Whether to run when you are updated and all your
            input is ready. (Default is True).
        update_on_instantiation (bool): Whether to force an update at the end of instantiation.
            (Default is False.)
        **kwargs: Any additional keyword arguments whose keyword matches the label of an
            input channel will have their value assigned to that channel.

    Attributes:
        inputs (Inputs): A collection of input channels.
        outputs (Outputs): A collection of output channels.
        ready (bool): All input reports ready.
        connected (bool): Any IO channel has at least one connection.
        fully_connected (bool): Every IO channel has at least one connection.

    Methods:
        update: If `run_automatically` is true and all your input is ready, will
            run the engine.
        run: Parse and process the input, execute the engine, process the results and
            update the output.
        disconnect: Disconnect all IO connections.

    Note:
        The number of return values for the node function, and the number of output
        labels provided must be consistent!

    Examples:
        At the most basic level, to use nodes all we need to do is provide the `Node`
        class with a function and labels for its output, like so:
        >>> from pyiron_contrib.workflow.node import Node
        >>>
        >>> def mwe(x, y):
        ...     return x+1, y-1
        >>>
        >>> plus_minus_1 = Node(mwe, "p1", "m1")
        >>>
        >>> print(plus_minus_1.outputs.p1)
        None

        There is no output because we haven't given our function any input, and it has
        no defaults!
        However, we'll run into a hiccup if we try to update one of the inputs...
        >>> plus_minus_1.inputs.x = 1
        TypeError

        This is because updating an input value triggers the node to update -- i.e. it
        checks if all it's input is of a valid type, and if so attempts to execute its
        node function.
        In this case, our input is untyped -- so it's always considered valid -- and the
        type error comes from our `y - 1` term in the function, which is `None - 1`.

        There are three ways to resolve this: First, we could set
        `run_automatically = False`, then the node would not execute until we
        manually call the `run()` method.
        This impacts the long-term behaviour of the node though, so let's keep
        searching.

        We can provide initial values for our node function at instantiation using our
        kwargs.
        The node update is deferred until _all_ of these initial values are processed.
        Thus, the second solution is to ensure that _all_ the arguments of our function
        are receiving good enough initial values to facilitate an execution of the node
        function at the end of instantiation:
        >>> plus_minus_1 = Node(mwe, "p1", "m1", x=1, y=2)
        >>>
        >>> print(plus_minus_1.outputs.to_value_dict())
        {'p1': 2, 'm1': 1}

        Second, we could add type hints/defaults to our function so that it knows better
        than to try to evaluate itself with bad data.
        Let's make a new node following the second path.

        In this example, note the mixture of old-school (`typing.Union`) and new (`|`)
        type hints as well as nested hinting with a union-type inside the tuple for the
        return hint.
        Our treatment of type hints is **not infinitely robust**, but covers a wide
        variety of common use cases.
        >>> from typing import Union
        >>>
        >>> def hinted_example(
        ...     x: Union[int, float],
        ...     y: int | float = 1
        ... ) -> tuple[int, int | float]:
        ...     return x+1, y-1
        >>>
        >>> plus_minus_1 = Node(hinted_example, "p1", "m1")
        >>>
        >>> plus_minus_1.inputs.x = 1
        >>> print(plus_minus_1.outputs.to_value_dict())
        {'p1': 2, 'm1': 0}

        In this case, we're able to use the default value for `y`, but you can
        experiment with updating `y` first (when `x` still has the invalid value of
        `None`) to verify that the update is not triggered until _both_ inputs have
        valid values.

        In these examples, we've instantiated nodes directly from the base `Node` class,
        and populated their input directly with data.
        In practice, these nodes are meant to be part of complex workflows; that means
        both that you are likely to have particular nodes that get heavily re-used, and
        that you need the nodes to pass data to each other.

        For reusable nodes, we want to create a sub-class of `Node` that fixes some of
        the node behaviour -- usually the `node_function` and `output_labels`.
        There are two straightforward ways to accomplish this.
        The first is to override the `__init__` method directly:
        >>> from typing import Literal, Optional
        >>>
        >>> class AlphabetModThree(Node):
        ...     def __init__(
        ...         self,
        ...         label: Optional[str] = None,
        ...         input_storage_priority: Optional[dict[str, int]] = None,
        ...         output_storage_priority: Optional[dict[str, int]] = None,
        ...         run_automatically: bool = True,
        ...         update_on_instantiation: bool = False,
        ...         **kwargs
        ...     ):
        ...         super().__init__(
        ...             self.alphabet_mod_three,
        ...             "letter",
        ...             labe=label,
        ...             input_storage_priority=input_storage_priority,
        ...             output_storage_priority=output_storage_priority,
        ...             run_automatically=run_automatically,
        ...             update_on_instantiation=update_on_instantiation,
        ...             **kwargs
        ...         )
        ...
        ...     @staticmethod
        ...     def alphabet_mod_three(i: int) -> Literal["a", "b", "c"]:
        ...         return ["a", "b", "c"][i % 3]

        The second effectively does the same thing, but leverages python's
        `functools.partialmethod` to do so much more succinctly.
        In this example, note that the function is declared _before_ `__init__` is set,
        so that it is available in the correct scope (above, we could place it
        afterwards because we were accessing it through self).
        >>> from functools import partialmethod
        >>>
        >>> class Adder(Node):
        ...     @staticmethod
        ...     def adder(x: int = 0, y: int = 0) -> int:
        ...         return x + y
        ...
        ...     __init__ = partialmethod(
        ...         Node.__init__,
        ...         node_function=adder,
        ...         output_labels="sum",
        ...     )

        Finally, instead of setting input to a particular data value, we'll set it to
        be another node's output channel, thus forming a connection.
        When we update the upstream node, we'll see the result passed downstream:
        >>> adder = Adder()
        >>> alpha = AlphabetModThree(i=adder.outputs.sum)
        >>>
        >>> adder.inputs.x = 1
        >>> print(alpha.outputs.letter)
        "b"
        >>> adder.inputs.y = 1
        >>> print(alpha.outputs.letter)
        "c"
        >>> adder.inputs.x = 0
        >>> adder.inputs.y = 0
        >>> print(alpha.outputs.letter)
        "a"

        To see how to use many nodes together, look at the `Workflow` class.
    """
    def __init__(
            self,
            node_function: callable,
            *output_labels: str,
            label: Optional[str] = None,
            input_storage_priority: Optional[dict[str, int]] = None,
            output_storage_priority: Optional[dict[str, int]] = None,
            run_automatically: bool = True,
            update_on_instantiation: bool = True,
            workflow: Optional[Workflow] = None,
            **kwargs
    ):
        self.node_function = node_function
        self.label = label if label is not None else node_function.__name__

        self.workflow = None
        if workflow is not None:
            workflow.add(self)

        input_channels = self._build_input_channels(input_storage_priority)
        self.inputs = Inputs(*input_channels)

        output_channels = self._build_output_channels(
            *output_labels, storage_priority=output_storage_priority
        )
        self.outputs = Outputs(*output_channels)

        self.signals = self._build_signal_channels()

        self.run_automatically = False
        for k, v in kwargs.items():
            if k in self.inputs.labels:
                if isinstance(v, OutputData):
                    self.inputs[k] = v
                else:
                    self.inputs[k].update(v)
        self.run_automatically = run_automatically

        if update_on_instantiation:
            self.update()

    def _build_input_channels(self, storage_priority: dict[str:int]):
        channels = []
        type_hints = get_type_hints(self.node_function)
        parameters = inspect.signature(self.node_function).parameters

        for label, value in parameters.items():
            try:
                priority = storage_priority[label]
            except (KeyError, TypeError):
                priority = None

            try:
                type_hint = type_hints[label]
            except KeyError:
                type_hint = None

            if value.default is not inspect.Parameter.empty:
                default = value.default
            else:
                default = None

            channels.append(InputData(
                label=label,
                node=self,
                default=default,
                type_hint=type_hint,
                storage_priority=priority,
            ))
        return channels

    def _build_output_channels(
            self, *return_labels: str, storage_priority: dict[str:int] = None
    ):
        try:
            type_hints = get_type_hints(self.node_function)["return"]
            if len(return_labels) > 1:
                type_hints = get_args(type_hints)
                if not isinstance(type_hints, tuple):
                    raise TypeError(
                        f"With multiple return labels expected to get a tuple of type "
                        f"hints, but got type {type(type_hints)}"
                    )
                if len(type_hints) != len(return_labels):
                    raise ValueError(
                        f"Expected type hints and return labels to have matching "
                        f"lengths, but got {len(type_hints)} hints and "
                        f"{len(return_labels)} labels: {type_hints}, {return_labels}"
                    )
            else:
                # If there's only one hint, wrap it in a tuple so we can zip it with
                # *return_labels and iterate over both at once
                type_hints = (type_hints,)
        except KeyError:
            type_hints = [None] * len(return_labels)

        channels = []
        for label, hint in zip(return_labels, type_hints):
            try:
                priority = storage_priority[label]
            except (KeyError, TypeError):
                priority = None

            channels.append(
                OutputData(
                    label=label,
                    node=self,
                    type_hint=hint,
                    storage_priority=priority,
                )
            )

        return channels

    def _build_signal_channels(self):
        signals = Signals()
        signals.input.run = InputSignal("run", self, self.run)
        signals.output.ran = OutputSignal("ran", self)
        return signals

    def update(self) -> None:
        if self.run_automatically and self.ready:
            self.run()

    def run(self) -> None:
        function_output = self.node_function(**self.inputs.to_value_dict())

        if len(self.outputs) == 1:
            function_output = (function_output,)

        for out, value in zip(self.outputs, function_output):
            out.update(value)

        self.signals.output.ran()

    def __call__(self) -> None:
        self.run()

    def disconnect(self):
        self.inputs.disconnect()
        self.outputs.disconnect()
        self.signals.disconnect()

    @property
    def ready(self) -> bool:
        return self.inputs.ready

    @property
    def connected(self) -> bool:
        return self.inputs.connected or self.outputs.connected or self.signals.connected

    @property
    def fully_connected(self):
        return self.inputs.fully_connected \
            and self.outputs.fully_connected \
            and self.signals.fully_connected

    def set_storage_priority(self, priority: int):
        self.inputs.set_storage_priority(priority)
        self.outputs.set_storage_priority(priority)

    def to_dict(self):
        return {
            "label": self.label,
            "ready": self.ready,
            "connected": self.connected,
            "fully_connected": self.fully_connected,
            "inputs": self.inputs.to_dict(),
            "outputs": self.outputs.to_dict(),
            "signals": self.signals.to_dict()
        }


class FastNode(Node):
    """
    Like a regular node, but _all_ input channels _must_ have default values provided,
    and the initialization signature forces `` and `` to be `True`.
    """

    def __init__(
            self,
            node_function: callable,
            *output_labels: str,
            label: Optional[str] = None,
            input_storage_priority: Optional[dict[str, int]] = None,
            output_storage_priority: Optional[dict[str, int]] = None,
            workflow: Optional[Workflow] = None,
            **kwargs
    ):
        self.ensure_params_have_defaults(node_function)
        super().__init__(
            node_function,
            *output_labels,
            label=label,
            input_storage_priority=input_storage_priority,
            output_storage_priority=output_storage_priority,
            run_automatically=True,
            update_on_instantiation=True,
            workflow=workflow,
            ** kwargs
        )

    @classmethod
    def ensure_params_have_defaults(cls, fnc: callable) -> None:
        """Raise a `ValueError` if any parameters of the callable lack defaults."""
        if any(
            param.default == inspect._empty
            for param in inspect.signature(fnc).parameters.values()
        ):
            raise ValueError(
                f"{cls.__name__} requires all function parameters to have defaults, "
                f"but {fnc.__name__} has the parameters "
                f"{inspect.signature(fnc).parameters.values()}"
            )


class SingleValueNode(FastNode):
    """
    A fast node that _must_ return only a single value.

    Attribute and item access is modified to finally attempt access on the output value.
    """
    def __init__(
            self,
            node_function: callable,
            *output_labels: str,
            label: Optional[str] = None,
            input_storage_priority: Optional[dict[str, int]] = None,
            output_storage_priority: Optional[dict[str, int]] = None,
            workflow: Optional[Workflow] = None,
            **kwargs
    ):
        self.ensure_there_is_only_one_return_value(output_labels)
        super().__init__(
            node_function,
            *output_labels,
            label=label,
            input_storage_priority=input_storage_priority,
            output_storage_priority=output_storage_priority,
            workflow=workflow,
            **kwargs
        )

    @classmethod
    def ensure_there_is_only_one_return_value(cls, output_labels):
        if len(output_labels) > 1:
            raise ValueError(
                f"{cls.__name__} must only have a single return value, but got "
                f"multiple output labels: {output_labels}"
            )


def node(*output_labels: str):
    """
    A decorator for dynamically creating node classes from functions.

    Decorates a function.
    Takes an output label for each returned value of the function.
    Returns a `Node` subclass whose name is the camel-case version of the function node,
    and whose signature is modified to exclude the node function and output labels
    (which are explicitly defined in the process of using the decorator).
    """

    def as_node(node_function: callable):
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (Node,),  # Define parentage
            {'__init__': partialmethod(Node.__init__, node_function, *output_labels)}
        )

    return as_node


def fast_node(*output_labels: str):
    """
    A decorator for dynamically creating fast node classes from functions.

    Unlike normal nodes, fast nodes _must_ have default values set for all their inputs.
    """
    def as_fast_node(node_function: callable):
        FastNode.ensure_params_have_defaults(node_function)
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (FastNode,),  # Define parentage
            {
                '__init__': partialmethod(
                    FastNode.__init__,
                    node_function,
                    *output_labels
                )
            }
        )

    return as_fast_node


def single_value_node(*output_labels: str):
    """
    A decorator for dynamically creating fast node classes from functions.

    Unlike normal nodes, fast nodes _must_ have default values set for all their inputs.
    """
    def as_single_value_node(node_function: callable):
        SingleValueNode.ensure_there_is_only_one_return_value(output_labels)
        SingleValueNode.ensure_params_have_defaults(node_function)
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (SingleValueNode,),  # Define parentage
            {
                '__init__': partialmethod(
                    SingleValueNode.__init__,
                    node_function,
                    *output_labels
                )
            }
        )

    return as_single_value_node
