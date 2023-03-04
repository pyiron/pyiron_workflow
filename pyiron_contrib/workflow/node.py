from __future__ import annotations

import inspect
from typing import get_args, get_type_hints, Optional

from pyiron_contrib.workflow.channels import InputChannel, OutputChannel
from pyiron_contrib.workflow.io import Inputs, Outputs


class Node:
    """
    Nodes have input and output channels that interface with the outside world, and
    a callable that determines what they actually compute. After running, their output
    channels are updated with the results of the node's computation, which
    triggers downstream node updates if those output channels are connected to other
    input channels.

    Nodes can be forced to run, or more gently "updated", which will trigger a
    calculation only if all of the input is ready.

    Nodes can optionally update themselves at instantiation.

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
        update_automatically (bool): Whether to run when you are updated and all your
            input is ready. (Default is True).
        update_now (bool): Whether to force an update at the end of instantiation.
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
        update: If `update_automatically` is true and all your input is ready, will
            run the engine.
        run: Parse and process the input, execute the engine, process the results and
            update the output.
        disconnect: Disconnect all IO connections.

    Note:
        The number of return values for the node function, and the number of output
        labels provided must be consistent!

    Examples:
        Instantiating from `Node`:
        >>> from pyiron_contrib.workflow.node import Node
        >>>
        >>> def start_to_end(a=None):
        ...     return a
        >>>
        >>> def add_one(x=None):
        ...    return x + 1
        >>>
        >>> my_adder = Node(node_function=add_one, output_labels=("y",))
        >>> my_adder.outputs.y.value

        >>> # Nothing! It tried to update automatically, but there's no default for
        >>> # "a", so it's not ready!
        >>> my_adder.inputs.a.update(1)
        >>> my_adder.outputs.y.value
        2

        Subclassing `Node`:
        >>> from pyiron_contrib.workflow.node import Node, pass_all
        >>> from pyiron_contrib.workflow.channels import ChannelTemplate
        >>>
        >>> class ThreeToOne(Node):
        ...     # Expects an engine that maps three numeric values (xyz) to one (w)
        ...     input_channels = [
        ...         ChannelTemplate("x", default=1, types=(int, float)),
        ...         ChannelTemplate("y", default=2, types=(int, float)),
        ...         ChannelTemplate("z", default=3, types=(int, float)),
        ...     ]
        ...     preprocessor = staticmethod(pass_all)
        ...
        ...     @staticmethod
        ...     def postprocessor(**kwargs):
        ...         return pass_all(**kwargs)
        ...     # Neither pre- nor post-processor does anything here,
        ...     # they're just exampels of how to declare them as static
        ...
        ...     output_channels = [
        ...         ChannelTemplate("w", types=(int, float)),
        ...     ]
        ...
        ...     def __init__(self, label: str, engine: callable, **kwargs):
        ...         # We'll modify what's available in init to push our users a certain direction.
        ...         super().__init__(label=label, node_function=node_function, **kwargs)
        >>>
        >>> def add(x, y, z):
        ...     return {"w": x + y + z}
        >>>
        >>> adder = ThreeToOne("add", add)
        >>> adder.outputs.w.value
        6
        >>> def multiply(x, y, z):
        ...     return {"w": x * y * z}
        >>>
        >>> multiplier = ThreeToOne("mult", multiply, z=4)
        >>> multiplier.outputs.w.value
        8
    """
    def __init__(
            self,
            node_function: callable,
            output_labels: tuple[str] | str,
            label: Optional[str] = None,
            input_storage_priority: Optional[dict[str:int]] = None,
            output_storage_priority: Optional[dict[str:int]] = None,
            update_automatically: bool = True,
            update_now: bool = False,
            **kwargs
    ):
        self.node_function = node_function
        self.label = label if label is not None else node_function.__name__

        input_channels = self._build_input_channels(input_storage_priority)
        self.inputs = Inputs(*input_channels)

        if not isinstance(output_labels, (tuple, list)):
            output_labels = (output_labels,)
        output_channels = self._build_output_channels(
            output_labels, output_storage_priority
        )
        self.outputs = Outputs(*output_channels)
        self.update_automatically = update_automatically

        for k, v in kwargs.items():
            if k in self.inputs.labels:
                if isinstance(v, OutputChannel):
                    self.inputs[k] = v
                else:
                    self.inputs[k].update(v)

        if update_now:
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

            channels.append(InputChannel(
                label=label,
                node=self,
                default=default,
                type_hint=type_hint,
                storage_priority=priority,
            ))
        return channels

    def _build_output_channels(self, return_labels, storage_priority: dict[str:int]):
        channels = []
        try:
            type_hints = get_type_hints(self.node_function)["return"]
        except KeyError:
            type_hints = None

        if isinstance(return_labels, str):
            try:
                priority = storage_priority[return_labels]
            except (KeyError, TypeError):
                priority = None

            channels.append(
                OutputChannel(
                    label=return_labels,
                    node=self,
                    type_hint=type_hints,
                    storage_priority=priority,
                )
            )
        else:
            hints = get_args(type_hints) if type_hints is not None else [None] * len(
                return_labels)
            for label, hint in zip(return_labels, hints):
                try:
                    priority = storage_priority[label]
                except (KeyError, TypeError):
                    priority = None

                channels.append(
                    OutputChannel(
                        label=label,
                        node=self,
                        type_hint=hint,
                        storage_priority=priority,
                    )
                )
        return channels

    def update(self) -> None:
        if self.update_automatically and self.ready:
            self.run()

    def run(self) -> None:
        function_output = self.node_function(**self.inputs.to_value_dict())

        if len(self.outputs) == 1:
            function_output = (function_output,)

        for out, value in zip(self.outputs, function_output):
            out.update(value)

    def __call__(self) -> None:
        self.run()

    def disconnect(self):
        self.inputs.disconnect()
        self.outputs.disconnect()

    @property
    def ready(self) -> bool:
        return self.inputs.ready

    @property
    def connected(self) -> bool:
        return self.inputs.connected or self.outputs.connected

    @property
    def fully_connected(self):
        return self.inputs.fully_connected and self.outputs.fully_connected

    def set_storage_priority(self, priority: int):
        self.inputs.set_storage_priority(priority)
        self.outputs.set_storage_priority(priority)
