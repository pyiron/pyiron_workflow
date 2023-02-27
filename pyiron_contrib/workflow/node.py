from __future__ import annotations

import inspect
from typing import get_args, Optional

from pyiron_contrib.workflow.channels import ChannelTemplate, OutputChannel
from pyiron_contrib.workflow.io import Inputs, Outputs


class Node:
    """
    Nodes have input and output channels that interface with the outside world, and
    internally have a structure of preprocess->run->postprocess. After running, their
    output channels are updated with the results of the node's computation, which
    triggers downstream node updates if those output channels are connected to other
    input channels.

    Nodes can be forced to run, or more gently "updated", which will trigger a
    calculation only if all of the input is ready.

    Nodes can optionally update themselves at instantiation.

    Nodes can be instantiated with keyword arguments for their input channel values.
    These can be values, or output channels. In the latter case, the upstream node
    will need to be updated again before the output channel value gets passed into this
    node's input.

    Actual node instances can either be instances of the base node class, in which case
    all information about IO, processing, and computation needs to be provided at
    instantiation as arguments, OR they can be instances of children of this class.
    Those children may define some or all of the node behaviour at the class level, and
    if they do it is no longer available for specification at instantiation time.

    Args:
        label (str): The node's label.
        input_channels (Optional[list[ChannelTemplate]]): A list of channel templates
            used to create the input. (Default is an empty list.)
        preprocessor (Optional[callable]): Any callable taking only kwargs and returning
            a dict. Will get receive the input values as a dictionary. (Default is
            `pass_all`, a function that just returns the kwargs as a dict.)
        node_function (Optional[callable]): Any callable taking only kwargs and returning
            a dict. Will receive the preprocessor output. (Default is `pass_all`.)
        postprocessor (Optional[callable]): Any callable taking only kwargs and
            returning a dict. Will receive the engine output. (Default is `pass_all`.)
        output_channels (Optional[list[ChannelTemplate]]): A list of channel templates
            used to create the output. Will get updated from the output of the
            postprocessor. (Default is an empty list.)
        update_automatically (bool): Whether to run when you are updated and all your
            input is ready. (Default is True).
        update_now (bool): Whether to call an update at the end of instantiation.
            (Default is True.)
        **kwargs: Any additional keyword arguments whose keyword matches the label of an
            input channel will have their value assigned to that channel.

    Attributes:
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
        The IO keys/channel labels throughout your node need to be consistent:
        input -> pre-processor -> engine -> post-processor -> output. But the processors
        exist so that the terminology (and even number of arguments) for your internal
        engine can differ arbitrarily from the IO interface exposed to users.

    Note:
        When specifying any of `preprocessor`, `engine`, or `postprocessor`.

    Note:
        The simplest case for the  `preprocessor`, `engine`, or `postprocessor` is a
        function, but it is also possible to pass in any class instance where `__call__`
        is defined. These attributes are intended to operate in a purely functional way
        (i.e. without internal state), but it's possible a class-based description may
        be useful.

    Warning:
        `input_channels` and `output_channels` are mutable class-level variables.
        Modifying them in any way is probably unwise.

    Examples:
        Instantiating from `Node`:
        >>> from pyiron_contrib.workflow.node import Node
        >>> from pyiron_contrib.workflow.channels import ChannelTemplate
        >>>
        >>> def start_to_end(a=None):
        ...     return {"x": a}
        >>>
        >>> def add_one(x=None):
        ...    return {"y": x + 1}
        >>>
        >>> my_adder = Node(
        ...     "my_adder",
        ...     input_channels=[ChannelTemplate("a", types=(int, float))],
        ...     preprocessor=start_to_end,
        ...     node_function=add_one,
        ...     # We'll leave the post-processor empty and just align our output
        ...     # with what our engine returns
        ...     output_channels=[ChannelTemplate("y")],
        ... )
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
            output_labels: tuple[str],
            label: Optional[str] = None,
            input_storage_priority: Optional[dict[str:int]] = None,
            output_storage_priority: Optional[dict[str:int]] = None,
            update_automatically: bool = True,
            update_now: bool = True,
            **kwargs
    ):
        self.node_function = node_function
        self.label = label

        input_channels = self._build_input_channels(
            input_storage_priority if input_storage_priority is not None else {}
        )
        self.inputs = Inputs(self, *input_channels)
        output_channels = self._build_output_channels(
            output_labels,
            output_storage_priority if output_storage_priority is not None else {}
        )
        self.outputs = Outputs(self, *output_channels)
        self.update_automatically = update_automatically

        for k, v in kwargs.items():
            if k in self.inputs.labels:
                if isinstance(v, OutputChannel):
                    self.inputs[k] = v
                else:
                    self.inputs[k].update(v)

        if update_now:
            self.update()

    def _build_input_channels(self, input_storage_priority: dict[str:int]):
        channels = []
        for key, value in inspect.signature(self.node_function).parameters.items():
            new_input = {"label": key}
            if value.annotation is not inspect.Parameter.empty:
                new_input["types"] = get_args(value.annotation)
            if value.default is not inspect.Parameter.empty:
                new_input["default"] = value.default
            try:
                new_input["storage_priority"] = input_storage_priority[key]
            except KeyError:
                pass
            channels.append(ChannelTemplate(**new_input))
        return channels

    def _build_output_channels(self, channel_names, storage_priority):
        channels = []
        return_annotations = inspect.signature(self.node_function).return_annotation
        if not isinstance(return_annotations, tuple):
            return_annotations = return_annotations,
        for key, annotation in zip(channel_names, return_annotations):
            new_input = {"label": key}
            if annotation is not inspect.Parameter.empty:
                new_input["types"] = get_args(annotation)
            try:
                new_input["storage_priority"] = storage_priority[key]
            except KeyError:
                pass
            channels.append(ChannelTemplate(**new_input))
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
