from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from pyiron_contrib.workflow.io import Input, Output

if TYPE_CHECKING:
    from pyiron_contrib.workflow.channels import ChannelTemplate


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

    Actual node instances can either be instances of the base node class, in which case
    all information about IO, processing, and computation needs to be provided at
    instantiation as arguments, OR they can be instances of children of this class.
    Those children may define some or all of the node behaviour at the class level, and
    if they do it is no longer available for specification at instantiation time.

    Args:
        name (str): The node's name.
        input_channels (Optional[list[ChannelTemplate]]): A list of channel templates
            used to create the input. (Default is an empty list.)
        preprocessor (Optional[callable]): Any callable taking only kwargs and returning
            a dict. Will get receive the input values as a dictionary. (Default is
            `pass_all`, a function that just returns the kwargs as a dict.)
        engine (Optional[callable]): Any callable taking only kwargs and returning
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
        **kwargs: Any additional keyword arguments whose keyword matches the name of an
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
        The IO keys/channel names throughout your node need to be consistent:
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
        ...     engine=add_one,
        ...     # We'll leave the post-processor empty and just align our output
        ...     # with what our engine returns
        ...     output_channels=[ChannelTemplate("y")],
        ... )
        >>> my_adder.output.y.value

        >>> # Nothing! It tried to update automatically, but there's no default for
        >>> # "a", so it's not ready!
        >>> my_adder.input.a.update(1)
        >>> my_adder.output.y.value
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
        ...     def __init__(self, name: str, engine: callable, **kwargs):
        ...         # We'll modify what's available in init to push our users a certain direction.
        ...         super().__init__(name=name, engine=engine, **kwargs)
        >>>
        >>> def add(x, y, z):
        ...     return {"w": x + y + z}
        >>>
        >>> adder = ThreeToOne("add", add)
        >>> adder.output.w.value
        6
        >>> def multiply(x, y, z):
        ...     return {"w": x * y * z}
        >>>
        >>> multiplier = ThreeToOne("mult", multiply, z=4)
        >>> multiplier.output.w.value
        8
    """
    # Children may define sub-components at the class level and override __init__ to
    # not accept them
    input_channels: list[ChannelTemplate] = None
    preprocessor: callable = None
    engine: callable = None
    postprocessor: callable = None
    output_channels: list[ChannelTemplate] = None

    def __init__(
            self,
            name: Optional[str] = None,
            input_channels: Optional[list[ChannelTemplate]] = None,
            preprocessor: Optional[callable] = None,
            engine: Optional[callable] = None,
            postprocessor: Optional[callable] = None,
            output_channels: Optional[list[ChannelTemplate]] = None,
            update_automatically: bool = True,
            update_now: bool = True,
            **kwargs
    ):
        for key, arg in [
            ("input_channels", input_channels),
            ("preprocessor", preprocessor),
            ("engine", engine),
            ("postprocessor", postprocessor),
            ("output_channels", output_channels)
        ]:
            if arg is not None and getattr(self, key) is not None:
                raise ValueError(
                    f"{key} can be defined at the class level XOR passed as an argument"
                    f"to __init__, but {name} just got both."
                )

        self.name = name

        self.input_channels = input_channels or self.input_channels or []
        self.preprocessor = preprocessor or self.preprocessor or pass_all
        self.engine = engine or self.engine or pass_all
        self.postprocessor = postprocessor or self.postprocessor or pass_all
        self.output_channels = output_channels or self.output_channels or []

        self.input = Input(self, *self.input_channels)
        self.output = Output(self, *self.output_channels)
        self.update_automatically = update_automatically

        for k, v in kwargs.items():
            if k in self.input.names:
                self.input[k].update(v)

        if update_now:
            self.update()

    def update(self) -> None:
        if self.update_automatically and self.ready:
            self.run()

    def run(self) -> None:
        engine_input = self.preprocessor(**self.input.to_value_dict())
        engine_output = self.engine(**engine_input)
        node_output = self.postprocessor(**engine_output)
        self._update_output(node_output)

    def _update_output(self, data: dict):
        for k, v in data.items():
            self.output[k].update(v)

    def __call__(self) -> None:
        self.run()

    def disconnect(self):
        self.input.disconnect()
        self.output.disconnect()

    @property
    def ready(self) -> bool:
        return self.input.ready

    @property
    def connected(self) -> bool:
        return self.input.connected or self.output.connected

    @property
    def fully_connected(self):
        return self.input.fully_connected and self.output.fully_connected

    def set_storage_priority(self, priority: int):
        self.input.set_storage_priority(priority)
        self.output.set_storage_priority(priority)


def pass_all(**kwargs) -> dict:
    """Just returns everything it gets as a dictionary."""
    return kwargs
