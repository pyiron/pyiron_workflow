from __future__ import annotations

import inspect
import warnings
from functools import partialmethod
from typing import get_args, get_type_hints, Optional, TYPE_CHECKING

from pyiron_contrib.workflow.channels import InputData, OutputData, NotData
from pyiron_contrib.workflow.has_channel import HasChannel
from pyiron_contrib.workflow.io import Inputs, Outputs, Signals
from pyiron_contrib.workflow.node import Node

if TYPE_CHECKING:
    from pyiron_contrib.workflow.composite import Composite
    from pyiron_contrib.workflow.workflow import Workflow


class Function(Node):
    """
    Function nodes wrap an arbitrary python function.
    Node IO, including type hints, is generated automatically from the provided function
    and (in the case of labeling output channels) the provided output labels.
    On running, the function node executes this wrapped function with its current input
    and uses the results to populate the node output.

    Function nodes must be instantiated with a callable to deterimine their function,
    and a string to name each returned value of that callable. (If you really want to
    return a tuple, just have multiple return values but only one output label -- there
    is currently no way to mix-and-match, i.e. to have multiple return values at least
    one of which is a tuple.)

    The node label (unless otherwise provided), IO types, and input defaults for the
    node are produced _automatically_ from introspection of the node function.
    Additional properties like storage priority (present but doesn't do anything yet)
    and ontological type (not yet present) can be set using kwarg dictionaries with
    keys corresponding to the channel labels (i.e. the node arguments of the node
    function, or the output labels provided).

    Actual function node instances can either be instances of the base node class, in
    which case the callable node function and output labels *must* be provided, in
    addition to other data, OR they can be instances of children of this class.
    Those children may define some or all of the node behaviour at the class level, and
    modify their signature accordingly so this is not available for alteration by the
    user, e.g. the node function and output labels may be hard-wired.

    Although not strictly enforced, it is a best-practice that where possible, function
    nodes should be both functional (always returning the same output given the same
    input) and idempotent (not modifying input data in-place, but creating copies where
    necessary and returning new objects as output).

    By default, function nodes will attempt to run whenever one or more inputs is
    updated, and will attempt to update on initialization (after setting _all_ initial
    input values).

    Output is updated in the `process_run_result` inside the parent class `finish_run`
    call, such that output data gets pushed after the node stops running but before
    then `ran` signal fires.

    Args:
        node_function (callable): The function determining the behaviour of the node.
        *output_labels (str): A name for each return value of the node function.
        label (str): The node's label. (Defaults to the node function's name.)
        run_on_updates (bool): Whether to run when you are updated and all your
            input is ready. (Default is True).
        update_on_instantiation (bool): Whether to force an update at the end of
            instantiation. (Default is True.)
        channels_requiring_update_after_run (list[str]): All the input channels named
            here will be set to `wait_for_update()` at the end of each node run, such
            that they are not `ready` again until they have had their `.update` method
            called. This can be used to create sets of input data _all_ of which must
            be updated before the node is ready to produce output again. (Default is
            None, which makes the list empty.)
        **kwargs: Any additional keyword arguments whose keyword matches the label of an
            input channel will have their value assigned to that channel.

    Attributes:
        inputs (Inputs): A collection of input data channels.
        outputs (Outputs): A collection of output data channels.
        signals (Signals): A holder for input and output collections of signal channels.
        ready (bool): All input reports ready, not running or failed.
        running (bool): Currently running.
        failed (bool): An exception was thrown when executing the node function.
        connected (bool): Any IO channel has at least one connection.
        fully_connected (bool): Every IO channel has at least one connection.

    Methods:
        update: If `run_on_updates` is true and all your input is ready, will
            run the engine.
        run: Parse and process the input, execute the engine, process the results and
            update the output.
        disconnect: Disconnect all data and signal IO connections.

    Examples:
        At the most basic level, to use nodes all we need to do is provide the
        `Function` class with a function and labels for its output, like so:
        >>> from pyiron_contrib.workflow.function import Function
        >>>
        >>> def mwe(x, y):
        ...     return x+1, y-1
        >>>
        >>> plus_minus_1 = Function(mwe, "p1", "m1")
        >>>
        >>> print(plus_minus_1.outputs.p1)
        <class 'pyiron_contrib.workflow.channels.NotData'>

        There is no output because we haven't given our function any input, it has
        no defaults, and we never ran it! It tried to `update()` on instantiation, but
        the update never got to `run()` because the node could see that some its input
        had never been specified. So outputs have the channel default value of
        `NotData` -- a special non-data class (since `None` is sometimes a meaningful
        value in python).

        We'll run into a hiccup if we try to set only one of the inputs and force the
        run:
        >>> plus_minus_1.inputs.x = 2
        >>> plus_minus_1.run()
        TypeError

        This is because the second input (`y`) still has no input value, so we can't do
        the sum.

        Once we update `y`, all the input is ready and the automatic `update()` call
        will be allowed to proceed to a `run()` call, which succeeds and updates the
        output:
        >>> plus_minus_1.inputs.x = 3
        >>> plus_minus_1.outputs.to_value_dict()
        {'p1': 3, 'm1': 2}

        We can also, optionally, provide initial values for some or all of the input
        >>> plus_minus_1 = Function(mwe, "p1", "m1",  x=1)
        >>> plus_minus_1.inputs.y = 2  # Automatically triggers an update call now
        >>> plus_minus_1.outputs.to_value_dict()
        {'p1': 2, 'm1': 1}

        Finally, we might stop these updates from happening automatically, even when
        all the input data is present and available:
        >>> plus_minus_1 = Function(
        ...     mwe, "p1", "m1",
        ...     x=0, y=0,
        ...     run_on_updates=False, update_on_instantiation=False
        ... )
        >>> plus_minus_1.outputs.p1.value
        <class 'pyiron_contrib.workflow.channels.NotData'>

        With these flags set, the node requires us to manually call a run:
        >>> plus_minus_1.run()
        >>> plus_minus_1.outputs.to_value_dict()
        {'p1': 1, 'm1': -1}

        So function nodes have the most basic level of protection that they won't run
        if they haven't seen any input data.
        However, we could still get them to raise an error by providing the _wrong_
        data:
        >>> plus_minus_1 = Function(mwe, "p1", "m1", x=1, y="can't add to an int")
        TypeError

        Here everything tries to run automatically, but we get an error from adding the
        integer and string!
        We can make our node even more sensible by adding type
        hints (and, optionally, default values) when defining the function that the node
        wraps.
        The node will automatically figure out defaults and type hints for the IO
        channels from inspection of the wrapped function.

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
        >>> plus_minus_1 = Function(hinted_example, "p1", "m1", x="not an int")
        >>> plus_minus_1.outputs.to_value_dict()
        {'p1': <class 'pyiron_contrib.workflow.channels.NotData'>, 'm1': <class
        'pyiron_contrib.workflow.channels.NotData'>}

        Here, even though all the input has data, the node sees that some of it is the
        wrong type and so the automatic updates don't proceed all the way to a run.
        Note that the type hinting doesn't actually prevent us from assigning bad values
        directly to the channel (although it will, by default, prevent connections
        _between_ type-hinted channels with incompatible hints), but it _does_ stop the
        node from running and throwing an error because it sees that the channel (and
        thus node) is not ready
        >>> plus_minus_1.inputs.x.value
        'not an int'

        >>> plus_minus_1.ready, plus_minus_1.inputs.x.ready, plus_minus_1.inputs.y.ready
        (False, False, True)

        In these examples, we've instantiated nodes directly from the base `Function`
        class, and populated their input directly with data.
        In practice, these nodes are meant to be part of complex workflows; that means
        both that you are likely to have particular nodes that get heavily re-used, and
        that you need the nodes to pass data to each other.

        For reusable nodes, we want to create a sub-class of `Function` that fixes some
        of the node behaviour -- usually the `node_function` and `output_labels`.

        This can be done most easily with the `node` decorator, which takes a function
        and returns a node class:
        >>> from pyiron_contrib.workflow.function import function_node
        >>>
        >>> @function_node("p1", "m1")
        ... def my_mwe_node(
        ...     x: int | float, y: int | float = 1
        ... ) -> tuple[int | float, int | float]:
        ...     return x+1, y-1
        >>>
        >>> node_instance = my_mwe_node(x=0)
        >>> node_instance.outputs.to_value_dict()
        {'p1': 1, 'm1': 0}

        Where we've passed the output labels and class arguments to the decorator,
        and inital values to the newly-created node class (`my_mwe_node`) at
        instantiation.
        Because we provided a good initial value for `x`, we get our result right away.

        Using the decorator is the recommended way to create new node classes, but this
        magic is just equivalent to these two more verbose ways of defining a new class.
        The first is to override the `__init__` method directly:
        >>> from typing import Literal, Optional
        >>>
        >>> class AlphabetModThree(Function):
        ...     def __init__(
        ...         self,
        ...         label: Optional[str] = None,
        ...         run_on_updates: bool = True,
        ...         update_on_instantiation: bool = False,
        ...         **kwargs
        ...     ):
        ...         super().__init__(
        ...             self.alphabet_mod_three,
        ...             "letter",
        ...             label=label,
        ...             run_on_updates=run_on_updates,
        ...             update_on_instantiation=update_on_instantiation,
        ...             **kwargs
        ...         )
        ...
        ...     @staticmethod
        ...     def alphabet_mod_three(i: int) -> Literal["a", "b", "c"]:
        ...         return ["a", "b", "c"][i % 3]

        Note that we've overridden the default value for `update_on_instantiation`
        above.
        We can also provide different defaults for these flags as kwargs in the
        decorator.

        The second effectively does the same thing, but leverages python's
        `functools.partialmethod` to do so much more succinctly.
        In this example, note that the function is declared _before_ `__init__` is set,
        so that it is available in the correct scope (above, we could place it
        afterwards because we were accessing it through self).
        >>> from functools import partialmethod
        >>>
        >>> class Adder(Function):
        ...     @staticmethod
        ...     def adder(x: int = 0, y: int = 0) -> int:
        ...         return x + y
        ...
        ...     __init__ = partialmethod(
        ...         Function.__init__,
        ...         adder,
        ...         "sum",
        ...     )

        Finally, let's put it all together by using both of these nodes at once.
        Instead of setting input to a particular data value, we'll set it to
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

        To see more details on how to use many nodes together, look at the
        `Workflow` class.

    Comments:

        If you use the function argument `self` in the first position, the
        whole node object is inserted there:

        >>> def with_self(self, x):
        >>>     ...
        >>>     return x

        For this function, you don't have the freedom to choose `self`, because
        pyiron automatically sets the node object there (which is also the
        reason why you do not see `self` in the list of inputs).
    """

    def __init__(
        self,
        node_function: callable,
        *output_labels: str,
        label: Optional[str] = None,
        run_on_updates: bool = True,
        update_on_instantiation: bool = True,
        channels_requiring_update_after_run: Optional[list[str]] = None,
        parent: Optional[Composite] = None,
        **kwargs,
    ):
        super().__init__(
            label=label if label is not None else node_function.__name__,
            parent=parent,
            # **kwargs,
        )
        if len(output_labels) == 0:
            raise ValueError("Nodes must have at least one output label.")

        self.node_function = node_function

        self._inputs = None
        self._outputs = None
        self._output_labels = output_labels
        # TODO: Parse output labels from the node function in case output_labels is None

        self.signals = self._build_signal_channels()

        self.channels_requiring_update_after_run = (
            []
            if channels_requiring_update_after_run is None
            else channels_requiring_update_after_run
        )
        self._verify_that_channels_requiring_update_all_exist()

        self.run_on_updates = False
        # Temporarily disable running on updates to set all initial values at once
        for k, v in kwargs.items():
            if k in self.inputs.labels:
                self.inputs[k] = v
            elif k not in self._init_keywords:
                warnings.warn(f"The keyword '{k}' was received but not used.")
        self.run_on_updates = run_on_updates  # Restore provided value

        if update_on_instantiation:
            self.update()

    @property
    def _input_args(self):
        return inspect.signature(self.node_function).parameters

    @property
    def inputs(self) -> Inputs:
        if self._inputs is None:
            self._inputs = Inputs(*self._build_input_channels())
        return self._inputs

    @property
    def outputs(self) -> Outputs:
        if self._outputs is None:
            self._outputs = Outputs(*self._build_output_channels(*self._output_labels))
        return self._outputs

    def _build_input_channels(self):
        channels = []
        type_hints = get_type_hints(self.node_function)

        for ii, (label, value) in enumerate(self._input_args.items()):
            is_self = False
            if label == "self":  # `self` is reserved for the node object
                if ii == 0:
                    is_self = True
                else:
                    warnings.warn(
                        "`self` is used as an argument but not in the first"
                        " position, so it is treated as a normal function"
                        " argument. If it is to be treated as the node object,"
                        " use it as a first argument"
                    )
            if label in self._init_keywords:
                # We allow users to parse arbitrary kwargs as channel initialization
                # So don't let them choose bad channel names
                raise ValueError(
                    f"The Input channel name {label} is not valid. Please choose a "
                    f"name _not_ among {self._init_keywords}"
                )

            try:
                type_hint = type_hints[label]
                if is_self:
                    warnings.warn("type hint for self ignored")
            except KeyError:
                type_hint = None

            default = NotData  # The standard default in DataChannel
            if value.default is not inspect.Parameter.empty:
                if is_self:
                    warnings.warn("default value for self ignored")
                else:
                    default = value.default

            if not is_self:
                channels.append(
                    InputData(
                        label=label,
                        node=self,
                        default=default,
                        type_hint=type_hint,
                    )
                )
        return channels

    @property
    def _init_keywords(self):
        return list(inspect.signature(self.__init__).parameters.keys())

    def _build_output_channels(self, *return_labels: str):
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
            channels.append(
                OutputData(
                    label=label,
                    node=self,
                    type_hint=hint,
                )
            )

        return channels

    def _verify_that_channels_requiring_update_all_exist(self):
        if not all(
            channel_name in self.inputs.labels
            for channel_name in self.channels_requiring_update_after_run
        ):
            raise ValueError(
                f"On or more channel name among those listed as requiring updates "
                f"after the node runs ({self.channels_requiring_update_after_run}) was "
                f"not found among the input channels ({self.inputs.labels})"
            )

    @property
    def on_run(self):
        return self.node_function

    @property
    def run_args(self) -> dict:
        kwargs = self.inputs.to_value_dict()
        if "self" in self._input_args:
            kwargs["self"] = self
        return kwargs

    def process_run_result(self, function_output):
        """
        Take the results of the node function, and use them to update the node output.

        By extracting this as a separate method, we allow the node to pass the actual
        execution off to another entity and release the python process to do other
        things. In such a case, this function should be registered as a callback
        so that the node can finishing "running" and push its data forward when that
        execution is finished.
        """
        for channel_name in self.channels_requiring_update_after_run:
            self.inputs[channel_name].wait_for_update()

        if len(self.outputs) == 1:
            function_output = (function_output,)

        for out, value in zip(self.outputs, function_output):
            out.update(value)

    def __call__(self) -> None:
        self.run()

    def to_dict(self):
        return {
            "label": self.label,
            "ready": self.ready,
            "connected": self.connected,
            "fully_connected": self.fully_connected,
            "inputs": self.inputs.to_dict(),
            "outputs": self.outputs.to_dict(),
            "signals": self.signals.to_dict(),
        }


class Slow(Function):
    """
    Like a regular node, but `run_on_updates` and `update_on_instantiation` default to
    `False`.
    This is intended for wrapping function which are potentially expensive to call,
    where you don't want the output recomputed unless `run()` is _explicitly_ called.
    """

    def __init__(
        self,
        node_function: callable,
        *output_labels: str,
        label: Optional[str] = None,
        run_on_updates=False,
        update_on_instantiation=False,
        parent: Optional[Workflow] = None,
        **kwargs,
    ):
        super().__init__(
            node_function,
            *output_labels,
            label=label,
            run_on_updates=run_on_updates,
            update_on_instantiation=update_on_instantiation,
            parent=parent,
            **kwargs,
        )


class SingleValue(Function, HasChannel):
    """
    A node that _must_ return only a single value.

    Attribute and item access is modified to finally attempt access on the output value.
    """

    def __init__(
        self,
        node_function: callable,
        *output_labels: str,
        label: Optional[str] = None,
        run_on_updates=True,
        update_on_instantiation=True,
        parent: Optional[Workflow] = None,
        **kwargs,
    ):
        self.ensure_there_is_only_one_return_value(output_labels)
        super().__init__(
            node_function,
            *output_labels,
            label=label,
            run_on_updates=run_on_updates,
            update_on_instantiation=update_on_instantiation,
            parent=parent,
            **kwargs,
        )

    @classmethod
    def ensure_there_is_only_one_return_value(cls, output_labels):
        if len(output_labels) > 1:
            raise ValueError(
                f"{cls.__name__} must only have a single return value, but got "
                f"multiple output labels: {output_labels}"
            )

    @property
    def single_value(self):
        return self.outputs[self.outputs.labels[0]].value

    @property
    def channel(self) -> OutputData:
        """The channel for the single output"""
        return list(self.outputs.channel_dict.values())[0]

    def __getitem__(self, item):
        return self.single_value.__getitem__(item)

    def __getattr__(self, item):
        return getattr(self.single_value, item)

    def __repr__(self):
        return self.single_value.__repr__()

    def __str__(self):
        return f"{self.label} ({self.__class__.__name__}) output single-value: " + str(
            self.single_value
        )


def function_node(*output_labels: str, **node_class_kwargs):
    """
    A decorator for dynamically creating node classes from functions.

    Decorates a function.
    Takes an output label for each returned value of the function.
    Returns a `Function` subclass whose name is the camel-case version of the function node,
    and whose signature is modified to exclude the node function and output labels
    (which are explicitly defined in the process of using the decorator).
    """

    def as_node(node_function: callable):
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (Function,),  # Define parentage
            {
                "__init__": partialmethod(
                    Function.__init__,
                    node_function,
                    *output_labels,
                    **node_class_kwargs,
                )
            },
        )

    return as_node


def slow_node(*output_labels: str, **node_class_kwargs):
    """
    A decorator for dynamically creating slow node classes from functions.

    Unlike normal nodes, slow nodes do update themselves on initialization and do not
    run themselves when they get updated -- i.e. they will not run when their input
    changes, `run()` must be explicitly called.
    """

    def as_slow_node(node_function: callable):
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (Slow,),  # Define parentage
            {
                "__init__": partialmethod(
                    Slow.__init__,
                    node_function,
                    *output_labels,
                    **node_class_kwargs,
                )
            },
        )

    return as_slow_node


def single_value_node(*output_labels: str, **node_class_kwargs):
    """
    A decorator for dynamically creating fast node classes from functions.

    Unlike normal nodes, fast nodes _must_ have default values set for all their inputs.
    """

    def as_single_value_node(node_function: callable):
        SingleValue.ensure_there_is_only_one_return_value(output_labels)
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (SingleValue,),  # Define parentage
            {
                "__init__": partialmethod(
                    SingleValue.__init__,
                    node_function,
                    *output_labels,
                    **node_class_kwargs,
                )
            },
        )

    return as_single_value_node
