from __future__ import annotations

import inspect
import warnings
from functools import partialmethod
from typing import Any, get_args, get_type_hints, Optional, TYPE_CHECKING

from pyiron_workflow.channels import InputData, OutputData, NotData
from pyiron_workflow.has_channel import HasChannel
from pyiron_workflow.io import Inputs, Outputs, Signals
from pyiron_workflow.node import Node
from pyiron_workflow.output_parser import ParseOutput
from pyiron_workflow.util import SeabornColors

if TYPE_CHECKING:
    from pyiron_workflow.composite import Composite
    from pyiron_workflow.workflow import Workflow


class Function(Node):
    """
    Function nodes wrap an arbitrary python function.

    Actual function node instances can either be instances of the base node class, in
    which case the callable node function *must* be provided OR they can be instances
    of children of this class which provide the node function as a class-level method.
    Those children may define some or all of the node behaviour at the class level, and
    modify their signature accordingly so this is not available for alteration by the
    user, e.g. the node function and output labels may be hard-wired.

    Although not strictly enforced, it is a best-practice that where possible, function
    nodes should be both functional (always returning the same output given the same
    input) and idempotent (not modifying input data in-place, but creating copies where
    necessary and returning new objects as output).
    Further, functions with multiple return branches that return different types or
    numbers of return values may or may not work smoothly, depending on the details.

    Promises:
    - IO channels are constructed automatically from the wrapped function
        - This includes type hints (if any)
        - This includes defaults (if any)
        - By default one channel is created for each returned value (from a tuple)...
        - Output channel labels are taken from the returned value, but may be overriden
        - A single tuple output channel can be forced by manually providing exactly one
            output label
    - Running the node executes the wrapped function and returns its result
    - Input updates can be made with `*args` as well as the usual `**kwargs`, following
        the same input order as the wrapped function.
    - A default label can be scraped from the name of the wrapped function

    Args:
        node_function (callable): The function determining the behaviour of the node.
        label (str): The node's label. (Defaults to the node function's name.)
        output_labels (Optional[str | list[str] | tuple[str]]): A name for each return
            value of the node function OR a single label. (Default is None, which
            scrapes output labels automatically from the source code of the wrapped
            function.) This can be useful when returned values are not well named, e.g.
            to make the output channel dot-accessible if it would otherwise have a label
            that requires item-string-based access. Additionally, specifying a _single_
            label for a wrapped function that returns a tuple of values ensures that a
            _single_ output channel (holding the tuple) is created, instead of one
            channel for each return value. The default approach of extracting labels
            from the function source code also requires that the function body contain
            _at most_ one `return` expression, so providing explicit labels can be used
            to circumvent this (at your own risk).
        **kwargs: Any additional keyword arguments whose keyword matches the label of an
            input channel will have their value assigned to that channel.

    Examples:
        At the most basic level, to use nodes all we need to do is provide the
        `Function` class with a function and labels for its output, like so:
        >>> from pyiron_workflow.function import Function
        >>>
        >>> def mwe(x, y):
        ...     return x+1, y-1
        >>>
        >>> plus_minus_1 = Function(mwe)
        >>>
        >>> print(plus_minus_1.outputs["x+1"])
        <class 'pyiron_workflow.channels.NotData'>

        There is no output because we haven't given our function any input, it has
        no defaults, and we never ran it! So outputs have the channel default value of
        `NotData` -- a special non-data class (since `None` is sometimes a meaningful
        value in python).

        We'll run into a hiccup if we try to set only one of the inputs and force the
        run:
        >>> plus_minus_1.inputs.x = 2
        >>> plus_minus_1.run()
        ValueError: mwe received a run command but is not ready. The node should be
        neither running nor failed, and all input values should conform to type hints:
        running: False
        failed: False
        x ready: True
        y ready: False

        This is because the second input (`y`) still has no input value -- indicated in
        the error message -- so we can't do the sum between `NotData` and `2`.

        Once we update `y`, all the input is ready we will be allowed to proceed to a
        `run()` call, which succeeds and updates the output.
        The final thing we need to do is disable the `failed` status we got from our
        last run call
        >>> plus_minus_1.failed = False
        >>> plus_minus_1.inputs.y = 3
        >>> plus_minus_1.run()
        >>> plus_minus_1.outputs.to_value_dict()
        {'x+1': 3, 'y-1': 2}

        We can also, optionally, provide initial values for some or all of the input and
        labels for the output:
        >>> plus_minus_1 = Function(mwe, output_labels=("p1", "m1"),  x=1)
        >>> plus_minus_1.inputs.y = 2
        >>> out = plus_minus_1.run()
        >>> out
        (2, 1)

        Input data can be provided to both initialization and on call as ordered args
        or keyword kwargs.
        When running the node (or any alias to run like pull, execute, or just calling
        the node), the output of the wrapped function is returned:
        >>> plus_minus_1(2, y=3)
        (3, 2)

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
        Note that getting "good" (i.e. dot-accessible) output labels can be achieved by
        using good variable names and returning those variables instead of using
        `output_labels`.
        If we try to assign a value of the wrong type, it will raise an error:
        >>> from typing import Union
        >>>
        >>> def hinted_example(
        ...     x: Union[int, float],
        ...     y: int | float = 1
        ... ) -> tuple[int, int | float]:
        ...     p1, m1 = x+1, y-1
        ...     return p1, m1
        >>>
        >>> plus_minus_1 = Function(hinted_example)
        >>> plus_minus_1.inputs.x =  "not an int or float"
        TypeError: The channel x cannot take the value `not an int or float` because it
        is not compliant with the type hint typing.Union[int, float]

        We can turn off type hinting with the `strict_hints` boolean property, or just
        circumvent the type hinting by applying the new data directly to the private
        `_value` property.
        In the latter case, we'd still get a readiness error when we try to run and
        the ready check sees that the data doesn't conform to the type hint:
        >>> plus_minus_1.inputs.x._value =  "not an int or float"
        >>> plus_minus_1.run()
        ValueError: hinted_example received a run command but is not ready. The node
        should be neither running nor failed, and all input values should conform to
        type hints:
        running: False
        failed: False
        x ready: False
        y ready: True

        Here, even though all the input has data, the node sees that some of it is the
        wrong type and so (by default) the run raises an error right away.
        This causes the failure to come earlier because we stop the node from running
        and throwing an error because it sees that the channel (and thus node) is not
        ready:

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
        >>> from pyiron_workflow.function import function_node
        >>>
        >>> @function_node(output_labels=("p1", "m1"))
        ... def my_mwe_node(
        ...     x: int | float, y: int | float = 1
        ... ) -> tuple[int | float, int | float]:
        ...     return x+1, y-1
        >>>
        >>> node_instance = my_mwe_node(x=0)
        >>> node_instance(y=0)
        (1, -1)

        Where we've passed the output labels and class arguments to the decorator,
        and inital values to the newly-created node class (`my_mwe_node`) at
        instantiation.
        Because we provided a good initial value for `x`, we get our result right away.

        Using the decorator is the recommended way to create new node classes, but this
        magic is just equivalent to creating a child class with the `node_function`
        already defined as a `staticmethod`:
        >>> from typing import Literal, Optional
        >>>
        >>> class AlphabetModThree(Function):
        ...     def __init__(
        ...         self,
        ...         label: Optional[str] = None,
        ...         **kwargs
        ...     ):
        ...         super().__init__(
        ...             self.alphabet_mod_three,
        ...             label=label,
        ...             **kwargs
        ...         )
        ...
        ...     @staticmethod
        ...     def alphabet_mod_three(i: int) -> Literal["a", "b", "c"]:
        ...         letter = ["a", "b", "c"][i % 3]
        ...         return letter

        Finally, let's put it all together by using both of these nodes at once.
        Instead of setting input to a particular data value, we'll set it to
        be another node's output channel, thus forming a connection.
        At the end of the day, the graph will also need to know about the execution
        flow, but in most cases (directed acyclic graphs -- DAGs), this can be worked
        out automatically by the topology of data connections.
        Let's put together a couple of nodes and then run in a "pull" paradigm to get
        the final node to run everything "upstream" then run itself:
        >>> @function_node()
        ... def adder_node(x: int = 0, y: int = 0) -> int:
        ...     sum = x + y
        ...     return sum
        >>>
        >>> adder = adder_node(x=1)
        >>> alpha = AlphabetModThree(i=adder.outputs.sum)
        >>> print(alpha())
        "b"
        >>> adder.inputs.y = 1
        >>> print(alpha())
        "c"
        >>> adder.inputs.x = 0
        >>> adder.inputs.y = 0
        >>> print(alpha())
        "a"

        Alternatively, execution flows can be specified manualy by connecting
        `.signals.input.run` and `.signals.output.ran` channels, either by their
        `.connect` method or by assignment (both cases just like data chanels), or by
        some syntactic sugar using the `>` operator.
        Then we can use a "push" paradigm with the `run` command to force execution
        forwards through the graph to get an end result.
        This is a bit more verbose, but a necessary tool for more complex situations
        (like cyclic graphs).
        Here's our simple example from above using this other paradigm:
        >>> @function_node()
        ... def adder_node(x: int = 0, y: int = 0) -> int:
        ...     sum = x + y
        ...     return sum
        >>>
        >>> adder = adder_node()
        >>> alpha = AlphabetModThree(i=adder.outputs.sum)
        >>> adder > alpha
        >>>
        >>> adder.run(x=1)
        >>> print(alpha.outputs.letter)
        "b"
        >>> adder.run(y=1)
        >>> print(alpha.outputs.letter)
        "c"
        >>> adder.inputs.x = 0
        >>> adder.inputs.y = 0
        >>> adder.run()
        >>> print(alpha.outputs.letter)
        "a"

        To see more details on how to use many nodes together, look at the
        `Workflow` class.

    Comments:
        Using the `self` argument for function nodes is not fully supported; it will
        raise an error when combined with an executor, and otherwise behaviour is not
        guaranteed.
    """

    def __init__(
        self,
        node_function: callable,
        *args,
        label: Optional[str] = None,
        parent: Optional[Composite] = None,
        output_labels: Optional[str | list[str] | tuple[str]] = None,
        **kwargs,
    ):
        if not callable(node_function):
            # Children of `Function` may explicitly provide a `node_function` static
            # method so the node has fixed behaviour.
            # In this case, the `__init__` signature should be changed so that the
            # `node_function` argument is just always `None` or some other non-callable.
            # If a callable `node_function` is not received, you'd better have it as an
            # attribute already!
            if not hasattr(self, "node_function"):
                raise AttributeError(
                    f"If `None` is provided as a `node_function`, a `node_function` "
                    f"property must be defined instead, e.g. when making child classes"
                    f"of `Function` with specific behaviour"
                )
        else:
            # If a callable node function is received, use it
            self.node_function = node_function
            self._type_hints = get_type_hints(node_function)

        super().__init__(
            label=label if label is not None else self.node_function.__name__,
            parent=parent,
            # **kwargs,
        )

        self._inputs = None
        self._outputs = None
        self._output_labels = self._get_output_labels(output_labels)
        # TODO: Parse output labels from the node function in case output_labels is None

        self.signals = self._build_signal_channels()
        self.set_input_values(*args, **kwargs)

    def _get_output_labels(self, output_labels: str | list[str] | tuple[str] | None):
        """
        If output labels are provided, turn convert them to a list if passed as a
        string and return them, else scrape them from the source channel.

        Note: When the user explicitly provides output channels, they are taking
        responsibility that these are correct, e.g. in terms of quantity, order, etc.
        """
        if output_labels is None:
            return self._scrape_output_labels()
        elif isinstance(output_labels, str):
            return [output_labels]
        else:
            return output_labels

    def _scrape_output_labels(self):
        """
        Inspect the source code to scrape out strings representing the returned values.
        _Only_ works for functions with a single `return` expression in their body.

        Will return expressions and function calls just fine, thus best practice is to
        create well-named variables and return those so that the output labels stay
        dot-accessible.
        """
        parsed_outputs = ParseOutput(self.node_function).output
        return [] if parsed_outputs is None else parsed_outputs

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
        type_hints = self._type_hints

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
            type_hints = self._type_hints["return"]
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

    @property
    def on_run(self):
        return self.node_function

    @property
    def run_args(self) -> dict:
        kwargs = self.inputs.to_value_dict()
        if "self" in self._input_args:
            if self.executor:
                raise ValueError(
                    f"Function node {self.label} uses the `self` argument, but this "
                    f"can't yet be run with executors"
                )
            kwargs["self"] = self
        return kwargs

    def process_run_result(self, function_output: Any | tuple) -> Any | tuple:
        """
        Take the results of the node function, and use them to update the node output.
        """
        for out, value in zip(
            self.outputs,
            (function_output,) if len(self.outputs) == 1 else function_output,
        ):
            out.value = value
        return function_output

    def _convert_input_args_and_kwargs_to_input_kwargs(self, *args, **kwargs):
        reverse_keys = list(self._input_args.keys())[::-1]
        if len(args) > len(reverse_keys):
            raise ValueError(
                f"Received {len(args)} positional arguments, but the node {self.label}"
                f"only accepts {len(reverse_keys)} inputs."
            )

        positional_keywords = reverse_keys[-len(args) :] if len(args) > 0 else []  # -0:
        if len(set(positional_keywords).intersection(kwargs.keys())) > 0:
            raise ValueError(
                f"Cannot use {set(positional_keywords).intersection(kwargs.keys())} "
                f"as both positional _and_ keyword arguments; args {args}, kwargs "
                f"{kwargs}, reverse_keys {reverse_keys}, positional_keyworkds "
                f"{positional_keywords}"
            )

        for arg in args:
            key = positional_keywords.pop()
            kwargs[key] = arg

        return kwargs

    def set_input_values(self, *args, **kwargs) -> None:
        """
        Match positional and keyword arguments to input channels and update input
        values.

        Args:
            *args: Interpreted in the same order as node function arguments.
            **kwargs: input label - input value (including channels for connection)
             pairs.
        """
        kwargs = self._convert_input_args_and_kwargs_to_input_kwargs(*args, **kwargs)
        return super().set_input_values(**kwargs)

    def execute(self, *args, **kwargs):
        kwargs = self._convert_input_args_and_kwargs_to_input_kwargs(*args, **kwargs)
        return super().execute(**kwargs)

    def pull(self, *args, run_parent_trees_too=False, **kwargs):
        kwargs = self._convert_input_args_and_kwargs_to_input_kwargs(*args, **kwargs)
        return super().pull(run_parent_trees_too=run_parent_trees_too, **kwargs)

    def __call__(self, *args, **kwargs) -> None:
        kwargs = self._convert_input_args_and_kwargs_to_input_kwargs(*args, **kwargs)
        return super().__call__(**kwargs)

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

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.green


class SingleValue(Function, HasChannel):
    """
    A node that _must_ return only a single value.

    Attribute and item access is modified to finally attempt access on the output value.
    Note that this means any attributes/method available on the output value become
    available directly at the node level (at least those which don't conflict with the
    existing node namespace).

    Promises (in addition parent class promises):
    - Attribute and item access will finally attempt to access the output value
    - The entire node can be used in place of its output value for connections, e.g.
        `some_node.input.some_channel = my_svn_instance`.
    """

    def __init__(
        self,
        node_function: callable,
        *args,
        label: Optional[str] = None,
        parent: Optional[Workflow] = None,
        output_labels: Optional[str | list[str] | tuple[str]] = None,
        **kwargs,
    ):
        super().__init__(
            node_function,
            *args,
            label=label,
            parent=parent,
            output_labels=output_labels,
            **kwargs,
        )

    def _get_output_labels(self, output_labels: str | list[str] | tuple[str] | None):
        output_labels = super()._get_output_labels(output_labels)
        if len(output_labels) > 1:
            raise ValueError(
                f"{self.__class__.__name__} must only have a single return value, but "
                f"got multiple output labels: {output_labels}"
            )
        return output_labels

    @property
    def single_value(self):
        return self.outputs[self.outputs.labels[0]].value

    @property
    def channel(self) -> OutputData:
        """The channel for the single output"""
        return list(self.outputs.channel_dict.values())[0]

    @property
    def color(self) -> str:
        """For drawing the graph"""
        return SeabornColors.cyan

    def __getitem__(self, item):
        return self.single_value.__getitem__(item)

    def __getattr__(self, item):
        try:
            return getattr(self.single_value, item)
        except Exception as e:
            raise AttributeError(
                f"Could not find {item} as an attribute of the single value "
                f"{self.single_value}"
            ) from e

    def __repr__(self):
        return self.single_value.__repr__()

    def __str__(self):
        return f"{self.label} ({self.__class__.__name__}) output single-value: " + str(
            self.single_value
        )


def _wrapper_factory(
    parent_class: type[Function], output_labels: Optional[list[str]]
) -> callable:
    """
    An abstract base for making decorators that wrap a function as `Function` or its
    children.
    """

    # One really subtle thing is that we manually parse the function type hints right
    # here and include these as a class-level attribute.
    # This is because on (de)(cloud)pickling a function node, somehow the node function
    # method attached to it gets its `__globals__` attribute changed; it retains stuff
    # _inside_ the function, but loses imports it used from the _outside_ -- i.e. type
    # hints! I (@liamhuber) don't deeply understand _why_ (de)pickling is modifying the
    # __globals__ in this way, but the result is that type hints cannot be parsed after
    # the change.
    # The final piece of the puzzle here is that because the node function is a _class_
    # level attribute, if you (de)pickle a node, _new_ instances of that node wind up
    # having their node function's `__globals__` trimmed down in this way!
    # So to keep the type hint parsing working, we snag and interpret all the type hints
    # at wrapping time, when we are guaranteed to have all the globals available, and
    # also slap them on as a class-level attribute. These get safely packed and returned
    # when (de)pickling so we can keep processing type hints without trouble.
    def as_node(node_function: callable):
        return type(
            node_function.__name__.title().replace("_", ""),  # fnc_name to CamelCase
            (parent_class,),  # Define parentage
            {
                "__init__": partialmethod(
                    parent_class.__init__,
                    None,
                    output_labels=output_labels,
                ),
                "node_function": staticmethod(node_function),
                "_type_hints": get_type_hints(node_function),
            },
        )

    return as_node


def function_node(output_labels=None):
    """
    A decorator for dynamically creating node classes from functions.

    Decorates a function.
    Returns a `Function` subclass whose name is the camel-case version of the function
    node, and whose signature is modified to exclude the node function and output labels
    (which are explicitly defined in the process of using the decorator).

    Optionally takes any keyword arguments of `Function`.
    """
    return _wrapper_factory(parent_class=Function, output_labels=output_labels)


def single_value_node(output_labels=None):
    """
    A decorator for dynamically creating fast node classes from functions.

    Unlike normal nodes, fast nodes _must_ have default values set for all their inputs.

    Optionally takes any keyword arguments of `SingleValueNode`.
    """
    return _wrapper_factory(parent_class=SingleValue, output_labels=output_labels)
