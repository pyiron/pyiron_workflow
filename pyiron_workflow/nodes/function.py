from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyiron_snippets.colors import SeabornColors
from pyiron_snippets.factory import classfactory

from pyiron_workflow.mixin.preview import ScrapesIO
from pyiron_workflow.nodes.static_io import StaticNode


class Function(StaticNode, ScrapesIO, ABC):
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

    Examples:
        At the most basic level, to use nodes all we need to do is provide the
        `Function` class with a function and labels for its output, like so:

        >>> from pyiron_workflow import function_node
        >>>
        >>> def mwe(x, y):
        ...     return x+1, y-1
        >>>
        >>> plus_minus_1 = function_node(mwe)
        >>>
        >>> print(plus_minus_1.outputs["x+1"])
        NOT_DATA

        There is no output because we haven't given our function any input, it has
        no defaults, and we never ran it! So outputs have the channel default value of
        `NOT_DATA` -- a special non-data singleton (since `None` is sometimes a
        meaningful value in python).

        We'll run into a hiccup if we try to set only one of the inputs and force the
        run:

        >>> plus_minus_1.inputs.x = 2
        >>> try:
        ...     plus_minus_1.run()
        ... except ValueError as e:
        ...     print("ValueError:", e.args[0])
        ValueError: mwe received a run command but is not ready. The node should be neither running nor failed, and all input values should conform to type hints.
        mwe readiness: False
        STATE:
        running: False
        failed: False
        INPUTS:
        x ready: True
        y ready: False

        We are able to check this without trying and failing by looking at the
        readiness report:

        >>> print(plus_minus_1.readiness_report)
        mwe readiness: False
        STATE:
        running: False
        failed: False
        INPUTS:
        x ready: True
        y ready: False

        This is because the second input (`y`) still has no input value -- indicated in
        the error message -- so we can't do the sum between `NOT_DATA` and `2`.

        Once we update `y`, all the input is ready we will be allowed to proceed to a
        `run()` call, which succeeds and updates the output.
        The final thing we need to do is disable the `failed` status we got from our
        last run call

        >>> plus_minus_1.failed = False
        >>> plus_minus_1.inputs.y = 3
        >>> out = plus_minus_1.run()
        >>> plus_minus_1.outputs.to_value_dict()
        {'x+1': 3, 'y-1': 2}

        We can also, optionally, provide initial values for some or all of the input
        and labels for the output:

        >>> plus_minus_1 = function_node(mwe, output_labels=("p1", "m1"),  x=1)
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
        hints (and, optionally, default values) when defining the function that the
        node wraps.
        The node will automatically figure out defaults and type hints for the IO
        channels from inspection of the wrapped function.

        In this example, note the mixture of old-school (`typing.Union`) and new (`|`)
        type hints as well as nested hinting with a union-type inside the tuple for the
        return hint.
        Our treatment of type hints is **not infinitely robust**, but covers a wide
        variety of common use cases.
        Note that getting "good" (i.e. dot-accessible) output labels can be achieved by
        using good variable names and returning those variables instead of using
        :param:`output_labels`.
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
        >>> plus_minus_1 = function_node(hinted_example)
        >>> try:
        ...     plus_minus_1.inputs.x =  "not an int or float"
        ... except TypeError as e:
        ...     print("TypeError:", e.args[0])
        TypeError: The channel /hinted_example.x cannot take the value `not an int or float` (<class 'str'>) because it is not compliant with the type hint typing.Union[int, float]

        We can turn off type hinting with the `strict_hints` boolean property, or just
        circumvent the type hinting by applying the new data directly to the private
        `_value` property.
        In the latter case, we'd still get a readiness error when we try to run and
        the ready check sees that the data doesn't conform to the type hint:

        >>> plus_minus_1.inputs.x._value =  "not an int or float"
        >>> try:
        ...     plus_minus_1.run()
        ... except ValueError as e:
        ...     print("ValueError:", e.args[0])
        ValueError: hinted_example received a run command but is not ready. The node should be neither running nor failed, and all input values should conform to type hints.
        hinted_example readiness: False
        STATE:
        running: False
        failed: False
        INPUTS:
        x ready: False
        y ready: True

        Here, even though all the input has data, the node sees that some of it is the
        wrong type and so (by default) the run raises an error right away.
        This causes the failure to come earlier because we stop the node from running
        and throwing an error because it sees that the channel (and thus node) is not
        ready:

        >>> plus_minus_1.ready, plus_minus_1.inputs.x.ready, plus_minus_1.inputs.y.ready
        (False, False, True)

        In these examples, we've instantiated nodes directly from the base
        :class:`Function` class, and populated their input directly with data.
        In practice, these nodes are meant to be part of complex workflows; that means
        both that you are likely to have particular nodes that get heavily re-used, and
        that you need the nodes to pass data to each other.

        For reusable nodes, we want to create a sub-class of :class:`Function`
        that fixes some of the node behaviour -- i.e. the :meth:`node_function`.

        This can be done most easily with the :func:`as_function_node` decorator, which
        takes a function and returns a node class. It also allows us to provide labels
        for the return values, :param:output_labels, which are otherwise scraped from
        the text of the function definition:

        >>> from pyiron_workflow import as_function_node
        >>>
        >>> @as_function_node("p1", "m1")
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
        >>> from pyiron_workflow import Function
        >>>
        >>> class AlphabetModThree(Function):
        ...
        ...     @staticmethod
        ...     def node_function(i: int) -> Literal["a", "b", "c"]:
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

        >>> @as_function_node()
        ... def adder_node(x: int = 0, y: int = 0) -> int:
        ...     sum = x + y
        ...     return sum
        >>>
        >>> adder = adder_node(x=1)
        >>> alpha = AlphabetModThree(i=adder.outputs.sum)
        >>> print(alpha())
        b
        >>> adder.inputs.y = 1
        >>> print(alpha())
        c
        >>> adder.inputs.x = 0
        >>> adder.inputs.y = 0
        >>> print(alpha())
        a

        Alternatively, execution flows can be specified manualy by connecting
        `.signals.input.run` and `.signals.output.ran` channels, either by their
        `.connect` method or by assignment (both cases just like data chanels), or by
        some syntactic sugar using the `>` operator.
        Then we can use a "push" paradigm with the `run` command to force execution
        forwards through the graph to get an end result.
        This is a bit more verbose, but a necessary tool for more complex situations
        (like cyclic graphs).
        Here's our simple example from above using this other paradigm:

        >>> @as_function_node()
        ... def adder_node(x: int = 0, y: int = 0) -> int:
        ...     sum = x + y
        ...     return sum
        >>>
        >>> adder = adder_node()
        >>> alpha = AlphabetModThree(i=adder.outputs.sum)
        >>> _ = adder >> alpha
        >>> # We catch and ignore output -- it's needed for chaining, but screws up
        >>> # doctests -- you don't normally need to catch it like this!
        >>> out = adder.run(x=1)
        >>> print(alpha.outputs.letter)
        b
        >>> out = adder.run(y=1)
        >>> print(alpha.outputs.letter)
        c
        >>> adder.inputs.x = 0
        >>> adder.inputs.y = 0
        >>> out = adder.run()
        >>> print(alpha.outputs.letter)
        a

        To see more details on how to use many nodes together, look at the
        :class:`Workflow` class.

    Comments:
        Using the `self` argument for function nodes is not fully supported; it will
        raise an error when combined with an executor, and otherwise behaviour is not
        guaranteed.
    """

    @staticmethod
    @abstractmethod
    def node_function(**kwargs) -> callable:
        """What the node _does_."""

    @classmethod
    def _io_defining_function(cls) -> callable:
        return cls.node_function

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        preview = super(Function, cls)._build_outputs_preview()
        return preview if len(preview) > 0 else {"None": type(None)}
        # If clause facilitates functions with no return value

    def on_run(self, **kwargs):
        return self.node_function(**kwargs)

    @property
    def run_args(self) -> tuple[tuple, dict]:
        kwargs = self.inputs.to_value_dict()
        return (), kwargs

    def process_run_result(self, function_output: Any | tuple) -> Any | tuple:
        """
        Take the results of the node function, and use them to update the node output.
        """
        for out, value in zip(
            self.outputs,
            (function_output,) if len(self.outputs) == 1 else function_output,
        ):
            out.value = value
        return self._outputs_to_run_return()

    def _outputs_to_run_return(self):
        output = tuple(self.outputs.to_value_dict().values())
        if len(output) == 1:
            output = output[0]
        return output

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


@classfactory
def function_node_factory(
    node_function: callable,
    validate_output_labels: bool,
    use_cache: bool = True,
    /,
    *output_labels,
):
    """
    Create a new :class:`Function` node class based on the given node function. This
    function gets executed on each :meth:`run` of the resulting function.

    Args:
        node_function (callable): The function to be wrapped by the node.
        validate_output_labels (bool): Flag to indicate if output labels should be
            validated.
        use_cache (bool): Whether nodes of this type should default to caching their
            values.
        *output_labels: Optional labels for the function's output channels.

    Returns:
        type[Node]: A new node class.
    """
    return (
        node_function.__name__,
        (Function,),  # Define parentage
        {
            "node_function": staticmethod(node_function),
            "__module__": node_function.__module__,
            "__qualname__": node_function.__qualname__,
            "_output_labels": None if len(output_labels) == 0 else output_labels,
            "_validate_output_labels": validate_output_labels,
            "__doc__": node_function.__doc__,
            "use_cache": use_cache,
        },
        {},
    )


def as_function_node(
    *output_labels: str,
    validate_output_labels=True,
    use_cache=True,
):
    """
    Decorator to create a new :class:`Function` node class from a given function. This
    function gets executed on each :meth:`run` of the resulting function.

    Args:
        *output_labels (str): Optional labels for the function's output channels.
        validate_output_labels (bool): Flag to indicate if output labels should be
            validated against the return values in the function node source code.
            Defaults to True.
        use_cache (bool): Whether nodes of this type should default to caching their
            values. (Default is True.)

    Returns:
        Callable: A decorator that converts a function into a :class:`Function` node
            subclass.
    """

    def decorator(node_function):
        function_node_factory.clear(node_function.__name__)  # Force a fresh class
        factory_made = function_node_factory(
            node_function, validate_output_labels, use_cache, *output_labels
        )
        factory_made._class_returns_from_decorated_function = node_function
        factory_made.preview_io()
        return factory_made

    return decorator


def function_node(
    node_function: callable,
    *node_args,
    output_labels: str | tuple[str, ...] | None = None,
    validate_output_labels: bool = True,
    use_cache: bool = True,
    **node_kwargs,
):
    """
    Create and initialize a new instance of a :class:`Function` node.

    Args:
        node_function (callable): The function to be wrapped by the node.
        *node_args: Positional arguments for the :class:`Function` initialization --
            parsed as node input data.
        output_labels (str | tuple | Noen): Labels for the function's output
            channels. Defaults to None, which tries to parse these from the return
            statement.
        validate_output_labels (bool): Flag to indicate if output labels should be
            validated against the return values in the function source code. Defaults
            to True. Disabling this may be useful if the source code is not available
            or if the function has multiple return statements.
        use_cache (bool): Whether this node should default to caching its values.
            (Default is True.)
        **node_kwargs: Keyword arguments for the :class:`Function` initialization --
            parsed as node input data when the keyword matches an input channel.

    Returns:
        Function: An instance of the generated :class:`Function` node subclass.
    """
    if output_labels is None:
        output_labels = ()
    elif isinstance(output_labels, str):
        output_labels = (output_labels,)
    function_node_factory.clear(node_function.__name__)  # Force a fresh class
    factory_made = function_node_factory(
        node_function, validate_output_labels, use_cache, *output_labels
    )
    factory_made.preview_io()
    return factory_made(*node_args, **node_kwargs)
