import abc
from typing import Any, ClassVar, TypeAlias, TypeGuard, cast

from pyiron_snippets import factory

from pyiron_workflow.mixin.run import InterpretableAsExecutor
from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.nodes.static_io import StaticNode
from pyiron_workflow.storage import BackendIdentifier, StorageInterface

label_connection: TypeAlias = tuple[str, str]
label_connections: TypeAlias = tuple[label_connection, ...]
label_connections_like: TypeAlias = (
    label_connection | list[label_connection] | label_connections
)


class InvalidTestOutputError(ValueError):
    """When the test type doesn't have the right output type."""


class InvalidEdgeError(ValueError):
    """When a requested edge does not exist for the provided node types."""


class NonTerminatingLoopError(ValueError):
    """When it is strictly impossible for the loop to terminate due to a lack of recursion"""


class NotConnectionslikeError(TypeError):
    """A type checking problem."""


def _is_label_connection(c: object) -> TypeGuard[tuple[str, str]]:
    return (
        isinstance(c, tuple)
        and len(c) == 2
        and isinstance(c[0], str)
        and isinstance(c[1], str)
    )


def _tuplefy(connections: label_connections_like) -> label_connections:
    if _is_label_connection(connections):
        return (connections,)
    elif isinstance(connections, tuple) and all(
        _is_label_connection(c) for c in connections
    ):
        return cast(label_connections, connections)
    elif isinstance(connections, list) and all(
        _is_label_connection(c) for c in connections
    ):
        return tuple(connections)
    raise NotConnectionslikeError(
        f"Expected connections in the form {label_connections_like}, "
        f"but got {connections}."
    )


class While(Composite, StaticNode, abc.ABC):
    """
    A second-order, stateful, dynamic node that resets its graph at each (non-cached)
    run to dynamically add copies of a body and test case node for as long as the test
    case node is outputting a true result.

    Subclasses must define the types of nodes to use for the test and body, as well
    as at least one edge from the body to the test and from the body back to itself.

    Instances can pass executor information to all child nodes of the same type using
    the :attr:`executor_for_test` and :attr:`executor_for_body` attributes.
    """

    _body_node_class: ClassVar[type[StaticNode]]
    _test_node_class: ClassVar[type[StaticNode]]
    _body_to_test_connections: ClassVar[label_connections]
    _body_to_body_connections: ClassVar[label_connections]
    _test_stem: ClassVar[str] = "test_"
    _body_stem: ClassVar[str] = "body_"

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        preview = {}
        for label, (hint, default) in cls._test_node_class.preview_inputs().items():
            preview[cls._test_stem + label] = (hint, default)
        for label, (hint, default) in cls._body_node_class.preview_inputs().items():
            preview[cls._body_stem + label] = (hint, default)
        preview["max_iterations"] = (int | None, None)
        return preview

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        return dict(cls._body_node_class.preview_outputs())

    def __init__(
        self,
        *args,
        label: str | None = None,
        parent: Composite | None = None,
        autoload: BackendIdentifier | StorageInterface | None = None,
        delete_existing_savefiles: bool = False,
        autorun: bool = False,
        checkpoint: BackendIdentifier | StorageInterface | None = None,
        strict_naming: bool = True,
        executor_for_test: InterpretableAsExecutor | None = None,
        executor_for_body: InterpretableAsExecutor | None = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            label=label,
            parent=parent,
            delete_existing_savefiles=delete_existing_savefiles,
            autorun=autorun,
            autoload=autoload,
            checkpoint=checkpoint,
            strict_naming=strict_naming,
            **kwargs,
        )
        self.executor_for_test = executor_for_test
        self.executor_for_body = executor_for_body

    def _on_cache_miss(self) -> None:
        super()._on_cache_miss()
        if self.ready:
            self._clean_existing_subgraph()

    def _clean_existing_subgraph(self):
        for label in self.child_labels:
            self.remove_child(label)

    def _on_run(self):
        n = 0
        test, body = self._extend_children(n)
        last_body = body  # In case of early termination, we need to link the output
        super()._on_run()
        while self._test_condition(test) and (
            self.inputs.max_iterations.value is None
            or n < self.inputs.max_iterations.value
        ):
            test >> body  # For posterity -- we manage the execution manually here
            self.starting_nodes = [body]
            super()._on_run()
            last_body = body
            n += 1
            test, body = self._extend_children(n)
            last_body >> test
            self._connect_cycles(last_body, test, body)
            super()._on_run()

        self._link_output_values(last_body)

        # Adding and removing children resets the cache, so make sure we cache _after_
        # we're done modifying the child graph
        self._cache_inputs()

        return self

    def _extend_children(self, n: int):
        test = self._test_node_class(label=f"{self._test_stem}{n}", parent=self)
        test.executor = self.executor_for_test
        body = self._body_node_class(label=f"{self._body_stem}{n}", parent=self)
        body.executor = self.executor_for_body
        self._link_input_values(test, body)
        self.starting_nodes = [test]
        return test, body

    def _link_input_values(self, test, body):
        for label, macro_input in self.inputs.items():
            if label.startswith(self._test_stem):
                macro_input.value_receiver = test.inputs[label[len(self._test_stem) :]]
            elif label.startswith(self._body_stem):
                macro_input.value_receiver = body.inputs[label[len(self._body_stem) :]]

    def _link_output_values(self, body):
        for label, body_output in body.outputs.items():
            body_output.value_receiver = self.outputs[label]

    def _connect_cycles(self, last_body, test, body):
        for target, connections in (
            (test, self._body_to_test_connections),
            (body, self._body_to_body_connections),
        ):
            for old_body_label, target_label in connections:
                last_body.outputs[old_body_label].connect(target.inputs[target_label])

    def _test_condition(self, test) -> bool:
        output_label = test.outputs.labels[0]
        return test.outputs.channel_dict[output_label].value


def _while_node_class_name(
    test_node_class: type[StaticNode],
    body_node_class: type[StaticNode],
    body_to_test_connections: label_connections,
    body_to_body_connections: label_connections,
) -> str:
    return (
        f"Do_{body_node_class.__name__}_While_{test_node_class.__name__}_With"
        f"_B2T_{'_'.join(con[0] + '2' + con[1] for con in body_to_test_connections)}"
        f"_B2B_{'_'.join(con[0] + '2' + con[1] for con in body_to_body_connections)}"
    )


@factory.classfactory
def while_node_factory(
    test_node_class: type[StaticNode],
    body_node_class: type[StaticNode],
    body_to_test_connections: label_connections,
    body_to_body_connections: label_connections,
    use_cache: bool = True,
    strict_condition_hint: bool = True,
    /,
) -> type[While]:
    """
    A factory method for creating a new :class:`While` node class.

    Args:
        test_node_class (type[StaticNode]): The class to use for the test condition.
            Must have a single output channel, and this must be hinted as boolean when
            :arg:`strict_condition_hint = True`.
        body_node_class (type[StaticNode]): The class to use for the body of the loop.
        body_to_test_connections (label_connections): Stringy representations of edge(s) from the body to the test instances.
        body_to_body_connections (label_connections): Stringy representations of edge(s) from the last-body to the next-body instances.
        use_cache (bool): Default class behavior for whether to skip running this node when its inputs match cached values. (Default is True, try to exploit caching.)
        strict_condition_hint (bool): Whether to demand that the :arg:`test_node_class` has its single output hinted as boolean. (Default is True.)

    Returns:
        type[While]: A new :class:`While` node class.
    """
    _verify_test_output(test_node_class, strict_condition_hint=strict_condition_hint)
    if len(body_to_test_connections) == 0:
        raise NonTerminatingLoopError(
            f"While-node received no connections from body ({body_node_class.__name__}) to test ({test_node_class.__name__}), and thus cannot transition from a non-terminating to terminating state. Please provide edges."
        )
    if len(body_to_body_connections) == 0:
        raise NonTerminatingLoopError(
            f"While-node received no connections from body ({body_node_class.__name__}) back to itself, and thus cannot transition from a non-terminating to terminating state. Please provide edges."
        )
    _verify_edges_exist(body_node_class, test_node_class, body_to_test_connections)
    _verify_edges_exist(body_node_class, body_node_class, body_to_body_connections)

    combined_docstring = (
        "While node docstring:\n"
        + (While.__doc__ if While.__doc__ is not None else "")
        + "\nBody node docstring:\n"
        + (body_node_class.__doc__ if body_node_class.__doc__ is not None else "")
        + "\nTest node docstring:\n"
        + (test_node_class.__doc__ if test_node_class.__doc__ is not None else "")
    )
    return (  # type: ignore[return-value]
        _while_node_class_name(
            test_node_class,
            body_node_class,
            body_to_test_connections,
            body_to_body_connections,
        ),
        (While,),
        {
            "_test_node_class": test_node_class,
            "_body_node_class": body_node_class,
            "_body_to_test_connections": body_to_test_connections,
            "_body_to_body_connections": body_to_body_connections,
            "__doc__": combined_docstring,
            "use_cache": use_cache,
        },
        {},
    )


def _verify_test_output(
    test_node_class: type[StaticNode], strict_condition_hint: bool = True
):
    test_outputs = test_node_class.preview_outputs()
    if len(test_outputs) != 1 or (
        strict_condition_hint and next(iter(test_outputs.values())) is not bool
    ):
        raise InvalidTestOutputError(
            f"While-loop Test node class {test_node_class.__name__} must have a single "
            f"boolean output channel when `strict_condition_hint = True`, but has "
            f"outputs {test_outputs}."
        )


def _verify_edges_exist(
    from_class: type[StaticNode], to_class: type[StaticNode], edges: label_connections
):
    for from_label, to_label in edges:
        if from_label not in from_class.preview_outputs():
            raise InvalidEdgeError(
                f"While-loop body node class {from_class.__name__} has no output "
                f"channel {from_label}."
            )
        if to_label not in to_class.preview_inputs():
            raise InvalidEdgeError(
                f"While-loop test or body node class {to_class.__name__} has no input {to_label}."
            )


def while_node(
    test_node_class: type[StaticNode],
    body_node_class: type[StaticNode],
    body_to_test_connections: label_connections_like,
    body_to_body_connections: label_connections_like,
    *node_args,
    use_cache: bool = True,
    strict_condition_hint: bool = True,
    **node_kwargs,
):
    """
    Make a new
    Args:
        test_node_class (type[StaticNode]): The class to use for the test condition.
            Must have a single output channel, and this must be hinted as boolean when
            :arg:`strict_condition_hint = True`.
        body_node_class (type[StaticNode]): The class to use for the body of the loop.
        body_to_test_connections (label_connections): Stringy representations of edge(s) from the body to the test instances.
        body_to_body_connections (label_connections): Stringy representations of edge(s) from the last-body to the next-body instances.
        *node_args: All the usual parent class args.
        use_cache (bool): Default class behavior for whether to skip running this node when its inputs match cached values. (Default is True, try to exploit caching.)
        strict_condition_hint (bool): Whether to demand that the :arg:`test_node_class` has its single output hinted as boolean. (Default is True.)
        **node_kwargs: All the usual parent class kwargs, including :arg:`executor_for_test` and :arg:`executor_for_body` that are specific to :class:`While`.

    Returns:
        While: An instance of the new :class:`While` subclass.

    Examples:
        >>> from pyiron_workflow import Workflow
        >>> wf = Workflow("my_while_loop")
        >>> wf.x0 = Workflow.create.std.UserInput(0)
        >>> wf.limit = Workflow.create.std.UserInput(5)
        >>> wf.step = Workflow.create.std.UserInput(2)
        >>> wf.add_while = Workflow.create.while_node(
        ...     Workflow.create.std.LessThan,  # Test
        ...     Workflow.create.std.Add,  # Body
        ...     (("add", "obj"),),  # body-to-test
        ...     (("add", "obj"),),  # body-to-body
        ...     strict_condition_hint=False,  # LessThan doesn't hint it's boolean return...
        ...     test_obj=wf.x0,
        ...     test_other=wf.limit,
        ...     body_obj=wf.x0,
        ...     body_other=wf.step
        ... )
        >>> wf()
        {'add_while__add': 6}

        Note how the while-node input maps the test and body node inputs with a pre-pended identifier, while the output maps the body node outputs directly.
    """
    b2t = _tuplefy(body_to_test_connections)
    b2b = _tuplefy(body_to_body_connections)
    while_node_factory.clear(
        _while_node_class_name(test_node_class, body_node_class, b2t, b2b)
    )
    cls = while_node_factory(
        test_node_class,
        body_node_class,
        b2t,
        b2b,
        use_cache,
        strict_condition_hint,
    )
    return cls(*node_args, **node_kwargs)
