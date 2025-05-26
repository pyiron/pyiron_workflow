import abc
from typing import ClassVar, TypeAlias, Any

from pyiron_snippets import factory

from pyiron_workflow.nodes.composite import Composite
from pyiron_workflow.nodes.static_io import StaticNode

label_connection: TypeAlias = tuple[str, str]
label_connections: TypeAlias = tuple[label_connection, ...]
label_connections_like: TypeAlias = (
    label_connection | list[label_connection] | label_connections
)


class InvalidTestOutputError(ValueError):
    pass


def _tuplefy(connections: label_connections_like) -> label_connections:
    if isinstance(connections, tuple) and isinstance(connections[0], str):
        return (connections,)
    elif (
        isinstance(connections, tuple)
        and isinstance(connections[0], tuple)
        and isinstance(connections[0][0], str)
    ):
        return connections
    elif (
        isinstance(connections, list)
        and isinstance(connections[0], tuple)
        and isinstance(connections[0][0], str)
    ):
        return tuple(connections)
    raise TypeError(
        f"Expected connections in the form {label_connections_like}, "
        f"but got {connections}."
    )


class While(Composite, StaticNode, abc.ABC):
    _body_node_class: ClassVar[type[StaticNode]]
    _test_node_class: ClassVar[type[StaticNode]]
    _body_to_body_connections: ClassVar[label_connections]
    _body_to_test_connections: ClassVar[label_connections]
    _test_stem: ClassVar[str] = "test_"
    _body_stem: ClassVar[str] = "body_"

    @classmethod
    def _build_inputs_preview(cls) -> dict[str, tuple[Any, Any]]:
        preview = {}
        for label, (hint, default) in cls._test_node_class.preview_inputs().items():
            preview[cls._test_stem + label] = (hint, default)
        for label, (hint, default) in cls._body_node_class.preview_inputs().items():
            preview[cls._body_stem + label] = (hint, default)
        return preview

    @classmethod
    def _build_outputs_preview(cls) -> dict[str, Any]:
        cls._verify_test_output()
        return dict(cls._body_node_class.preview_outputs())

    @classmethod
    def _verify_test_output(cls):
        test_outputs = cls._test_node_class.preview_outputs()
        if len(test_outputs) != 1 or next(iter(test_outputs.values())) is not bool:
            raise InvalidTestOutputError(
                f"Test node {cls._test_node_class.__name__} must have a single boolean "
                f"output channel, but has outputs {test_outputs}."
            )


def _while_node_class_name(
    body_node_class: type[StaticNode],
    test_node_class: type[StaticNode],
    body_to_body_connections: label_connections,
    body_to_test_connections: label_connections,
) -> str:
    return (
        f"Do_{body_node_class.__name__}_While_{test_node_class.__name__}_With"
        f"_B2B_{'_'.join(con[0] + '2' + con[1] for con in body_to_body_connections)}"
        f"_B2T_{'_'.join(con[0] + '2' + con[1] for con in body_to_body_connections)}"
    )


@factory.classfactory
def while_node_factory(
    test_node_class: type[StaticNode],
    body_node_class: type[StaticNode],
    body_to_test_connections: label_connections_like,
    body_to_body_connections: label_connections_like,
    use_cache: bool = True,
    /,
) -> type[While]:
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
            body_node_class,
            test_node_class,
            body_to_body_connections,
            body_to_test_connections,
        ),
        (While,),
        {
            "_body_node_class": body_node_class,
            "_test_node_class": test_node_class,
            "_body_to_body_connections": body_to_body_connections,
            "_body_to_test_connections": body_to_test_connections,
            "__doc__": combined_docstring,
            "use_cache": use_cache,
        },
        {},
    )


def while_node(
    body_node_class: type[StaticNode],
    test_node_class: type[StaticNode],
    body_to_body_connections: label_connections_like,
    body_to_test_connections: label_connections_like,
    *node_args,
    use_cache: bool = True,
    **node_kwargs,
):
    b2b = _tuplefy(body_to_body_connections)
    b2t = _tuplefy(body_to_test_connections)
    while_node_factory.clear(
        _while_node_class_name(body_node_class, test_node_class, b2b, b2t)
    )
    cls = while_node_factory(
        body_node_class,
        test_node_class,
        b2b,
        b2t,
        use_cache,
    )
    return cls(*node_args, **node_kwargs)
