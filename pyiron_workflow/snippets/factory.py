"""
Tools for making dynamically generated classes unique, and their instances pickleable.

Provides two main user-facing tools: :func:`classfactory`, which should be used
_exclusively_ as a decorator (this restriction pertains to namespace requirements for
re-importing), and `ClassFactory`, which can be used to instantiate a new factory from
some existing factory function.

In both cases, the decorated function/input argument should be a pickleable function
taking only positional arguments, and returning a tuple suitable for use in dynamic
class creation via :func:`builtins.type` -- i.e. taking a class name, a tuple of base
classes, a dictionary of class attributes, and a dictionary of values to be expanded
into kwargs for `__subclass_init__`.

The resulting factory produces classes that are (a) pickleable, and (b) the same object
as any previously built class with the same name. (Note: avoiding class degeneracy with
respect to class name is the responsibility of the person writing the factory function.)

These classes are then themselves pickleable, and produce instances which are in turn
pickleable (so long as any data they've been fed as inputs or attributes is pickleable,
i.e. here the only pickle-barrier we resolve is that of having come from a dynamically
generated class).

Since users need to build their own class factories returning classes with sensible
names, we also provide a helper function :func:`sanitize_callable_name`, which makes
sure a string is compliant with use as a class name. This is run internally on user-
provided names, and failure for the user name and sanitized name to match will give a
clear error message.

Constructed classes can, in turn be used as bases in further class factories.
"""

from __future__ import annotations

from abc import ABC, ABCMeta
from functools import wraps
from importlib import import_module
from inspect import signature, Parameter
import pickle
from re import sub
from typing import ClassVar


class _SingleInstance(ABCMeta):
    """Simple singleton pattern."""

    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_SingleInstance, cls).__call__(*args, **kwargs)
        return cls._instance


class _FactoryTown(metaclass=_SingleInstance):
    """
    Makes sure two factories created around the same factory function are the same
    factory object.
    """

    factories = {}

    @classmethod
    def clear(cls):
        """
        Remove factories.

        Can be useful if you're
        """
        cls.factories = {}

    @staticmethod
    def _factory_address(factory_function: callable) -> str:
        return f"{factory_function.__module__}.{factory_function.__qualname__}"

    def get_factory(self, factory_function: callable[..., type]) -> _ClassFactory:

        self._verify_function_only_takes_positional_args(factory_function)

        address = self._factory_address(factory_function)

        try:
            return self.factories[address]
        except KeyError:
            factory = self._build_factory(factory_function)
            self.factories[address] = factory
            return factory

    @staticmethod
    def _build_factory(factory_function):
        """
        Subclass :class:`_ClassFactory` and make an instance.
        """
        new_factory_class = type(
            sanitize_callable_name(
                f"{factory_function.__module__}{factory_function.__qualname__}"
                f"{factory_function.__name__.title()}"
                f"{_ClassFactory.__name__}"
            ).replace("_", ""),
            (_ClassFactory,),
            {},
            factory_function=factory_function,
        )
        return wraps(factory_function)(new_factory_class())

    @staticmethod
    def _verify_function_only_takes_positional_args(factory_function: callable):
        parameters = signature(factory_function).parameters.values()
        if any(
            p.kind not in [Parameter.POSITIONAL_ONLY, Parameter.VAR_POSITIONAL]
            for p in parameters
        ):
            raise InvalidFactorySignature(
                f"{_ClassFactory.__name__} can only be subclassed using factory "
                f"functions that take exclusively positional arguments, but "
                f"{factory_function.__name__} has the parameters {parameters}"
            )


_FACTORY_TOWN = _FactoryTown()


class InvalidFactorySignature(ValueError):
    """When the factory function's arguments are not purely positional"""

    pass


class InvalidClassNameError(ValueError):
    """When a string isn't a good class name"""

    pass


class _ClassFactory(metaclass=_SingleInstance):
    """
    For making dynamically created classes the same class.
    """

    _decorated_as_classfactory: ClassVar[bool] = False

    def __init_subclass__(cls, /, factory_function, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.factory_function = staticmethod(factory_function)
        cls.class_registry = {}

    def __call__(self, *args) -> type[_FactoryMade]:
        name, bases, class_dict, sc_init_kwargs = self.factory_function(*args)
        self._verify_name_is_legal(name)
        try:
            return self.class_registry[name]
        except KeyError:
            factory_made = self._build_class(
                name,
                bases,
                class_dict,
                sc_init_kwargs,
                args,
            )
            self.class_registry[name] = factory_made
            return factory_made

    @classmethod
    def clear(cls, *class_names, skip_missing=True):
        """
        Remove constructed class(es).

        Can be useful if you've updated the constructor and want to remove old
        instances.

        Args:
            *class_names (str): The names of classes to remove. Removes all of them
                when empty.
            skip_missing (bool): Whether to pass over key errors when a name is
                requested that is not currently in the class registry. (Default is
                True, let missing names pass silently.)
        """
        if len(class_names) == 0:
            cls.class_registry = {}
        else:
            for name in class_names:
                try:
                    cls.class_registry.pop(name)
                except KeyError as e:
                    if skip_missing:
                        continue
                    else:
                        raise KeyError(f"Could not find class {name}")

    def _build_class(
        self, name, bases, class_dict, sc_init_kwargs, class_factory_args
    ) -> type[_FactoryMade]:

        if "__module__" not in class_dict.keys():
            class_dict["__module__"] = self.factory_function.__module__
        if "__qualname__" not in class_dict.keys():
            class_dict["__qualname__"] = f"{self.__qualname__}.{name}"
        sc_init_kwargs["class_factory"] = self
        sc_init_kwargs["class_factory_args"] = class_factory_args

        if not any(_FactoryMade in base.mro() for base in bases):
            bases = (_FactoryMade, *bases)

        return type(name, bases, class_dict, **sc_init_kwargs)

    @staticmethod
    def _verify_name_is_legal(name):
        sanitized_name = sanitize_callable_name(name)
        if name != sanitized_name:
            raise InvalidClassNameError(
                f"The class name {name} failed to match with its sanitized version"
                f"({sanitized_name}), please supply a valid class name."
            )

    def __reduce__(self):
        if (
            self._decorated_as_classfactory
            and "<locals>" not in self.factory_function.__qualname__
        ):
            return (
                _import_object,
                (self.factory_function.__module__, self.factory_function.__qualname__),
            )
        else:
            return (_FACTORY_TOWN.get_factory, (self.factory_function,))


def _import_object(module_name, qualname):
    module = import_module(module_name)
    obj = module
    for name in qualname.split("."):
        obj = getattr(obj, name)
    return obj


class _FactoryMade(ABC):
    """
    A mix-in to make class-factory-produced classes pickleable.

    If the factory is used as a decorator for another function, it will conflict with
    this function (i.e. the owned function will be the true function, and will mismatch
    with imports from that location, which will return the post-decorator factory made
    class). This can be resolved by setting the
    :attr:`_class_returns_from_decorated_function` attribute to be the decorated
    function in the decorator definition.
    """

    _class_returns_from_decorated_function: ClassVar[callable | None] = None

    def __init_subclass__(cls, /, class_factory, class_factory_args, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._class_factory = class_factory
        cls._class_factory_args = class_factory_args
        cls._factory_town = _FACTORY_TOWN

    def __reduce__(self):
        if (
            self._class_returns_from_decorated_function is not None
            and "<locals>"
            not in self._class_returns_from_decorated_function.__qualname__
        ):
            # When we create a class by decorating some other function, this class
            # conflicts with its own factory_function attribute in the namespace, so we
            # rely on directly re-importing the factory
            return (
                _instantiate_from_decorated,
                (
                    self._class_returns_from_decorated_function.__module__,
                    self._class_returns_from_decorated_function.__qualname__,
                    self.__getnewargs_ex__(),
                ),
                self.__getstate__(),
            )
        else:
            return (
                _instantiate_from_factory,
                (
                    self._class_factory,
                    self._class_factory_args,
                    self.__getnewargs_ex__(),
                ),
                self.__getstate__(),
            )

    def __getnewargs_ex__(self):
        # Child classes can override this as needed
        return (), {}

    def __getstate__(self):
        # Python <3.11 compatibility
        try:
            return super().__getstate__()
        except AttributeError:
            return dict(self.__dict__)

    def __setstate__(self, state):
        # Python <3.11 compatibility
        try:
            super().__setstate__(state)
        except AttributeError:
            self.__dict__.update(**state)


def _instantiate_from_factory(factory, factory_args, newargs_ex):
    """
    Recover the dynamic class, then invoke its `__new__` to avoid instantiation (and
    the possibility of positional args in `__init__`).
    """
    cls = factory(*factory_args)
    return cls.__new__(cls, *newargs_ex[0], **newargs_ex[1])


def _instantiate_from_decorated(module, qualname, newargs_ex):
    """
    In case the class comes from a decorated function, we need to import it directly.
    """
    cls = _import_object(module, qualname)
    return cls.__new__(cls, *newargs_ex[0], **newargs_ex[1])


def classfactory(
    factory_function: callable[..., tuple[str, tuple[type, ...], dict, dict]]
) -> _ClassFactory:
    """
    A decorator for building dynamic class factories whose classes are unique and whose
    terminal instances can be pickled.

    Under the hood, classes created by factories get dependence on
    :class:`_FactoryMade` mixed in. This class leverages :meth:`__reduce__` and
    :meth:`__init_subclass__` and uses up the class namespace :attr:`_class_factory`
    and :attr:`_class_factory_args` to hold data (using up corresponding public
    variable names in the :meth:`__init_subclass__` kwargs), so any interference with
    these fields may cause unexpected side effects. For un-pickling, the dynamic class
    gets recreated then its :meth:`__new__` is called using `__newargs_ex__`; a default
    implementation returning no arguments is provided on :class:`_FactoryMade` but can
    be overridden.

    Args:
        factory_function (callable[..., tuple[str, tuple[type, ...], dict, dict]]):
            A function returning arguments that would be passed to `builtins.type` to
            dynamically generate a class. The function must accept exclusively
            positional arguments

    Returns:
        (type[_ClassFactory]): A new callable that returns unique classes whose
            instances can be pickled.

    Notes:
        If the :param:`factory_function` itself, or any data stored on instances of
        its resulting class(es) cannot be pickled, then the instances will not be able
        to be pickled. Here we only remove the trouble associated with pickling
        dynamically created classes.

        If the `__init_subclass__` kwargs are exploited, remember that these are
        subject to all the same "gotchas" as their regular non-factory use; namely, all
        child classes must specify _all_ parent class kwargs in order to avoid them
        getting overwritten by the parent class defaults!

        Dynamically generated classes can, in turn, be used as base classes for further
        `@classfactory` decorated factory functions.

    Warnings:
        Use _exclusively_ as a decorator. For an inline constructor for an existing
        callable, use :class:`ClassFactory` instead.

    Examples:
        >>> import pickle
        >>>
        >>> from pyiron_workflow.snippets.factory import classfactory
        >>>
        >>> class HasN(ABC):
        ...     '''Some class I want to make dynamically subclass.'''
        ...     def __init_subclass__(cls, /, n=0, s="foo", **kwargs):
        ...         super(HasN, cls).__init_subclass__(**kwargs)
        ...         cls.n = n
        ...         cls.s = s
        ...
        ...     def __init__(self, x, y=0):
        ...         self.x = x
        ...         self.y = y
        >>>
        >>> @classfactory
        ... def has_n_factory(n, s="wrapped_function", /):
        ...     return (
        ...         f"{HasN.__name__}{n}{s}",  # New class name
        ...         (HasN,),  # Base class(es)
        ...         {},  # Class attributes dictionary
        ...         {"n": n, "s": s}
        ...         # dict of `builtins.type` kwargs (passed to `__init_subclass__`)
        ...     )
        >>>
        >>> Has2 = has_n_factory(2, "my_dynamic_class")
        >>> HasToo = has_n_factory(2, "my_dynamic_class")
        >>> HasToo is Has2
        True

        >>> foo = Has2(42, y=-1)
        >>> print(foo.n, foo.s, foo.x, foo.y)
        2 my_dynamic_class 42 -1

        >>> reloaded = pickle.loads(pickle.dumps(foo))  # doctest: +SKIP
        >>> print(reloaded.n, reloaded.s, reloaded.x, reloaded.y)  # doctest: +SKIP
        2 my_dynamic_class 42 -1  # doctest: +SKIP

    """
    factory = _FACTORY_TOWN.get_factory(factory_function)
    factory._decorated_as_classfactory = True
    return factory


class ClassFactory:
    """
    A constructor for new class factories.

    Use on existing class factory callables, _not_ as a decorator.

    Cf. the :func:`classfactory` decorator for more info.
    """

    def __new__(cls, factory_function):
        return _FACTORY_TOWN.get_factory(factory_function)


def sanitize_callable_name(name: str):
    """
    A helper class for sanitizing a string so it's appropriate as a class/function name.
    """
    # Replace non-alphanumeric characters except underscores
    sanitized_name = sub(r"\W+", "_", name)
    # Ensure the name starts with a letter or underscore
    if (
        len(sanitized_name) > 0
        and not sanitized_name[0].isalpha()
        and sanitized_name[0] != "_"
    ):
        sanitized_name = "_" + sanitized_name
    return sanitized_name
