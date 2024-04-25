"""
Tools for making dynamically generated classes unique, and their instances pickleable.
"""

from __future__ import annotations

from abc import ABC, ABCMeta
from functools import wraps
from importlib import import_module
from inspect import signature, Parameter
from re import sub


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

    @staticmethod
    def _factory_address(factory_function: callable) -> str:
        return f"{factory_function.__module__}.{factory_function.__qualname__}"

    def get_factory(
        self, factory_function: callable[..., type]
    ) -> _ClassFactory:

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
            ).replace('_', ''),
            (_ClassFactory,),
            {},
            factory_function=factory_function
        )
        return wraps(factory_function)(new_factory_class())

    @staticmethod
    def _verify_function_only_takes_positional_args(factory_function: callable):
        parameters = signature(factory_function).parameters.values()
        if any(
            p.kind not in [
                Parameter.POSITIONAL_ONLY,
                Parameter.VAR_POSITIONAL
            ] for p in parameters
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
    pass


class _ClassFactory(metaclass=_SingleInstance):
    """
    For making dynamically created classes the same class.
    """
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

    def _build_class(
        self, name, bases, class_dict, sc_init_kwargs, class_factory_args
    ) -> type[_FactoryMade]:

        class_dict["__module__"] = self.factory_function.__module__
        sc_init_kwargs["class_factory"] = self
        sc_init_kwargs["class_factory_args"] = class_factory_args

        if not any(
            _FactoryMade in base.mro() for base in bases
        ):
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
        if hasattr(self.factory_function, "_decorated_as_classfactory"):
            # When we decorate a function, this object conflicts with its own
            # factory_function attribute in the namespace, so we rely on directly
            # re-importing the factory
            return (
                import_object,
                (self.factory_function.__module__, self.factory_function.__qualname__)
            )
        else:
            return (
                _FACTORY_TOWN.get_factory,
                (self.factory_function,)
            )


def import_object(module_name, qualname):
    module = import_module(module_name)
    obj = module
    for name in qualname.split("."):
        obj = getattr(obj, name)
    return obj


class _FactoryMade(ABC):
    """
    A mix-in to make class-factory-produced classes pickleable.
    """
    def __init__(self, *args, **kwargs):
        self.__instance_args = args
        self.__instance_kwargs = kwargs
        super().__init__(*args, **kwargs)

    def __init_subclass__(cls, /, class_factory, class_factory_args, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._class_factory = class_factory
        cls._class_factory_args = class_factory_args

    def __reduce__(self):
        return (
            _instantiate_from_factory,
            (
                self._class_factory,
                self._class_factory_args,
                self.__instance_args,
                self.__instance_kwargs,
            ),
            self.__getstate__(),
        )

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


def _instantiate_from_factory(factory, factory_args, instance_args, instance_kwargs):
    return factory(*factory_args)(*instance_args, **instance_kwargs)


def classfactory(
    factory_function: callable[..., tuple[str, tuple[type, ...], dict, dict]]
) -> _ClassFactory:
    """
    A decorator for building dynamic class factories whose classes are unique and whose
    terminal instances can be pickled.

    Args:
        factory_function (callable[..., tuple[str, tuple[type, ...], dict, dict]]):
            A function returning arguments that would be passed to `builtins.type` to
            dynamically generate a class. The function must accept exclusively
            positional arguments

    Returns:
        (type[_ClassFactory]): A new callable that returns unique classes whose
            instances can be pickled.

    Note:
        If the :param:`factory_function` itself, or any data stored on instances of
        its resulting class(es) cannot be pickled, then the instances will not be able
        to be pickled. Here we only remove the trouble associated with pickling
        dynamically created classes.
    """
    factory = _FACTORY_TOWN.get_factory(factory_function)
    factory.factory_function._decorated_as_classfactory = True
    return factory


class ClassFactory:
    def __new__(cls, factory_function):
        return _FACTORY_TOWN.get_factory(factory_function)


def sanitize_callable_name(name: str):
    """
    A helper class for sanitizing a string so it's appropriate as a class/function name.
    """
    # Replace non-alphanumeric characters except underscores
    sanitized_name = sub(r'\W+', '_', name)
    # Ensure the name starts with a letter or underscore
    if (
        len(sanitized_name) > 0
        and not sanitized_name[0].isalpha()
        and sanitized_name[0] != '_'
    ):
        sanitized_name = '_' + sanitized_name
    return sanitized_name
