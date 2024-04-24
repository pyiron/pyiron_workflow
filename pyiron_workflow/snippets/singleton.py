# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Tools for singleton objects.
"""

from __future__ import annotations

from abc import ABCMeta
from functools import wraps


class Singleton(ABCMeta):
    """
    Implemented with suggestions from

    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class _RegisteredFactory(metaclass=Singleton):
    """
    For making dynamically created classes the same class.

    Used under-the-hood in the public facing decorator below.
    """

    def __init_subclass__(cls, /, factory_function: callable[..., type]):
        cls.factory_function = staticmethod(factory_function)
        cls.registry = {}

    def __call__(self, *args, **kwargs):
        constructed = self.factory_function(*args, **kwargs)
        try:
            return self.registry[constructed.__name__]
        except KeyError:
            self.registry[constructed.__name__] = constructed
            return constructed

    def __reduce__(self):
        return (
            _FactoryTown().get_registered_factory,
            (self.factory_function,),
        )


class _FactoryTown(metaclass=Singleton):
    """
    A singleton to hold existing factories, so that if we wrap the same factory
    function more than once, we always get back the same factory.
    """

    factories = {}

    def get_registered_factory(
        self, factory_function: callable[..., type]
    ) -> type[_RegisteredFactory]:
        try:
            return self.factories[id(factory_function)]
        except KeyError:
            factory_class = type(
                f"{_RegisteredFactory.__name__}{factory_function.__name__.title()}",
                (_RegisteredFactory,),
                {},
                factory_function=factory_function,
            )
            factory = wraps(factory_function)(factory_class())
            self.factories[id(factory_function)] = factory
            return factory


def registered_factory(factory_function: callable[..., type]):
    """
    A decorator for wrapping class factories.

    Wrapped factories return the _same object_ when they would generate a class with
    the same name as they have generated before. I.e. a sort of singleton-class
    generator where the classes themselves are singleton, not their instances.

    Args:
        factory_function (callabe[..., type]): The class factory to wrap.

    Returns:
        (_RegisteredFactory): A factory instance that will return the same class object
            whenever the factory method would return a class whose name has been seen
            before.

    Example:
        >>> from abc import ABC
        >>>
        >>> from pyiron_workflow.snippets.singleton import registered_factory
        >>>
        >>> class Foo(ABC):
        ...     def __init_subclass__(cls, /, n=0, **kwargs):
        ...         super().__init_subclass__(**kwargs)
        ...         cls.n = n
        >>>
        >>> @registered_factory
        ... def foo_factory(n):
        ...     return type(
        ...         f"{Foo.__name__}{n}",
        ...         (Foo,),
        ...         {},
        ...         n=n
        ...     )
        >>>
        >>> FooTwo = foo_factory(2)
        >>> Foo2 = foo_factory(2)
        >>> print(FooTwo.__name__, FooTwo.n)
        Foo2 2

        >>> print(Foo2.__name__, Foo2.n)
        Foo2 2

        >>> print(FooTwo is Foo2)
        True
    """
    return _FactoryTown().get_registered_factory(factory_function)
