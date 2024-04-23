from __future__ import annotations

from abc import ABC, abstractmethod


class Constructed(ABC):
    """
    A mixin which overrides `__reduce__` to return a constructor callable and arguments.

    This is useful for classes which are not importable, but who have constructor
    functions that _are_, e.g. children of `StaticNode` that are dynamically created so
    their flexibility comes from the class.

    Unlike overriding `__new__`, no changes are needed for the `__init__
    """

    @property
    @abstractmethod
    def _instance_constructor_args(self) -> tuple:
        """
        The arguments to pass to :meth:`_instance_constructor`.

        Should be a 3-tuple of the callable class factory, class factory args, and
        class factory kwargs. On unpickling, instance args and kwargs get provided
        by :meth:`__setstate__` from the pickled state, so these can be ignored.
        """

    @staticmethod
    def _instance_constructor(
        class_factory,
        factory_args,
        factory_kwargs,
        instance_args,
        instance_kwargs,
    ) -> callable[[...], Constructed]:
        """
        A constructor function returning an instance of the class.

        Args:
            class_factory (callable): A method returning the class
            factory_args (tuple): Args to pass to the factory method.
            factory_kwargs (dict): Kwargs to pass to the factory method.
            instance_args (tuple): Args to pass to the new instance.
            instance_kwargs (dict): Kwargs to pass to the new instance.

        Returns:
            (Constructed): A new instance of the factory result mixed with
                :class:`Constructed`.
        """
        return construct_instance(
            class_factory, factory_args, factory_kwargs, instance_args, instance_kwargs
        )

    def __reduce__(self):
        return (
            self._instance_constructor,
            (*self._instance_constructor_args, (), {}),
            self.__getstate__()
        )


def construct_instance(
    class_factory, factory_args, factory_kwargs, instance_args, instance_kwargs
):
    """
    A constructor function for classes that inherit from :class:`Constructed`.

    Args:
        class_factory (callable): A method returning a new class, i.e. `return type...`.
        factory_args (tuple): Args to pass to the factory method.
        factory_kwargs (dict): Kwargs to pass to the factory method.
        instance_args (tuple): Args to pass to the new instance.
        instance_kwargs (dict): Kwargs to pass to the new instance.

    Returns:
        (Constructed): A new instance of the factory result mixed with
            :class:`Constructed`.
    """
    return class_factory(*factory_args, **factory_kwargs)(
        *instance_args, **instance_kwargs
    )


def mix_and_construct_instance(
    class_factory, factory_args, factory_kwargs, instance_args, instance_kwargs
):
    """
    A constructor function that dynamically mixes in :class:`Constructed` inheritance.

    Args:
        class_factory (callable): A method returning a new class, i.e. `return type...`.
        factory_args (tuple): Args to pass to the factory method.
        factory_kwargs (dict): Kwargs to pass to the factory method.
        instance_args (tuple): Args to pass to the new instance.
        instance_kwargs (dict): Kwargs to pass to the new instance.

    Returns:
        (Constructed): A new instance of the factory result mixed with
            :class:`Constructed`.
    """
    base_class = class_factory(*factory_args, **factory_kwargs)
    return type(
        Constructed.__name__ + base_class.__name__,
        (base_class, Constructed),
        {
            "_instance_constructor": staticmethod(mix_and_construct_instance),
            "_instance_constructor_args": (
                class_factory,
                factory_args,
                factory_kwargs,
                (),
                {}
            ),
            "__module__": base_class.__module__,
        }
    )(*instance_args, **instance_kwargs)
