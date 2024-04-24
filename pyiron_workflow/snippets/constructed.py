from __future__ import annotations

from abc import ABC


class Constructed(ABC):
    """
    A mixin which overrides `__reduce__` to return a constructor callable and arguments.

    This is useful for classes which are not importable, but who have constructor
    functions that _are_, e.g. classes dynamically created classes.

    Unlike overriding `__new__`, no changes are needed for the `__init__` signature.

    Child classes must define the :attr:`_class_factory`, as well as
    :attr:`_class_factory_args` and :attr:`_class_instance_args` (if either the factory
    or returned class's `__init__` take positional arguments), and may optionally
    specify :attr:`_class_factory_kwargs` and/or :attr:`_class_instance_kwargs` if
    either callable has keyword arguments that should be specified.
    In the case that the mixin is being added dynamically at instantiation, subclass
    kwargs need to be re-specified in :attr:`_class_subclass_kwargs`.
    All of these parameters should be specified at the time of subclassing (via
    :meth:`__init_subclass__`) using keywords of the same (but public) names.
    """

    def __init_subclass__(
        cls,
        /,
        class_factory: callable = None,
        class_factory_args: tuple = (),
        class_factory_kwargs: dict | None = None,
        class_instance_args: tuple = (),
        class_instance_kwargs: dict | None = None,
        class_subclass_kwargs: dict | None = None,
        **kwargs
    ):
        super(Constructed, cls).__init_subclass__(**kwargs)

        cls._class_factory = staticmethod(class_factory)
        if class_factory is not None:
            # Intermediate abstract classes may not yet specify a factory
            cls.__module__ = class_factory.__module__

        cls._class_factory_args = class_factory_args
        cls._class_factory_kwargs = (
            {} if class_factory_kwargs is None else class_factory_kwargs
        )
        cls._class_instance_args = class_instance_args
        cls._class_instance_kwargs = (
            {} if class_instance_kwargs is None else class_instance_kwargs
        )
        cls._class_subclass_kwargs = (
            {} if class_subclass_kwargs is None else class_subclass_kwargs
        )

    @staticmethod
    def _instance_constructor(
        class_factory,
        factory_args,
        factory_kwargs,
        instance_args,
        instance_kwargs,
        subclass_kwargs,
    ) -> callable[[...], Constructed]:
        """
        A constructor function returning an instance of the class.

        Args:
            class_factory (callable): A method returning the class
            factory_args (tuple): Args to pass to the factory method.
            factory_kwargs (dict): Kwargs to pass to the factory method.
            instance_args (tuple): Args to pass to the new instance.
            instance_kwargs (dict): Kwargs to pass to the new instance.
            subclass_kwargs (dict): Kwargs to pass to `type` when this method is
                overridden by :func:`mix_and_construct_instance`.

        Returns:
            (Constructed): A new instance of the factory result mixed with
                :class:`Constructed`.
        """
        return class_factory(*factory_args, **factory_kwargs)(
            *instance_args, **instance_kwargs
        )

    def __reduce__(self):
        return (
            self._instance_constructor,
            (
                self._class_factory,
                self._class_factory_args,
                self._class_factory_kwargs,
                self._class_instance_args,  # Args may be _necessary_ for __init__
                self._class_instance_kwargs,
                self._class_subclass_kwargs,
            ),
            self.__getstate__()
        )

    def __getstate__(self):
        # Backwards compatibility
        try:
            super().__getstate__()
        except AttributeError:
            return dict(self.__dict__)

    def __setstate__(self, state):
        # Backwards compatibility
        try:
            super().__setstate__(state)
        except AttributeError:
            self.__dict__.update(**state)


def mix_and_construct_instance(
    class_factory,
    factory_args,
    factory_kwargs,
    instance_args,
    instance_kwargs,
    subclass_kwargs
):
    """
    A constructor function that dynamically mixes in :class:`Constructed` inheritance.

    Args:
        class_factory (callable): A method returning a new class, i.e. `return type...`.
        factory_args (tuple): Args to pass to the factory method.
        factory_kwargs (dict): Kwargs to pass to the factory method.
        instance_args (tuple): Args to pass to the new instance.
        instance_kwargs (dict): Kwargs to pass to the new instance.
        subclass_kwargs (dict): Kwargs to pass to `type` to make sure subclassing
            doesn't lose info passed to the factory by getting overwritten by the
            superclass's `__init_subclass__` default values.

    Returns:
        (Constructed): A new instance of the factory result mixed with
            :class:`Constructed`.
    """
    base_class = class_factory(*factory_args, **factory_kwargs)
    return type(
        Constructed.__name__ + base_class.__name__,
        (Constructed, base_class),
        {"_instance_constructor": staticmethod(mix_and_construct_instance)},
        class_factory=class_factory,
        class_factory_args=factory_args,
        class_factory_kwargs=factory_kwargs,
        class_instance_args=instance_args,
        class_subclass_kwargs=subclass_kwargs,
        **subclass_kwargs
    )(*instance_args, **instance_kwargs)
