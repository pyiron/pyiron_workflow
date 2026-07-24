import functools


def overloaded_classmethod(class_method):
    """
    Decorator to define a method that behaves like both a classmethod and an
    instancemethod under the same name.

    Args:
        instance_method: A method defined on the same object as the decorated method,
            to be used when an instance of the object calls the decorated method (
            instead of a class call)

    Returns
    -------
    descriptor
        A descriptor that dispatches to the classmethod when accessed
        via the class, and to the given instance method when accessed
        via an instance.

    Examples:
        >>> class Foo:
        ...     def __init__(self, y):
        ...         self.y = y
        ...
        ...     @classmethod
        ...     def _doit_classmethod(cls, x):
        ...         return f"Class {cls.__name__} doing {x}"
        ...
        ...     @overloaded_classmethod(class_method=_doit_classmethod)
        ...     def doit(self, x):
        ...         return f"Instance of type {type(self).__name__} doing {x} + {self.y}"
        ...
        >>> Foo.doit(10)
        'Class Foo doing 10'
        >>> Foo(5).doit(20)
        'Instance of type Foo doing 20 + 5'
    """

    class Overloaded:
        def __init__(self, f_instance, f_class):
            self.f_instance = f_instance
            self.f_class = f_class
            functools.update_wrapper(self, f_instance)

        def __get__(self, obj, cls):
            if obj is None:
                f_class = (
                    cls.__dict__[self.f_class]
                    if isinstance(self.f_class, str)
                    else self.f_class
                )

                if isinstance(f_class, classmethod):
                    f_class = f_class.__func__

                @functools.wraps(self.f_class)
                def bound(*args, **kwargs):
                    return f_class(cls, *args, **kwargs)

                return bound
            else:

                @functools.wraps(self.f_instance)
                def bound(*args, **kwargs):
                    return self.f_instance(obj, *args, **kwargs)

                return bound

    def wrapper(f_instance):
        return Overloaded(f_instance, class_method)

    return wrapper
