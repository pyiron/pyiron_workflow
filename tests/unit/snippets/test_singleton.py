from abc import ABC
import unittest

from pyiron_workflow.snippets.singleton import Singleton, registered_factory


class TestSingleton(unittest.TestCase):
    def test_uniqueness(self):
        class Foo(metaclass=Singleton):
            def __init__(self):
                self.x = 1

        f1 = Foo()
        f2 = Foo()
        self.assertIs(f1, f2)
        f2.x = 2
        self.assertEqual(2, f1.x)


class TestRegisteredFactory(unittest.TestCase):
    def test_decorator(self):
        class Foo(ABC):
            def __init_subclass__(cls, /, n=0, **kwargs):
                super().__init_subclass__(**kwargs)
                cls.n = n

        def foo_factory(n):
            """The foo factory docstring."""
            return type(
                f"{Foo.__name__}{n}",
                (Foo,),
                {},
                n=n
            )

        FooTwo = foo_factory(2)
        Foo2 = foo_factory(2)
        self.assertEqual(
            FooTwo.__name__,
            Foo2.__name__,
            msg="Sanity check"
        )
        self.assertIsNot(
            FooTwo,
            Foo2,
            msg="Sanity check"
        )
        self.assertEqual(
            2,
            Foo2.n,
            msg="Sanity check"
        )

        Foo3 = foo_factory(3)
        self.assertIsNot(
            Foo3,
            Foo2,
            msg="Sanity check"
        )

        registered_foo_factory = registered_factory(foo_factory)
        FooTwo = registered_foo_factory(2)
        Foo2 = registered_foo_factory(2)
        self.assertEqual(
            FooTwo.__name__,
            Foo2.__name__,
            msg="Sanity check"
        )
        self.assertIs(
            FooTwo,
            Foo2,
            msg="The point of the registration is that dynamically generated classes "
                "with the same name from the same generator should wind up being the "
                "same class"
        )
        self.assertEqual(
            "The foo factory docstring.",
            registered_foo_factory.__doc__,
            msg="The wrapper should preserve the factory's docstring"
        )

        re_registered_foo_factory = registered_factory(foo_factory)
        self.assertIs(
            registered_foo_factory,
            re_registered_foo_factory,
            msg="The factories themselves are singletons based on the id of the "
                "factory function they use; If you register the same factory function "
                "twice, you should get the same factory back."
        )
        self.assertIs(
            re_registered_foo_factory(2),
            Foo2,
            msg="From the above, it should hold trivially that building the same class "
                "from each factory gives the same class."
        )

        @registered_factory
        def different_but_similar(n):
            """The foo factory docstring."""
            return foo_factory(n)

        Foo2b = different_but_similar(2)
        self.assertEqual(
            Foo2b.__name__,
            Foo2.__name__,
            msg="Sanity check. It's the same factory behaviour after all."
        )
        self.assertIsNot(
            Foo2b,
            Foo2,
            msg="These come from two separate registered factories, each of which is "
                "maintaining its own internal registration list."
        )


if __name__ == '__main__':
    unittest.main()
