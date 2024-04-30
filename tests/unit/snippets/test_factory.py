from __future__ import annotations

from abc import ABC
import pickle
from typing import ClassVar
import unittest

import cloudpickle

from pyiron_workflow.snippets.factory import (
    _ClassFactory,
    _FactoryMade,
    ClassFactory,
    classfactory,
    InvalidClassNameError,
    InvalidFactorySignature,
    sanitize_callable_name
)


class HasN(ABC):
    def __init_subclass__(cls, /, n=0, s="foo", **kwargs):
        super().__init_subclass__(**kwargs)
        cls.n = n
        cls.s = s

    def __init__(self, x, *args, y=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.x = x
        self.y = y


@classfactory
def has_n_factory(n, s="wrapped_function", /):
    return (
        f"{HasN.__name__}{n}{s}",
        (HasN,),
        {},
        {"n": n, "s": s}
    )


def undecorated_function(n, s="undecorated_function", /):
    return (
        f"{HasN.__name__}{n}{s}",
        (HasN,),
        {},
        {"n": n, "s": s}
    )


def takes_kwargs(n, /, s="undecorated_function"):
    return (
        f"{HasN.__name__}{n}{s}",
        (HasN,),
        {},
        {"n": n, "s": s}
    )


class FactoryOwner:
    @staticmethod
    @classfactory
    def has_n_factory(n, s="decorated_method", /):
        return (
            f"{HasN.__name__}{n}{s}",
            (HasN,),
            {},
            {"n": n, "s": s}
        )


Has2 = has_n_factory(2, "factory_made")  # For testing repeated inheritance


class HasM(ABC):
    def __init_subclass__(cls, /, m=0, **kwargs):
        super(HasM, cls).__init_subclass__(**kwargs)
        cls.m = m

    def __init__(self, z, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.z = z


@classfactory
def has_n2_m_factory(m, /):
    return (
        f"HasN2M{m}",
        (Has2, HasM),
        {},
        {"m": m, "n": Has2.n, "s": Has2.s}
    )


@classfactory
def has_m_n2_factory(m, /):
    return (
        f"HasM{m}N2",
        (HasM, Has2,),
        {},
        {"m": m}
    )


class AddsNandX(ABC):
    fnc: ClassVar[callable]
    n: ClassVar[int]

    def __init__(self, x):
        self.x = x

    def add_to_function(self, *args, **kwargs):
        return self.fnc(*args, **kwargs) + self.n + self.x


@classfactory
def adder_factory(fnc, n, /):
    return (
        f"{AddsNandX.__name__}{fnc.__name__}",
        (AddsNandX,),
        {
            "fnc": staticmethod(fnc),
            "n": n,
            "_class_returns_from_decorated_function": fnc
        },
        {},
    )


def add_to_this_decorator(n):
    def wrapped(fnc):
        factory_made = adder_factory(fnc, n)
        factory_made._class_returns_from_decorated_function = fnc
        return factory_made
    return wrapped


@add_to_this_decorator(5)
def adds_5_plus_x(y: int):
    return y


class TestClassfactory(unittest.TestCase):

    def test_factory_initialization(self):
        self.assertTrue(
            issubclass(has_n_factory.__class__, _ClassFactory),
            msg="Creation by decorator should yield a subclass"
        )
        self.assertTrue(
            issubclass(ClassFactory(undecorated_function).__class__, _ClassFactory),
            msg="Creation by public instantiator should yield a subclass"
        )

        factory = has_n_factory(2, "foo")
        self.assertTrue(
            issubclass(factory, HasN),
            msg=f"Resulting class should inherit from the base"
        )
        self.assertEqual(2, factory.n, msg="Factory args should get interpreted")
        self.assertEqual("foo", factory.s, msg="Factory kwargs should get interpreted")

    def test_factory_uniqueness(self):
        f1 = classfactory(undecorated_function)
        f2 = classfactory(undecorated_function)

        self.assertIs(
            f1,
            f2,
            msg="Repeatedly packaging the same function should give the exact same "
                "factory"
        )
        self.assertIsNot(
            f1,
            has_n_factory,
            msg="Factory degeneracy is based on the actual wrapped function, we don't "
                "do any parsing for identical behaviour inside those functions."
        )

    def test_factory_pickle(self):
        with self.subTest("By decoration"):
            reloaded = pickle.loads(pickle.dumps(has_n_factory))
            self.assertIs(has_n_factory, reloaded)

        with self.subTest("From instantiation"):
            my_factory = ClassFactory(undecorated_function)
            reloaded = pickle.loads(pickle.dumps(my_factory))
            self.assertIs(my_factory, reloaded)

        with self.subTest("From qualname by decoration"):
            my_factory = FactoryOwner().has_n_factory
            reloaded = pickle.loads(pickle.dumps(my_factory))
            self.assertIs(my_factory, reloaded)

    def test_class_creation(self):
        n2 = has_n_factory(2, "something")
        self.assertEqual(
            2,
            n2.n,
            msg="Factory args should be getting parsed"
        )
        self.assertEqual(
            "something",
            n2.s,
            msg="Factory kwargs should be getting parsed"
        )
        self.assertTrue(
            issubclass(n2, HasN),
            msg=""
        )
        self.assertTrue(
            issubclass(n2, HasN),
            msg="Resulting classes should inherit from the requested base(s)"
        )

        with self.assertRaises(
            InvalidClassNameError,
            msg="Invalid class names should raise an error"
        ):
            has_n_factory(
                2,
                "our factory function uses this as part of the class name, but spaces"
                "are not allowed!"
            )

    def test_class_uniqueness(self):
        n2 = has_n_factory(2)

        self.assertIs(
            n2,
            has_n_factory(2),
            msg="Repeatedly creating the same class should give the exact same class"
        )
        self.assertIsNot(
            n2,
            has_n_factory(2, "something_else"),
            msg="Sanity check"
        )

    def test_bad_factory_function(self):
        with self.assertRaises(
            InvalidFactorySignature,
            msg="For compliance with __reduce__, we can only use factory functions "
                "that strictly take positional arguments"
        ):
            ClassFactory(takes_kwargs)

    def test_instance_creation(self):
        foo = has_n_factory(2, "used")(42, y=43)
        self.assertEqual(
            2, foo.n, msg="Class attributes should be inherited"
        )
        self.assertEqual(
            "used", foo.s, msg="Class attributes should be inherited"
        )
        self.assertEqual(
            42, foo.x, msg="Initialized args should be captured"
        )
        self.assertEqual(
            43, foo.y, msg="Initialized kwargs should be captured"
        )
        self.assertIsInstance(
            foo,
            HasN,
            msg="Instances should inherit from the requested base(s)"
        )
        self.assertIsInstance(
            foo,
            _FactoryMade,
            msg="Instances should get :class:`_FactoryMade` mixed in."
        )

    def test_instance_pickle(self):
        foo = has_n_factory(2, "used")(42, y=43)
        reloaded = pickle.loads(pickle.dumps(foo))
        self.assertEqual(
            foo.n, reloaded.n, msg="Class attributes should be reloaded"
        )
        self.assertEqual(
            foo.s, reloaded.s, msg="Class attributes should be reloaded"
        )
        self.assertEqual(
            foo.x, reloaded.x, msg="Initialized args should be reloaded"
        )
        self.assertEqual(
            foo.y, reloaded.y, msg="Initialized kwargs should be reloaded"
        )
        self.assertIsInstance(
            reloaded,
            HasN,
            msg="Instances should inherit from the requested base(s)"
        )
        self.assertIsInstance(
            reloaded,
            _FactoryMade,
            msg="Instances should get :class:`_FactoryMade` mixed in."
        )

    def test_decorated_method(self):
        msg = "It should be possible to have class factories as methods on a class"
        foo = FactoryOwner().has_n_factory(2)(42, y=43)
        reloaded = pickle.loads(pickle.dumps(foo))
        self.assertEqual(foo.n, reloaded.n, msg=msg)
        self.assertEqual(foo.s, reloaded.s, msg=msg)
        self.assertEqual(foo.x, reloaded.x, msg=msg)
        self.assertEqual(foo.y, reloaded.y, msg=msg)

    def test_factory_inside_a_function(self):
        @classfactory
        def internal_factory(n, s="unimportable_scope", /):
            return (
                f"{HasN.__name__}{n}{s}",
                (HasN,),
                {},
                {"n": n, "s": s}
            )

        foo = internal_factory(2)(1, y=0)
        self.assertEqual(2, foo.n, msg="Nothing should stop the factory from working")
        self.assertEqual(
            "unimportable_scope",
            foo.s,
            msg="Nothing should stop the factory from working"
        )
        self.assertEqual(1, foo.x, msg="Nothing should stop the factory from working")
        self.assertEqual(0, foo.y, msg="Nothing should stop the factory from working")
        with self.assertRaises(
            AttributeError,
            msg="`internal_factory` is defined only locally inside the scope of "
                "another function, so we don't expect it to be pickleable whether it's "
                "a class factory or not!"
        ):
            pickle.dumps(foo)

        reloaded = cloudpickle.loads(cloudpickle.dumps(foo))
        self.assertTupleEqual(
            (foo.n, foo.s, foo.x, foo.y),
            (reloaded.n, reloaded.s, reloaded.x, reloaded.y),
            msg="Cloudpickle is powerful enough to overcome this <locals> limitation."
        )

    def test_repeated_inheritance(self):
        n2m3 = has_n2_m_factory(3)(5, 6)
        m3n2 = has_m_n2_factory(3)(5, 6)

        self.assertListEqual(
            [3, 2, "factory_made"],
            [n2m3.m, n2m3.n, n2m3.s],
            msg="Sanity check on class property inheritance"
        )
        self.assertListEqual(
            [3, 0, "foo"],  # n and s defaults from HasN!
            [m3n2.m, m3n2.n, m3n2.s],
            msg="When exploiting __init_subclass__, each subclass must take care to "
                "specify _all_ parent class __init_subclass__ kwargs, or they will "
                "revert to the default behaviour. This is totally normal python "
                "behaviour, and here we just verify that we're vulnerable to the same "
                "'gotcha' as the rest of the language."
        )
        self.assertListEqual(
            [5, 6],
            [n2m3.x, n2m3.z],
            msg="Sanity check on instance inheritance"
        )
        self.assertListEqual(
            [m3n2.z, m3n2.x],
            [n2m3.x, n2m3.z],
            msg="Inheritance order should impact arg order, also completely as usual "
                "for python classes"
        )
        reloaded_nm = pickle.loads(pickle.dumps(n2m3))
        self.assertListEqual(
            [n2m3.m, n2m3.n, n2m3.s, n2m3.z, n2m3.x, n2m3.y],
            [
                reloaded_nm.m,
                reloaded_nm.n,
                reloaded_nm.s,
                reloaded_nm.z,
                reloaded_nm.x,
                reloaded_nm.y
            ],
            msg="Pickling behaviour should not care that one of the parents was itself "
                "a factory made class."
        )

        reloaded_mn = pickle.loads(pickle.dumps(m3n2))
        self.assertListEqual(
            [m3n2.m, m3n2.n, m3n2.s, m3n2.z, m3n2.x, m3n2.y],
            [
                reloaded_mn.m,
                reloaded_mn.n,
                reloaded_mn.s,
                reloaded_mn.z,
                reloaded_mn.x,
                reloaded_nm.y
            ],
            msg="Pickling behaviour should not care about the order of bases."
        )

    def test_clearing_town(self):

        self.assertGreater(len(Has2._factory_town.factories), 0, msg="Sanity check")

        Has2._factory_town.clear()
        self.assertEqual(
            len(Has2._factory_town.factories),
            0,
            msg="Town should get cleared"
        )

        ClassFactory(undecorated_function)
        self.assertEqual(
            len(Has2._factory_town.factories),
            1,
            msg="Has2 exists in memory and the factory town has forgotten about it, "
                "but it still knows about the factory town and can see the newly "
                "created one."
        )

    def test_clearing_class_register(self):
        self.assertGreater(
            len(has_n_factory.class_registry),
            0,
            msg="Sanity. We expect to have created at least one class up in the header."
        )
        has_n_factory.clear()
        self.assertEqual(
            len(has_n_factory.class_registry),
            0,
            msg="Clear should remove all instances"
        )
        n_new = 3
        for i in range(n_new):
            has_n_factory(i)
        self.assertEqual(
            len(has_n_factory.class_registry),
            n_new,
            msg="Should see the new constructed classes"
        )

    def test_other_decorators(self):
        """
        In case the factory-produced class itself comes from a decorator, we need to
        check that name conflicts between the class and decorated function are handled.
        """
        a5 = adds_5_plus_x(2)
        self.assertIsInstance(a5, AddsNandX)
        self.assertIsInstance(a5, _FactoryMade)
        self.assertEqual(5, a5.n)
        self.assertEqual(2, a5.x)
        self.assertEqual(
            1 + 5 + 2,  # y + n=5 + x=2
            a5.add_to_function(1),
            msg="Should execute the function as part of call"
        )

        reloaded = pickle.loads(pickle.dumps(a5))
        self.assertEqual(a5.n, reloaded.n)
        self.assertIs(a5.fnc, reloaded.fnc)
        self.assertEqual(a5.x, reloaded.x)

    def test_other_decorators_inside_locals(self):
        @add_to_this_decorator(6)
        def adds_6_plus_x(y: int):
            return y

        a6 = adds_6_plus_x(42)
        self.assertEqual(
            1 + 42 + 6,
            a6.add_to_function(1),
            msg="Nothing stops us from creating and running these"
        )
        with self.assertRaises(
            AttributeError,
            msg="We can't find the <locals> function defined to import and recreate"
                "the factory"
        ):
            pickle.dumps(a6)

        reloaded = cloudpickle.loads(cloudpickle.dumps(a6))
        self.assertTupleEqual(
            (a6.n, a6.x),
            (reloaded.n, reloaded.x),
            msg="Cloudpickle is powerful enough to overcome this <locals> limitation."
        )


class TestSanitization(unittest.TestCase):

    def test_simple_string(self):
        self.assertEqual(sanitize_callable_name("SimpleString"), "SimpleString")

    def test_string_with_spaces(self):
        self.assertEqual(
            sanitize_callable_name("String with spaces"), "String_with_spaces"
        )

    def test_string_with_special_characters(self):
        self.assertEqual(sanitize_callable_name("a!@#$%b^&*()c"), "a_b_c")

    def test_string_with_numbers_at_start(self):
        self.assertEqual(sanitize_callable_name("123Class"), "_123Class")

    def test_empty_string(self):
        self.assertEqual(sanitize_callable_name(""), "")

    def test_string_with_only_special_characters(self):
        self.assertEqual(sanitize_callable_name("!@#$%"), "_")

    def test_string_with_only_numbers(self):
        self.assertEqual(sanitize_callable_name("123456"), "_123456")


if __name__ == '__main__':
    unittest.main()
