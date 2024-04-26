from abc import ABC
import pickle
import unittest

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
        super(HasN, cls).__init_subclass__(**kwargs)
        cls.n = n
        cls.s = s

    def __init__(self, x, y=0):
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

        foo = internal_factory(2)(1, 0)
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
            pickle.loads(pickle.dumps(foo))


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
