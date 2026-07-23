import unittest

from pyiron_workflow.overloading import (
    overloaded_classmethod,
)  # replace with actual module path


def class_string(obj, x):
    return f"Class {obj.__name__} doing {x}"


def instance_string(obj, x):
    return f"Instance of type {type(obj).__name__} doing {x}"


class Foo:
    @overloaded_classmethod(class_method="_pseudo_classmethod")
    def undecorated_string(self, x):
        return instance_string(self, x)

    @overloaded_classmethod(class_method="_classmethod")
    def decorated_string(self, x):
        return instance_string(self, x)

    def _pseudo_classmethod(cls, x):
        return class_string(cls, x)

    @classmethod
    def _classmethod(cls, y):
        return class_string(cls, y)

    @overloaded_classmethod(class_method=_pseudo_classmethod)
    def undecorated_direct(self, x):
        return instance_string(self, x)

    @overloaded_classmethod(class_method=_classmethod)
    def decorated_direct(self, x):
        return instance_string(self, x)


class TestOverloadedClassMethod(unittest.TestCase):
    def test_instance_and_class_calls(self):
        self.assertEqual(Foo.undecorated_string(1), class_string(Foo, 1))
        self.assertEqual(Foo.decorated_string(2), class_string(Foo, 2))
        self.assertEqual(Foo.undecorated_direct(3), class_string(Foo, 3))
        self.assertEqual(Foo.decorated_direct(4), class_string(Foo, 4))

        self.assertEqual(Foo().undecorated_string(1), instance_string(Foo(), 1))
        self.assertEqual(Foo().decorated_string(2), instance_string(Foo(), 2))
        self.assertEqual(Foo().undecorated_direct(3), instance_string(Foo(), 3))
        self.assertEqual(Foo().decorated_direct(4), instance_string(Foo(), 4))


if __name__ == "__main__":
    unittest.main()
