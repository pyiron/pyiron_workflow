from unittest import TestCase, skipUnless
from sys import version_info

import pyiron_workflow.util as util


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestUtil(TestCase):
    def test_dot_dict(self):
        dd = util.DotDict({'foo': 42})

        self.assertEqual(dd['foo'], dd.foo, msg="Dot access should be equivalent.")
        dd.bar = "towel"
        self.assertEqual("towel", dd["bar"], msg="Dot assignment should be equivalent.")

        self.assertListEqual(dd.to_list(), [42, "towel"])

    def test_has_post_metaclass(self):
        class Foo(metaclass=util.HasPost):
            def __init__(self, x=0):
                self.x = x
                self.y = x
                self.z = x
                self.x += 1

            @property
            def data(self):
                return self.x, self.y, self.z

        class Bar(Foo):
            def __init__(self, x=0, extra=1):
                super().__init__(x)

            def __post__(self, *args, extra=1, **kwargs):
                self.z = self.x + extra

        self.assertTupleEqual(
            (1, 0, 0),
            Foo().data,
            msg="It should be fine to have this metaclass but not define post"
        )

        self.assertTupleEqual(
            (1, 0, 2),
            Bar().data,
            msg="Metaclass should be inherited, able to use input, and happen _after_ "
                "__init__"
        )
