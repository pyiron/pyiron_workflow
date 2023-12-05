import unittest

import pyiron_workflow.snippets.has_post


class TestHasPost(unittest.TestCase):
    def test_has_post_metaclass(self):
        class Foo(metaclass=pyiron_workflow.snippets.has_post.HasPost):
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


if __name__ == '__main__':
    unittest.main()
