import unittest
from pyiron_workflow.snippets.singleton import Singleton


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


if __name__ == '__main__':
    unittest.main()

