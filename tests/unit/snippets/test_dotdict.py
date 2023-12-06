import unittest

from pyiron_workflow.snippets.dotdict import DotDict


class TestDotDict(unittest.TestCase):
    def test_dot_dict(self):
        dd = DotDict({'foo': 42})

        self.assertEqual(dd['foo'], dd.foo, msg="Dot access should be equivalent.")
        dd.bar = "towel"
        self.assertEqual("towel", dd["bar"], msg="Dot assignment should be equivalent.")

        self.assertListEqual(dd.to_list(), [42, "towel"])


if __name__ == '__main__':
    unittest.main()
