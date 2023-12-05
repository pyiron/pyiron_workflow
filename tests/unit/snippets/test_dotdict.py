from unittest import TestCase

from pyiron_workflow.snippets.dotdict import DotDict


class TestDotDict(TestCase):
    def test_dot_dict(self):
        dd = DotDict({'foo': 42})

        self.assertEqual(dd['foo'], dd.foo, msg="Dot access should be equivalent.")
        dd.bar = "towel"
        self.assertEqual("towel", dd["bar"], msg="Dot assignment should be equivalent.")

        self.assertListEqual(dd.to_list(), [42, "towel"])
