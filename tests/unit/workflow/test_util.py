from unittest import TestCase

import pyiron_contrib.workflow.util as util


class TestUtil(TestCase):
    def test_dot_dict(self):
        dd = util.DotDict({'foo': 42})

        self.assertEqual(dd['foo'], dd.foo, msg="Dot access should be equivalent.")
        dd.bar = "towel"
        self.assertEqual("towel", dd["bar"], msg="Dot assignment should be equivalent.")