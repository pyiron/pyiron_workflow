from unittest import TestCase, skipUnless
from sys import version_info

import pyiron_contrib.workflow.util as util


@skipUnless(version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+")
class TestUtil(TestCase):
    def test_dot_dict(self):
        dd = util.DotDict({'foo': 42})

        self.assertEqual(dd['foo'], dd.foo, msg="Dot access should be equivalent.")
        dd.bar = "towel"
        self.assertEqual("towel", dd["bar"], msg="Dot assignment should be equivalent.")