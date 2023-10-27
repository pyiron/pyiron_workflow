from pathlib import Path
import sys
from unittest import TestCase, skipUnless


from pyiron_workflow.interfaces import Creator


@skipUnless(
    sys.version_info[0] == 3 and sys.version_info[1] >= 10, "Only supported for 3.10+"
)
class TestCreator(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.creator = Creator()
        path_to_tests = Path(__file__).parent.parent
        sys.path.append(str(path_to_tests.resolve()))
        # Now we can import from `static`

    def test_registration(self):

        with self.assertRaises(
            AttributeError,
            msg="Sanity check that the package isn't there yet and the test setup is "
                "what we want"
        ):
            self.creator.demo_nodes

        self.creator.register("demo", "static.demo_nodes")

        node = self.creator.demo.Add(1, 2)
        self.assertEqual(
            3,
            node(),
            msg="Node should get instantiated from creator and be operable"
        )
