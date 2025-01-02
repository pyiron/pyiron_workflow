import unittest
from pathlib import Path

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.find import find_nodes


class TestFind(unittest.TestCase):
    def test_find_nodes(self):
        """
        We compare names instead of direct `is` comparisons with the imported objects
        because the class factories are being forced to create new classes on repeated
        import, i.e. we don't leverage classfactory's ability to make the dynamic
        classes be the same object.
        This is because users might _intentionally_ be re-calling the factories, e.g.
        with new output labels, and we then _want_ new classes to get generated.
        There is probably a workaround that lets us have our cake and eat it to (i.e.
        only generate new classes when they are strictly needed), but we don't have it
        now.
        """
        demo_nodes_file = str(
            Path(__file__).parent.joinpath("..", "static", "demo_nodes.py").resolve()
        )
        found_by_string = find_nodes(demo_nodes_file)
        path = Path(demo_nodes_file)
        found_by_path = find_nodes(path)

        ensure_tests_in_python_path()
        from static import demo_nodes

        found_by_module = find_nodes(demo_nodes)

        self.assertListEqual(
            [o.__name__ for o in found_by_path],
            [o.__name__ for o in found_by_string],
            msg=f"You should find the same thing regardless of source representation;"
                f"by path got {found_by_path} and by string got {found_by_string}",
        )
        self.assertListEqual(
            [o.__name__ for o in found_by_string],
            [o.__name__ for o in found_by_module],
            msg=f"You should find the same thing regardless of source representation;"
                f"by string got {found_by_string} and by module got {found_by_module}",
        )
        self.assertListEqual(
            [o.__name__ for o in found_by_string],
            [
                demo_nodes.AddPlusOne.__name__,
                demo_nodes.AddThree.__name__,
                demo_nodes.Dynamic.__name__,
                demo_nodes.OptionallyAdd.__name__,
            ],
            msg=f"Should match a hand-selected expectation list that ignores the "
                f"private and non-local nodes. If you update the demo nodes this may "
                f"fail and need to be trivially updated. Got {found_by_module}",
        )


if __name__ == "__main__":
    unittest.main()
