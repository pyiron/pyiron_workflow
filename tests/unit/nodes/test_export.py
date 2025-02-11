import unittest

from pyiron_workflow._tests import ensure_tests_in_python_path
from pyiron_workflow.nodes.function import as_function_node
from pyiron_workflow.nodes.macro import as_macro_node
from pyiron_workflow.workflow import Workflow

ensure_tests_in_python_path()


@Workflow.wrap.as_function_node
def add_one(a: int):
    result = a + 1
    return result

@Workflow.wrap.as_function_node
def add_two(b: int = 10) -> int:
    result = b + 2
    return result

@Workflow.wrap.as_macro_node
def add_three(macro, c: int) -> int:
    macro.one = add_one(a=c)
    macro.two = add_two(b=macro.one)
    w = macro.two
    return w


class TestExport(unittest.TestCase):
    def test_io_independence(self):
        wf = Workflow("my_wf")
        wf.three = add_three(c=1)
        wf.four = add_one(a=wf.three)
        wf.run()
        data = wf.export_to_dict()
        self.assertEqual(
            set(data.keys()), {"edges", "inputs", "nodes", "outputs"}
        )
        self.assertEqual(
            data["inputs"], {'three__c': {'value': 1, 'type_hint': int}}
        )


if __name__ == "__main__":
    unittest.main()
