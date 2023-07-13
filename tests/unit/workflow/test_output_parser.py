from sys import version_info
import unittest

import numpy as np

from pyiron_contrib.workflow.output_parser import ParseOutput


@unittest.skipUnless(
    version_info[0] == 3 and version_info[1] >= 10, "Only supported for 3.10+"
)
class TestParseOutput(unittest.TestCase):
    def test_parsing(self):
        with self.subTest("Single return"):
            def identity(x):
                return x
            self.assertListEqual(ParseOutput(identity).output, ["x"])

        with self.subTest("Expression return"):
            def add(x, y):
                return x + y
            self.assertListEqual(ParseOutput(add).output, ["x + y"])

        with self.subTest("Weird whitespace"):
            def add(x, y):
                return   x  +  y
            self.assertListEqual(ParseOutput(add).output, ["x + y"])

        with self.subTest("Multiple expressions"):
            def add_and_subtract(x, y):
                return x + y, x - y
            self.assertListEqual(
                ParseOutput(add_and_subtract).output,
                ["x + y", "x - y"]
            )

        with self.subTest("Best-practice (well-named return vars)"):
            def md(job):
                temperature = job.output.temperature
                energy = job.output.energy
                return temperature, energy
            self.assertListEqual(ParseOutput(md).output, ["temperature", "energy"])

        with self.subTest("Function call returns"):
            def function_return(i, j):
                return (
                    np.arange(
                        i, dtype=int
                    ),
                    np.shape(i, j)
                )
            self.assertListEqual(
                ParseOutput(function_return).output,
                ["np.arange( i, dtype=int )", "np.shape(i, j)"]
            )

        with self.subTest("Methods too"):
            class Foo:
                def add(self, x, y):
                    return x + y
            self.assertListEqual(ParseOutput(Foo.add).output, ["x + y"])

    def test_void(self):
        with self.subTest("No return"):
            def no_return():
                pass
            self.assertIsNone(ParseOutput(no_return).output)

        with self.subTest("Empty return"):
            def empty_return():
                return
            self.assertIsNone(ParseOutput(empty_return).output)

        with self.subTest("Return None explicitly"):
            def none_return():
                return None
            self.assertIsNone(ParseOutput(none_return).output)

    def test_multiple_branches(self):
        def bifurcating(x):
            if x > 5:
                return True
            else:
                return False
        with self.assertRaises(ValueError):
            ParseOutput(bifurcating).output


if __name__ == '__main__':
    unittest.main()
