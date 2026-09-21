import math
import unittest

from pyiron_workflow.compatibility import output_parser


class TestParseOutput(unittest.TestCase):
    def test_parsing(self):
        with self.subTest("Single return"):

            def identity(x):
                return x

            self.assertListEqual(output_parser.ParseOutput(identity).output, ["x"])

        with self.subTest("Expression return"):

            def add(x, y):
                return x + y

            self.assertListEqual(output_parser.ParseOutput(add).output, ["x + y"])

        with self.subTest("Weird whitespace"):

            def add_with_whitespace(x, y):
                return x + y

            self.assertListEqual(
                output_parser.ParseOutput(add_with_whitespace).output, ["x + y"]
            )

        with self.subTest("Multiple expressions"):

            def add_and_subtract(x, y):
                return x + y, x - y

            self.assertListEqual(
                output_parser.ParseOutput(add_and_subtract).output, ["x + y", "x - y"]
            )

        with self.subTest("Best-practice (well-named return vars)"):

            def md(job):
                temperature = job.output.temperature
                energy = job.output.energy
                return temperature, energy

            self.assertListEqual(
                output_parser.ParseOutput(md).output, ["temperature", "energy"]
            )

        with self.subTest("Function call returns"):

            def function_return(i, j):
                return (math.log(10, base=2), math.atan2(1, 2))

            self.assertListEqual(
                output_parser.ParseOutput(function_return).output,
                ["math.log(10, base=2)", "math.atan2(1, 2)"],
            )

        with self.subTest("Methods too"):

            class Foo:
                def add(self, x, y):
                    return x + y

            self.assertListEqual(output_parser.ParseOutput(Foo.add).output, ["x + y"])

    def test_void(self):
        with self.subTest("No return"):

            def no_return():
                pass

            self.assertIsNone(output_parser.ParseOutput(no_return).output)

        with self.subTest("Empty return"):

            def empty_return():
                return

            self.assertIsNone(output_parser.ParseOutput(empty_return).output)

        with self.subTest("Return None explicitly"):

            def none_return():
                return None

            self.assertIsNone(output_parser.ParseOutput(none_return).output)

    def test_multiple_branches(self):
        def bifurcating(x):
            if x > 5:  # noqa: SIM103
                return True
            else:
                return False

        with self.assertRaises(ValueError):
            output_parser.ParseOutput(bifurcating)


if __name__ == "__main__":
    unittest.main()
