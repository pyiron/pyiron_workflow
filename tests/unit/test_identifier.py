import unittest

from pyiron_workflow.identifier import to_identifier


class TestToIdentifier(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(to_identifier("hello"), "hello")
        self.assertEqual(to_identifier("hello_world"), "hello_world")

    def test_starts_with_digit(self):
        self.assertEqual(to_identifier("123abc"), "_123abc")

    def test_special_characters(self):
        self.assertEqual(to_identifier("foo-bar!"), "foo_bar_")
        self.assertEqual(to_identifier("hello@world"), "hello_world")

    def test_keyword(self):
        self.assertEqual(to_identifier("class"), "class_")
        self.assertEqual(to_identifier("for"), "for_")

    def test_mixed_cases(self):
        self.assertEqual(to_identifier("123 foo-bar!"), "_123_foo_bar_")
        self.assertEqual(to_identifier("class!123"), "class_123")

    def test_only_special_chars(self):
        self.assertEqual(to_identifier("!!!"), "___")


if __name__ == "__main__":
    unittest.main()
