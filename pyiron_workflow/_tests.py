"""
Tools specifically for the test suite, not intended for general use.
"""

from pathlib import Path
import sys


def ensure_tests_in_python_path():
    """So that you can import from the static module"""
    path_to_tests = Path(__file__).parent.parent / "tests"
    as_string = str(path_to_tests.resolve())

    if as_string not in sys.path:
        sys.path.append(as_string)
