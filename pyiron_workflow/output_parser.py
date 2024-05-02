"""
Inspects code to automatically parse return values as strings
"""

import ast
from functools import lru_cache
import inspect
import re
from textwrap import dedent


def _remove_spaces_until_character(string):
    pattern = r"\s+(?=\s)"
    modified_string = re.sub(pattern, "", string)
    return modified_string


class ParseOutput:
    """
    Given a function with at most one `return` expression, inspects the source code and
    parses a list of strings containing the returned values.
    If the function returns `None`, the parsed value is also `None`.
    This parsed value is evaluated at instantiation and stored in the `output`
    attribute.
    In case more than one `return` expression is found, a `ValueError` is raised.
    """

    def __init__(self, function):
        self._func = function
        self._output = self.get_parsed_output()

    @property
    def func(self):
        return self._func

    @property
    def dedented_source_string(self):
        return dedent(inspect.getsource(self.func))

    @property
    def node_return(self):
        tree = ast.parse(self.dedented_source_string)
        returns = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Return):
                returns.append(node)

        if len(returns) > 1:
            raise ValueError(
                f"{self.__class__.__name__} can only parse callables with at most one "
                f"return value, but ast.walk found {len(returns)}."
            )

        try:
            return returns[0]
        except IndexError:
            return None

    @property
    @lru_cache(maxsize=1)
    def source(self):
        return self.dedented_source_string.split("\n")[:-1]

    def get_string(self, node):
        string = ""
        for ll in range(node.lineno - 1, node.end_lineno):
            if ll == node.lineno - 1 == node.end_lineno - 1:
                string += _remove_spaces_until_character(
                    self.source[ll][node.col_offset : node.end_col_offset]
                )
            elif ll == node.lineno - 1:
                string += _remove_spaces_until_character(
                    self.source[ll][node.col_offset :]
                )
            elif ll == node.end_lineno - 1:
                string += _remove_spaces_until_character(
                    self.source[ll][: node.end_col_offset]
                )
            else:
                string += _remove_spaces_until_character(self.source[ll])
        return string

    @property
    def output(self):
        return self._output

    def get_parsed_output(self):
        if self.node_return is None or self.node_return.value is None:
            return
        elif isinstance(self.node_return.value, ast.Tuple):
            return [self.get_string(s) for s in self.node_return.value.dims]
        else:
            out = [self.get_string(self.node_return.value)]
            if out == ["None"]:
                return
            else:
                return out
