def good():
    """
    >>> print(42)
    42
    """
    return


def bad():
    """
    >>> print(42)
    Not the right output
    """
    return


def error():
    """
    >>> 1/0
    """


class Documented:
    """
    >>> print(42)
    42
    """

    def ok(self):
        """
        >>> print(42)
        42
        """

    def bad(self):
        """
        >>> print(42)
        Not the right output
        """

    def error(self):
        """
        >>> 1/0
        """
