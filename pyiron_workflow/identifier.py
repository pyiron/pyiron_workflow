import keyword
import re


def to_identifier(s: str) -> str:
    # Replace invalid chars and ensure it doesn't start with a digit
    s = re.sub(r"\W|^(?=\d)", "_", s)
    # Append underscore if it's a keyword
    if keyword.iskeyword(s):
        s += "_"
    return s
