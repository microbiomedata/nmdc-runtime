import re


def get_typecode_from_id(id_: str) -> str | None:
    """
    Helper function that returns the typecode portion of the specified ID.

    >>> get_typecode_from_id("0") is None
    True
    >>> get_typecode_from_id("nmdc:-1") is None
    True
    >>> get_typecode_from_id("nmdc:foo-bar-11-1")
    'foo'
    >>> get_typecode_from_id("nmdc:sty-11-1234")
    'sty'
    """
    pattern = re.compile(r"^nmdc:(\w+)?-")
    match = pattern.search(id_)
    typecode_portion = match.group(1) if match else None
    return typecode_portion
