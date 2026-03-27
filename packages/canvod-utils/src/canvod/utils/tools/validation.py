"""Validation utilities for canVODpy."""

from typing import Any


def isfloat(value: Any) -> bool:
    """
    Check if a variable can be converted to float.

    Parameters
    ----------
    value : Any
        Value to check.

    Returns
    -------
    bool
        True if value can be converted to float, False otherwise.

    Examples
    --------
    >>> isfloat("3.14")
    True
    >>> isfloat("hello")
    False
    >>> isfloat(42)
    True
    """
    try:
        float(value)
        return True
    except ValueError, TypeError:
        return False
