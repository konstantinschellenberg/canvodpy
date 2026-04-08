"""Utility functions for RINEX readers."""

import hashlib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def get_version_from_pyproject(pyproject_path: Path | None = None) -> str:
    """Get the installed version of canvod-readers.

    Parameters
    ----------
    pyproject_path : Path, optional
        Ignored. Kept for backwards compatibility.

    Returns
    -------
    str
        Version string from installed package metadata, or ``"unknown"``
        if the package is not installed in the active environment.

    """
    try:
        return version("canvod-readers")
    except PackageNotFoundError:
        return "unknown"


def file_hash(path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA256 hash of a GNSS data file's content.

    Used by MyIcechunkStore for deduplication - ensures same file
    isn't ingested multiple times.

    Parameters
    ----------
    path : Path
        Path to GNSS data file.
    chunk_size : int, optional
        Chunk size for reading file in bytes. Default is 8192.

    Returns
    -------
    str
        First 16 characters of SHA256 hex digest.

    Examples
    --------
    >>> from pathlib import Path
    >>> hash1 = file_hash(Path("station.24o"))
    >>> hash2 = file_hash(Path("station.24o"))
    >>> hash1 == hash2
    True

    """
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)

    return h.hexdigest()[:16]


def isfloat(value: str) -> bool:
    """Check if a string value can be converted to float.

    Parameters
    ----------
    value : str
        String to check for float convertibility.

    Returns
    -------
    bool
        True if convertible to float, False otherwise.

    Examples
    --------
    >>> isfloat("3.14")
    True
    >>> isfloat("not_a_number")
    False
    >>> isfloat("-2.5")
    True

    """
    try:
        float(value)
        return True
    except ValueError:
        return False
