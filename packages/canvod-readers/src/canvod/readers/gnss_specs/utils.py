"""Utility functions for RINEX readers."""

import hashlib
import tomllib
from pathlib import Path


def get_version_from_pyproject(pyproject_path: Path | None = None) -> str:
    """Get package version from pyproject.toml.

    Parameters
    ----------
    pyproject_path : Path, optional
        Path to pyproject.toml. If None, auto-detects by traversing up
        from current file location.

    Returns
    -------
    str
        Version string from project metadata.

    Raises
    ------
    FileNotFoundError
        If pyproject.toml cannot be found.
    KeyError
        If version field is missing from pyproject.toml.

    """
    if pyproject_path is None:
        # Auto-find pyproject.toml (3 levels up from this file)
        pyproject_path = Path(__file__).resolve().parents[4] / "pyproject.toml"

    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)

    return data["project"]["version"]


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
