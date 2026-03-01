"""Hashing utilities for canVODpy."""

import hashlib
from pathlib import Path


def file_hash(path: Path, chunk_size: int = 8192) -> str:
    """
    Compute SHA256 hash of a GNSS data file's content.

    Uses first 16 characters of the SHA256 hexdigest for a compact hash.

    Parameters
    ----------
    path : Path
        Path to GNSS data file.
    chunk_size : int, optional
        Size of chunks to read (default: 8192 bytes).

    Returns
    -------
    str
        First 16 characters of SHA256 hash.

    Examples
    --------
    >>> from pathlib import Path
    >>> hash_val = file_hash(Path("data.rnx"))
    >>> len(hash_val)
    16
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    digest = h.hexdigest()[:16]
    return digest
