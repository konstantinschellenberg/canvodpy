"""Read/write metadata to/from Icechunk store root attrs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import icechunk
import zarr

from .schema import _METADATA_KEY, StoreMetadata


def _open_repo(store_path: Path) -> icechunk.Repository:
    """Open an existing Icechunk repository."""
    storage = icechunk.local_filesystem_storage(str(store_path))
    return icechunk.Repository.open(storage=storage)


def write_metadata(
    store_path: Path,
    metadata: StoreMetadata,
    branch: str = "main",
) -> str:
    """Write metadata to Icechunk store root attrs.

    Returns
    -------
    str
        Snapshot ID from the commit.
    """
    repo = _open_repo(store_path)
    session = repo.writable_session(branch)
    store = session.store
    try:
        root = zarr.open_group(store, mode="r+")
    except zarr.errors.GroupNotFoundError:
        root = zarr.open_group(store, mode="w")

    root.attrs.update(metadata.to_root_attrs())
    return session.commit("Write store metadata")


def read_metadata(
    store_path: Path,
    branch: str = "main",
) -> StoreMetadata:
    """Read metadata from Icechunk store root attrs.

    Raises
    ------
    KeyError
        If no metadata found in store.
    """
    repo = _open_repo(store_path)
    session = repo.readonly_session(branch=branch)
    store = session.store
    root = zarr.open_group(store, mode="r")
    attrs = dict(root.attrs)
    return StoreMetadata.from_root_attrs(attrs)


def update_metadata(
    store_path: Path,
    updates: dict[str, Any],
    branch: str = "main",
) -> str:
    """Merge updates into existing metadata.

    Supports dotted keys for nested updates (e.g. "temporal.updated").

    Returns
    -------
    str
        Snapshot ID from the commit.
    """
    existing = read_metadata(store_path, branch)
    data = existing.model_dump(mode="json")

    for key, value in updates.items():
        parts = key.split(".")
        target = data
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value

    updated = StoreMetadata.model_validate(data)

    repo = _open_repo(store_path)
    session = repo.writable_session(branch)
    store = session.store
    try:
        root = zarr.open_group(store, mode="r+")
    except zarr.errors.GroupNotFoundError:
        root = zarr.open_group(store, mode="w")

    root.attrs.update(updated.to_root_attrs())
    return session.commit("Update store metadata")


def metadata_exists(store_path: Path, branch: str = "main") -> bool:
    """Check if metadata exists in the store."""
    try:
        repo = _open_repo(store_path)
        session = repo.readonly_session(branch=branch)
        store = session.store
        root = zarr.open_group(store, mode="r")
        return _METADATA_KEY in root.attrs
    except Exception:
        return False
