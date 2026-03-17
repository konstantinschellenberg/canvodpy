"""
Compatibility wrapper for storage utilities using the polars-backed HemiGrid.

Historically the storage helpers lived in this module. They now reside in
`complete_grid_vod_workflow.py`; this file re-exports the public surface to
avoid breaking existing imports.
"""

from __future__ import annotations

from .complete_grid_vod_workflow import (
    HemiGridStorageAdapter,
    StoredHemiGrid,
    list_available_grids,
    load_grid_from_vod_store,
    store_grid_to_vod_store,
)

__all__ = [
    "HemiGridStorageAdapter",
    "StoredHemiGrid",
    "list_available_grids",
    "load_grid_from_vod_store",
    "store_grid_to_vod_store",
]

# Backwards compatibility: previously the storage helper was exposed as a mixin.
# Retain the name so existing imports continue to work, while encouraging
# composition for new code.
HemiGridStorageMixin = HemiGridStorageAdapter
