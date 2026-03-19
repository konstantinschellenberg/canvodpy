# canvod-store-metadata

Rich metadata schema for Icechunk stores (DataCite 4.5, ACDD 1.3, STAC 1.1).

## Key modules

| Module | Purpose |
|---|---|
| `schemas.py` | `StoreMetadata` root + 15 section models (~90 fields) |
| `collectors.py` | `collect_metadata()` — auto-populates from config, dataset, environment |
| `io.py` | `write_metadata()`, `read_metadata()`, `update_metadata()`, `metadata_exists()` |
| `validators.py` | `validate_all()` — schema + cross-field validation |
| `inventory.py` | `scan_stores()` — inventory across multiple stores |

## Store integration

Metadata stored as root Zarr attr `canvod_metadata`. Written on first ingest
(STEP 5b in orchestrator), timestamp updated on subsequent ingests.

## Important

Module is `canvod.store_metadata` (NOT `canvod.store.metadata` — that would
conflict with the existing `metadata.py` in canvod-store).

## Testing

```bash
uv run pytest packages/canvod-store-metadata/tests/
```
