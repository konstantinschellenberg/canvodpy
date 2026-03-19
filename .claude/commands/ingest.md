Guide the user through ingesting GNSS data files into an Icechunk store. Follow this checklist:

## Pre-flight

1. **Config check:** Verify `config/processing.yaml` and `config/sites.yaml` exist (these are user-specific, never committed)
2. **Naming validation:** Run `DataDirectoryValidator` on the data directory — unmatched or overlapping files block processing
3. **Store exists?** Check if the target Icechunk store already exists. If yes, warn about the three-layer dedup guardrails

## Ingest options (by API level)

**L1 (simplest):**
```python
from canvodpy import Site
site = Site("rosa")
site.process_date("2025-001")
```

**L2 (customizable):**
```python
from canvodpy import FluentWorkflow
result = FluentWorkflow(config).read().augment().grid().vod().store().run()
```

**L4 (Airflow/scripting):**
```python
from canvodpy.functional import read_rinex, augment_with_ephemeris, calculate_vod_to_file
```

## Post-ingest checks

- Verify commit succeeded (session becomes read-only after commit)
- Check metadata table: `store.read_metadata_table(group)`
- Verify file count matches expectation
- Check `source_format` root attr is set correctly

## Common issues

- **Overwrite strategy is broken** — do not use `_prepare_store_for_overwrite()` (known Dask serialization bug)
- Factory API: use `fpath=` (not `path=`), `.to_ds()` (not `.read()`)
- SBF metadata concat can be slow for large batches
