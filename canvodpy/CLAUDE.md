# canvodpy (orchestrator)

Main application package — orchestrates the full GNSS → VOD pipeline.

## Key modules

| Module | Purpose |
|---|---|
| `orchestrator/processor.py` | `RinexDataProcessor` — main pipeline (~2800 lines) |
| `orchestrator/pipeline.py` | `PipelineOrchestrator` — coordination |
| `api.py` | L1 convenience API: `Site`, `Pipeline`, `process_date()` |
| `fluent.py` | L2 fluent API: `FluentWorkflow` (deferred execution chain) |
| `functional.py` | L4 functional API: `read_rinex()`, `augment_with_ephemeris()`, etc. |
| `vod_computer.py` | `VodComputer` — `compute_day()` (inline) + `compute_bulk()` (from store) |
| `factories.py` | `ReaderFactory`, `GridFactory`, `VODFactory`, `AugmentationFactory` |
| `workflows/` | Task definitions, `validate_data_dirs()` pre-flight check |
| `orchestrator/resources.py` | `MemoryMonitor`, `DaskClusterManager` |

## API levels

| Level | Style | Entry point | Use case |
|---|---|---|---|
| L1 | Convenience | `Site("rosa").process_date("2025-001")` | Interactive / notebooks |
| L2 | Fluent | `FluentWorkflow(config).read().augment().grid().vod().run()` | Customizable pipelines |
| L3 | Low-level | `RinexDataProcessor(config).process()` | Full control |
| L4 | Functional | `read_rinex(path) \| augment() \| grid() \| vod()` | Airflow / orchestrators |

## Processing flow

```
Files → DataDirectoryValidator → GNSSDataReader → AuxDataAugmenter → GridAssignment → VODCalculator → MyIcechunkStore
```

## Important patterns

- `FluentWorkflow.read()` uses `FilenameMapper` when naming config is available
- Receiver position from RINEX header via `ECEFPosition.from_ds_metadata(ds)`
- Factory API: `fpath=` (not `path=`), `.to_ds()` (not `.read()`)
- `vod_analyses` returns `dict[str, VodAnalysisConfig]` (Pydantic models, attribute access)
- `VodComputer` accessible via `site.vod`

## Store integration

`_append_to_icechunk()` in processor.py:
1. Three-layer dedup check
2. `append_to_group()` write
3. Commit
4. SBF metadata concat + write (STEP 5a)
5. Rich metadata write/update (STEP 5b)

## Testing

```bash
uv run pytest canvodpy/tests/
```
