# canvod-virtualiconvname

Filename convention virtualization — single source of truth for GNSS file naming and discovery.

## Key modules

| Module | Purpose |
|---|---|
| `convention.py` | `CanVODFilename` — parses `{SIT}{T}{NN}{AGC}_R_{YYYY}{DOY}{HHMM}_{PERIOD}_{SAMPLING}_{CONTENT}.{TYPE}` |
| `mapping.py` | `FilenameMapper` — physical filenames → canonical names |
| `validator.py` | `DataDirectoryValidator` — pre-flight hard gate (unmatched/overlapping = blocked) |
| `catalog.py` | `FilenameCatalog` — DuckDB-backed file discovery |
| `patterns.py` | `BUILTIN_PATTERNS` — glob patterns for all GNSS file types |
| `config_models.py` | `SiteNamingConfig`, `ReceiverNamingConfig` (Pydantic) |
| `recipe.py` | `NamingRecipe` — config generation |

## Convention format

`{SIT}{T}{NN}{AGC}_R_{YYYY}{DOY}{HHMM}_{PERIOD}_{SAMPLING}_{CONTENT}.{TYPE}`

Example: `ROSA01TUW_R_20250010000_15M_05S_AA.rnx`

## Validation

`DataDirectoryValidator` is the pre-pipeline hard gate. If files don't match the
convention or have temporal overlaps, processing is blocked. This runs before any
data is read.

## Important

- `DataDirMatcher` and `PairDataDirMatcher` in canvod-readers are **deprecated** — use this package
- `BUILTIN_PATTERNS` is the single source of truth for file glob patterns
- Test data files use canonical names

## Testing

```bash
uv run pytest packages/canvod-virtualiconvname/tests/
```
