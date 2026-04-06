# canvod-virtualiconvname

Canonical GNSS-T filename parser and data directory validator.

Part of the [canVODpy](https://github.com/nfb2021/canvodpy) ecosystem.

## Overview

`canvod-virtualiconvname` enforces and virtualizes the canVOD filename convention
throughout the processing pipeline. It is the single source of truth for GNSS file
naming, discovery, and pre-pipeline validation.

**Convention format:**
```
{SIT}{T}{NN}{AGC}_R_{YYYY}{DOY}{HHMM}_{PERIOD}_{SAMPLING}_{CONTENT}.{TYPE}
```
Example: `ROSA01TUW_R_20250010000_15M_05S_AA.rnx`

## Key components

| Component | Purpose |
|---|---|
| `CanVODFilename` | Pydantic model — parses and validates a single filename |
| `FilenameMapper` | Maps physical filenames to canonical names (virtual renaming) |
| `DataDirectoryValidator` | Pre-pipeline hard gate: blocks on unmatched or overlapping files |
| `FilenameCatalog` | DuckDB-backed catalog for file discovery |
| `BUILTIN_PATTERNS` | Glob patterns for all GNSS file types (single source of truth) |

## Installation

```bash
uv pip install canvod-virtualiconvname
```

## Quick Start

```python
from canvod.virtualiconvname import CanVODFilename, DataDirectoryValidator

# Parse a filename
fname = CanVODFilename.from_string("ROSA01TUW_R_20250010000_15M_05S_AA.rnx")
print(fname.site, fname.year, fname.doy)  # ROSA, 2025, 1

# Validate a data directory before processing
validator = DataDirectoryValidator(site_config)
validator.validate()  # raises on unmatched or overlapping files
```

## Documentation

[Full documentation](https://nfb2021.github.io/canvodpy/packages/naming/overview/)

## License

Apache License 2.0
