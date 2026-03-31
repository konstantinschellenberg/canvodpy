# canVODpy

<!-- Build & Quality -->
[![Platform Tests](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml/badge.svg)](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml)
[![Code Coverage](https://github.com/nfb2021/canvodpy/actions/workflows/test_coverage.yml/badge.svg)](https://github.com/nfb2021/canvodpy/actions/workflows/test_coverage.yml)
[![FAIR Software Check](https://github.com/nfb2021/canvodpy/actions/workflows/fair-software.yml/badge.svg)](https://github.com/nfb2021/canvodpy/actions/workflows/fair-software.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

<!-- Security -->
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12329/badge)](https://www.bestpractices.dev/projects/12329)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/nfb2021/canvodpy/badge)](https://securityscorecards.dev/viewer/?uri=github.com/nfb2021/canvodpy)

<!-- Python & Platforms -->
[![Python](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/)
[![Platforms](https://img.shields.io/badge/platform-Linux%20|%20macOS%20|%20Windows-lightgrey)](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

<!-- Tech Stack -->
[![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)
[![Icechunk](https://img.shields.io/badge/Icechunk-Storage-00A3E0)](https://icechunk.io/)
[![Dask](https://img.shields.io/badge/Dask-Parallel-FDA061?logo=dask&logoColor=white)](https://www.dask.org/)

<!-- Standards & Compliance -->
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18636775.svg)](https://doi.org/10.5281/zenodo.18636775)
[![fair-software.eu](https://img.shields.io/badge/fair--software.eu-%E2%97%8B%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F-yellow)](https://fair-software.eu)
[![DataCite 4.5](https://img.shields.io/badge/DataCite-4.5-3F51B5)](https://schema.datacite.org/)
[![ACDD 1.3](https://img.shields.io/badge/ACDD-1.3-4CAF50)](https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-3)
[![STAC 1.1](https://img.shields.io/badge/STAC-1.1-FF9800)](https://stacspec.org/)
[![REUSE](https://img.shields.io/badge/REUSE-3.3-blue)](https://reuse.software/)

<!-- AI & Development -->
[![Claude Code](https://img.shields.io/badge/Claude_Code-AI_Assisted-cc785c?logo=anthropic&logoColor=white)](https://claude.com/claude-code)

<!-- Project -->
[![CLIMERS @ TU Wien](https://img.shields.io/badge/CLIMERS-TU_Wien-006699)](https://www.tuwien.at/en/mg/geo/climers)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An open Python ecosystem for GNSS-Transmissometry (GNSS-T) canopy VOD retrievals. canVODpy is a community-driven software suite for deriving and analyzing Vegetation Optical Depth from GNSS signal-to-noise ratio observations.

> [!CAUTION]
> **Confidential — Pre-release Software.**
> This software is shared under restricted access. Distribution, redistribution, or publication of this code or any derived results is **not permitted** without explicit written authorization from the author (Nicolas F. Bader, nicolas.bader@tuwien.ac.at). Access is granted solely to individuals authorized by the author.

> [!IMPORTANT]
> This project is in **beta**. Development requires `uv` and `just`:
> - Install `uv`: [uv documentation](https://docs.astral.sh/uv/getting-started/installation/)
> - Install `just`: [just documentation](https://github.com/casey/just)

## Overview

canVODpy is organized as a monorepo with independent, composable packages:

| Package | Description |
|---|---|
| **canvod-readers** | RINEX v3.04 and SBF binary file readers |
| **canvod-auxiliary** | SP3/CLK ephemeris download, interpolation, coordinate transforms |
| **canvod-grids** | 7 hemispheric grid types (equal-area, geodesic, HTM, ...) |
| **canvod-vod** | Tau-omega VOD retrieval algorithms |
| **canvod-store** | Versioned storage via Icechunk (Zarr v3) |
| **canvod-store-metadata** | Store-level provenance (DataCite, ACDD, STAC) |
| **canvod-viz** | Hemispheric and time-series visualisation |
| **canvod-ops** | Configurable preprocessing pipeline |
| **canvod-utils** | Configuration, date utilities, shared tooling |
| **canvod-virtualiconvname** | Filename mapping and pre-flight validation |
| **canvod-audit** | Three-tier verification suite (internal consistency, regression, vs gnssvod) |
| **canvodpy** | Umbrella package — 4 API levels, factory system, orchestrator |

## Installation

```bash
uv add canvodpy

# Or install specific components
uv add canvod-readers canvod-grids
```

## Quick Start

```python
from canvodpy import process_date, calculate_vod

# Process one day: read + augment + write to Icechunk store
process_date("Rosalia", "2025001")

# Compute VOD from stored data
vod = calculate_vod("Rosalia", "canopy_01", "reference_01", "2025001")
```

Four API levels are available — from one-liners to Airflow-ready stateless functions.
See the [API Levels guide](https://nfb2021.github.io/canvodpy/guides/api-levels/) for details.

## Development Setup

```bash
# Clone repository (with submodules for demo notebooks and test data)
git clone --recurse-submodules https://github.com/nfb2021/canvodpy.git
cd canvodpy

# Verify required tools
just check-dev-tools

# Install dependencies + pre-commit hooks
uv sync
just hooks

# Run tests and code quality checks
just test
just check
```

### Common Commands

```bash
just --list              # Show all commands
just test                # Run all tests
just check               # Lint + format + type-check
just docs                # Preview documentation locally
just open-notebook NAME  # Edit a marimo notebook interactively
just app-notebook NAME   # Run a marimo notebook as read-only app
just notebooks           # List available notebooks
```

## Documentation

Full documentation is available at **[nfb2021.github.io/canvodpy](https://nfb2021.github.io/canvodpy/)**.

Key pages:

- [Getting Started](https://nfb2021.github.io/canvodpy/guides/getting-started/)
- [Architecture & Design Patterns](https://nfb2021.github.io/canvodpy/guides/architecture-design/)
- [API Levels](https://nfb2021.github.io/canvodpy/guides/api-levels/)
- [Configuration Guide](https://nfb2021.github.io/canvodpy/guides/configuration/)
- [Contributing](CONTRIBUTING.md)

## Project Structure

```
canvodpy/                       # Monorepo root
├── packages/                   # Independent packages
│   ├── canvod-readers/         #   RINEX & SBF parsing
│   ├── canvod-auxiliary/       #   Ephemeris & coordinate transforms
│   ├── canvod-grids/           #   Hemispheric grid operations
│   ├── canvod-vod/             #   VOD retrieval algorithms
│   ├── canvod-store/           #   Icechunk versioned storage
│   ├── canvod-store-metadata/  #   Store provenance & compliance
│   ├── canvod-viz/             #   Visualisation
│   ├── canvod-ops/             #   Preprocessing pipeline
│   ├── canvod-utils/           #   Configuration & utilities
│   ├── canvod-virtualiconvname/#   Filename mapping
│   └── canvod-audit/          #   Three-tier verification suite
├── canvodpy/                   # Umbrella package + orchestrator
├── demo/                       # marimo notebooks (submodule)
├── config/                     # YAML configuration files
├── docs/                       # Zensical documentation
├── .github/                    # CI/CD workflows
├── CONTRIBUTORS.md             # Project contributors
├── NOTICE                      # Apache 2.0 attribution
└── LICENSE                     # Apache License 2.0
```

## AI-Assisted Development

This project uses [Claude Code](https://claude.com/claude-code) as a development and maintenance tool. The repository includes a comprehensive `CLAUDE.md` that provides the AI agent with:

- **Scientific domain knowledge** — GNSS-T, VOD, signal processing concepts
- **Architecture context** — monorepo structure, API levels, data contracts
- **15+ domain skills** — xarray, Zarr, Pydantic, pytest, marimo, and more
- **Persistent memory** — project decisions, conventions, and known issues across sessions

New contributors can run `claude` in the repo root to get an AI assistant with full project context — it can explain any module, run tests, generate diagrams, and navigate the 12-package monorepo.

## Contributing

Contributions of all kinds are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

First-time contributors: add yourself to [CONTRIBUTORS.md](CONTRIBUTORS.md) in your PR.

## License

Licensed under the [Apache License 2.0](LICENSE).

This software is provided "as is" without warranty of any kind. See the
[Impressum](https://nfb2021.github.io/canvodpy/impressum/) for full legal notice
and AI disclosure.

## Affiliation

Founded by **Nicolas François Bader**

[Climate and Environmental Remote Sensing Research Unit (CLIMERS)](https://www.tuwien.at/en/mg/geo/climers)
Department of Geodesy and Geoinformation, TU Wien
