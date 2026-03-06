# canVODpy

<!-- Build & Quality -->
[![Platform Tests](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml/badge.svg)](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml)
[![Code Coverage](https://github.com/nfb2021/canvodpy/actions/workflows/test_coverage.yml/badge.svg)](https://github.com/nfb2021/canvodpy/actions/workflows/test_coverage.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

<!-- Python & Platforms -->
[![Python](https://img.shields.io/badge/python-3.13%20|%203.14-blue.svg)](https://www.python.org/)
[![Platforms](https://img.shields.io/badge/platform-Linux%20|%20macOS%20|%20Windows-lightgrey)](https://github.com/nfb2021/canvodpy/actions/workflows/test_platforms.yml)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

<!-- Tech Stack -->
[![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)
[![Icechunk](https://img.shields.io/badge/Icechunk-Storage-00A3E0)](https://icechunk.io/)

<!-- Standards -->
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18636775.svg)](https://doi.org/10.5281/zenodo.18636775)
[![fair-software.eu](https://img.shields.io/badge/fair--software.eu-%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8B%20%20%E2%97%8F%20%20%E2%97%8B-orange)](https://fair-software.eu)

<!-- Project -->
[![CLIMERS @ TU Wien](https://img.shields.io/badge/CLIMERS-TU_Wien-006699)](https://www.tuwien.at/en/mg/geo/climers)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An open Python ecosystem for GNSS-Transmissometry (GNSS-T) canopy VOD retrievals. canVODpy aims to become the central community-driven software suite for deriving and analyzing Vegetation Optical Depth from GNSS signal-to-noise ratio observations.

> [!CAUTION]
> **Confidential — Pre-release Software.**
> This software is shared under restricted access. Distribution, redistribution, or publication of this code or any derived results is **not permitted** without explicit written authorization from the author (Nicolas F. Bader, nicolas.bader@tuwien.ac.at). Access is granted solely to individuals authorized by the author.

> [!IMPORTANT]
> This project is in **beta**. Install from [TestPyPI](https://test.pypi.org/project/canvodpy/) for now.
> Development requires `uv` and `just`:
> - Install `uv`: [uv documentation](https://docs.astral.sh/uv/getting-started/installation/)
> - Install `just`: [just documentation](https://github.com/casey/just)

> New to Git, GitHub, or Python tooling? See the [Getting Started Guide](docs/guides/getting-started.md) for a complete walkthrough.

## Overview

canVODpy is organized as a monorepo with independent, composable packages:

- **canvod-readers** - RINEX and GNSS data format readers
- **canvod-auxiliary** - Auxiliary data handling (orbits, meteorology)
- **canvod-grids** - Hemispheric grid operations (equal-area, HEALPix)
- **canvod-vod** - Vegetation Optical Depth calculation algorithms
- **canvod-store** - Versioned storage via Icechunk
- **canvod-viz** - Hemispheric and time-series visualization
- **canvod-utils** - Shared utilities
- **canvodpy** - Umbrella package with pipeline orchestrator

## Installation

```bash
# Install from TestPyPI (beta)
uv pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ canvodpy

# Or install specific components
uv pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ canvod-readers canvod-grids
```

## Development Setup

### Prerequisites

This project requires two tools that need to be installed separately:

1. **uv** - Fast Python package manager
   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

   # Or via package manager
   brew install uv  # macOS
   ```

2. **just** - Command runner
   ```bash
   # macOS/Linux
   curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash

   # Or via package manager
   brew install just      # macOS
   cargo install just     # Rust
   apt install just       # Ubuntu 23.04+
   ```

### Setup Steps

```bash
# Clone repository
git clone https://github.com/nfb2021/canvodpy.git
cd canvodpy

# Verify required tools are installed
just check-dev-tools  # Checks uv, just, python3

# Install Python dependencies
uv sync

# Install pre-commit hooks
just hooks

# Run tests
just test

# Check code quality
just check
```

### Available Commands

See all available commands:
```bash
just --list
```

Common commands:
- `just check` - Lint, format, and type-check
- `just test` - Run all tests
- `just test-coverage` - Run tests with coverage report
- `just changelog` - Generate CHANGELOG from commits
- `just release <VERSION>` - Create a new release

## Documentation

- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Contribution guidelines
- **[docs/VERSIONING.md](docs/VERSIONING.md)** - Versioning strategy
- **[docs/RELEASING.md](docs/RELEASING.md)** - Release process
- **[docs/guides/HOW_RELEASE_WORKS.md](docs/guides/HOW_RELEASE_WORKS.md)** - Release system internals
- **[docs/guides/PYPI_SETUP.md](docs/guides/PYPI_SETUP.md)** - PyPI publishing setup

## Usage

```python
# Import from namespace packages
from canvod.readers import Rnxv3Obs
from canvod.grids import create_hemigrid
from canvod.vod import VODCalculator

# Or use umbrella package
import canvodpy
```

## Project Structure

```
canvodpy/                    # Monorepo root
├── packages/                # Independent packages
│   ├── canvod-readers/
│   ├── canvod-auxiliary/
│   ├── canvod-grids/
│   ├── canvod-vod/
│   ├── canvod-store/
│   ├── canvod-viz/
│   └── canvod-utils/
├── canvodpy/               # Umbrella package + orchestrator
├── .github/                # CI/CD workflows
├── docs/                   # Documentation
└── pyproject.toml          # Workspace configuration
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE file](https://github.com/nfb2021/canvodpy/blob/main/LICENSE) for details.

## Author & Affiliation

**Author:** Nicolas François Bader (nicolas.bader@geo.tuwien.ac.at)

Developed at the **Climate and Environmental Remote Sensing Research Unit (CLIMERS)**
Department of Geodesy and Geoinformation
TU Wien (Vienna University of Technology)
[https://www.tuwien.at/en/mg/geo/climers](https://www.tuwien.at/en/mg/geo/climers)
