---
title: Monorepo Structure
description: Architecture of the canVODpy monorepo and its package organization
---

# Monorepo Structure

## Overview

canVODpy is organized as a monorepo containing ten Python packages for GNSS vegetation optical depth analysis. All packages reside in a single repository while maintaining technical independence: each can be developed, tested, and published separately.

---

## Package Layers

```mermaid
graph TD
    subgraph ORCHESTRATION["Orchestration"]
        CANVODPY["canvodpy"]
    end

    subgraph COMPUTE["Computation"]
        VOD["canvod-vod"]
        GRIDS["canvod-grids"]
        OPS["canvod-ops"]
    end

    subgraph STORE_LAYER["Persistence"]
        STORE["canvod-store"]
        STOREMETA["canvod-store-metadata"]
    end

    subgraph DATAIO["Data I/O"]
        READERS["canvod-readers"]
        AUX["canvod-auxiliary"]
        NAMING["canvod-virtualiconvname"]
    end

    subgraph PRESENT["Presentation"]
        VIZ["canvod-viz"]
    end

    subgraph FOUNDATION["Foundation"]
        UTILS["canvod-utils"]
    end

    CANVODPY --> READERS & AUX & NAMING
    CANVODPY --> STORE & STOREMETA
    CANVODPY --> VOD & GRIDS & OPS & VIZ

    OPS -.-> GRIDS
    OPS -.-> UTILS
    VIZ -.-> GRIDS
    AUX -.-> READERS
    STORE -.-> GRIDS
    STOREMETA -.-> UTILS
```

| Layer | Packages | Role |
|-------|----------|------|
| **Orchestration** | canvodpy | Pipeline orchestrator, Dask batch processing, 4-level public API |
| **Computation** | canvod-vod, canvod-grids, canvod-ops | VOD retrieval, hemispheric grids, preprocessing pipeline |
| **Persistence** | canvod-store, canvod-store-metadata | Icechunk versioned storage, hash deduplication, provenance metadata (DataCite/ACDD/STAC) |
| **Data I/O** | canvod-readers, canvod-auxiliary, canvod-virtualiconvname | RINEX/SBF parsing, SP3/CLK retrieval, filename mapping |
| **Presentation** | canvod-viz | 2D polar projections, 3D interactive surfaces |
| **Foundation** | canvod-utils | Pydantic configuration, date utilities, shared tooling |

---

## Key Design Decisions

<div class="grid cards" markdown>

-   :fontawesome-solid-cubes: &nbsp; **Namespace Packages**

    ---

    All packages share the `canvod.*` namespace — a coherent import API
    backed by separate installable packages:

    ```python
    from canvod.readers import Rnxv3Obs
    from canvod.readers.sbf import SbfReader
    from canvod.grids import EqualAreaBuilder
    from canvod.vod import TauOmegaZerothOrder
    ```

    [:octicons-arrow-right-24: Namespace details](namespace-packages.md)

-   :fontawesome-solid-lock: &nbsp; **Workspace Architecture**

    ---

    One `uv sync` installs all packages in editable mode with a shared lockfile.
    Dependencies are resolved together — no version conflicts possible.

    Each package keeps its own `pyproject.toml` for independent PyPI publishing.

-   :fontawesome-solid-plug: &nbsp; **Independent Install**

    ---

    Install only what you need:

    ```bash
    pip install canvod-readers          # Readers only
    pip install canvod-grids canvod-vod # Grid + VOD
    pip install canvodpy                # Everything
    ```

-   :fontawesome-solid-sitemap: &nbsp; **Flat Dependency Graph**

    ---

    Maximum depth = 1. Four foundation packages have zero inter-package
    dependencies. Six consumer packages depend on one or two foundation
    packages each.

</div>

---

## Directory Structure

```
canvodpy/                           # Repository root
  packages/                         # Independent packages
    canvod-readers/
      src/
        canvod/                     # Namespace root (no __init__.py)
          readers/                  # Package code
            __init__.py

      tests/
      pyproject.toml
    canvod-auxiliary/               # Same structure
    canvod-grids/
    canvod-vod/
    canvod-store/
    canvod-store-metadata/
    canvod-viz/
    canvod-utils/
    canvod-ops/
    canvod-virtualiconvname/
  canvodpy/                         # Umbrella package
    src/
      canvodpy/
        __init__.py                 # Re-exports all subpackages
  docs/                             # Centralized documentation
  pyproject.toml                    # uv workspace config
  uv.lock                           # Shared lockfile
  Justfile                          # Task runner
```

---

## Dependency Graph

```
canvod-readers    ──── no inter-package deps
canvod-grids      ──── no inter-package deps
canvod-vod        ──── no inter-package deps
canvod-utils      ──── no inter-package deps
canvod-auxiliary   ─── depends on canvod-readers
canvod-store      ──── depends on canvod-grids
canvod-store-metadata ── depends on canvod-utils
canvod-viz        ──── depends on canvod-grids
canvod-ops        ──── depends on canvod-grids, canvod-utils
canvod-virtualiconvname ── no inter-package deps
canvodpy          ──── depends on all packages
```

---

## Complete Processing Flow

```mermaid
flowchart TD
    subgraph CFG["Configuration"]
        YAML["`**YAML Config**
        processing, sites, sids`"]
        PYDANTIC["Pydantic Validation"]
        CONFIG["CanvodConfig"]
    end

    subgraph INIT["Site Initialization"]
        SITE["Site(name)"]
        RINEX_STORE["RINEX Icechunk Store"]
        VOD_STORE["VOD Icechunk Store"]
    end

    subgraph DISCOVERY["Data Discovery"]
        VALIDATOR["`**DataDirectoryValidator**
        pre-flight gate`"]
        MAPPER["`**FilenameMapper**
        VirtualFiles`"]
        SCHEDULE["Processing Schedule"]
    end

    subgraph AUX["Auxiliary Pipeline (RINEX only)"]
        FTP["`**FTP Download**
        ESA / NASA fallback`"]
        HERMITE["`**Hermite Interpolation**
        SP3 ephemerides`"]
        LINEAR["`**Piecewise Linear**
        CLK corrections`"]
        AUX_ZARR["Auxiliary Zarr Cache"]
    end

    subgraph PARALLEL["Parallel Processing (Dask Distributed)"]
        READ_R["`**Read GNSS file**
        ReaderFactory`"]
        SPHERICAL["`**Spherical Coords**
        ECEF to r, theta, phi
        or SBF embedded geometry`"]
    end

    subgraph WRITE["Icechunk Storage"]
        HASH_CHECK["`**File Hash Check**
        skip duplicates`"]
        APPEND["Append + Commit"]
    end

    subgraph GRID["Grid Assignment"]
        BUILD_GRID["`**Build Grid**
        equal-area / HEALPix / ...`"]
        KDTREE["`**KDTree Assign**
        O(n log m)`"]
    end

    subgraph VOD["VOD Retrieval"]
        DELTA["delta SNR canopy - ref"]
        TAU["VOD = -ln(T) cos(theta)"]
    end

    YAML --> PYDANTIC --> CONFIG --> SITE
    SITE --> RINEX_STORE & VOD_STORE

    CONFIG --> VALIDATOR --> MAPPER --> SCHEDULE

    SCHEDULE --> FTP --> HERMITE & LINEAR --> AUX_ZARR

    SCHEDULE --> READ_R --> SPHERICAL
    AUX_ZARR --> SPHERICAL

    SPHERICAL --> HASH_CHECK --> APPEND --> RINEX_STORE

    RINEX_STORE --> BUILD_GRID --> KDTREE --> DELTA --> TAU
    TAU --> VOD_STORE
```

---

## Trade-offs

!!! success "Advantages"

    - Clear separation of concerns between packages
    - Users install only the components they need
    - Independent testing and development per package
    - Smaller dependency trees for individual packages

!!! warning "Costs"

    - Additional `pyproject.toml` per package
    - Developers must understand the namespace package mechanism
    - Coordinated releases required for consistent versioning
