---
title: Monorepo Structure
description: Architecture of the canVODpy monorepo and its package organization
---

# Monorepo Structure

## Overview

canVODpy is organized as a monorepo containing eight Python packages for GNSS vegetation optical depth analysis. All packages reside in a single repository while maintaining technical independence: each can be developed, tested, and published separately.

---

## Package Layers

```mermaid
graph LR
    subgraph FOUNDATION["Foundation Layer"]
        UTILS["canvod-utils\nConfiguration (Pydantic)\nDate utilities (YYYYDOY)\nShared tooling"]
    end

    subgraph DATAIO["Data I/O Layer"]
        READERS["canvod-readers\nRINEX v3.04 (Rnxv3Obs)\nSignal ID mapping"]
        AUX["canvod-auxiliary\nSP3/CLK retrieval\nHermite interpolation\nFTP download management"]
    end

    subgraph STORE_LAYER["Persistence Layer"]
        STORE["canvod-store\nIcechunk versioned storage\nHash deduplication\nSite/receiver management"]
    end

    subgraph COMPUTE["Computation Layer"]
        VOD["canvod-vod\nTau-Omega inversion\nVODCalculator ABC"]
        GRIDS["canvod-grids\n7 hemispheric grid types\nKDTree cell assignment"]
    end

    subgraph PRESENT["Presentation Layer"]
        VIZ["canvod-viz\n2D polar projections\n3D interactive surfaces"]
    end

    subgraph ORCHESTRATION["Orchestration Layer"]
        CANVODPY["canvodpy\nPipeline orchestrator\nFactory system\n4-level public API"]
    end

    READERS -.-> UTILS
    AUX -.-> READERS
    AUX -.-> UTILS
    STORE -.-> AUX
    STORE -.-> READERS
    STORE -.-> UTILS
    GRIDS -.-> UTILS
    VIZ -.-> GRIDS

    CANVODPY ==> READERS
    CANVODPY ==> AUX
    CANVODPY ==> STORE
    CANVODPY ==> VOD
    CANVODPY ==> GRIDS
    CANVODPY ==> VIZ
```

---

## Key Design Decisions

<div class="grid cards" markdown>

-   :fontawesome-solid-cubes: &nbsp; **Namespace Packages**

    ---

    All packages share the `canvod.*` namespace — a coherent import API
    backed by separate installable packages:

    ```python
    from canvod.readers import Rnxv3Obs
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
    dependencies. Three consumer packages each depend on exactly one
    foundation package.

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
    canvod-viz/
    canvod-utils/
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
canvod-viz        ──── depends on canvod-grids
canvodpy          ──── depends on all packages
```

---

## Complete Processing Flow

```mermaid
flowchart TD
    subgraph CFG["Configuration"]
        YAML["YAML Config\n(processing · sites · sids)"]
        PYDANTIC["Pydantic Validation"]
        CONFIG["CanvodConfig"]
    end

    subgraph INIT["Site Initialization"]
        SITE["Site(name)"]
        RINEX_STORE["RINEX Icechunk Store"]
        VOD_STORE["VOD Icechunk Store"]
    end

    subgraph DISCOVERY["Data Discovery"]
        MATCHER["PairDataDirMatcher"]
        EXPAND["SCS Expansion"]
        SCHEDULE["Processing Schedule"]
    end

    subgraph AUX["Auxiliary Pipeline (RINEX only)"]
        FTP["FTP Download\nESA / NASA fallback"]
        HERMITE["Hermite Interpolation\n(SP3 ephemerides)"]
        LINEAR["Piecewise Linear\n(CLK corrections)"]
        AUX_ZARR["Auxiliary Zarr Cache"]
    end

    subgraph PARALLEL["Parallel Processing"]
        READ_R["Read RINEX (Rnxv3Obs)"]
        SPHERICAL["Spherical Coords\n(ECEF → r, θ, φ)"]
    end

    subgraph WRITE["Icechunk Storage"]
        HASH_CHECK["RINEX File Hash Check\n(skip duplicates)"]
        APPEND["Append + Commit"]
    end

    subgraph GRID["Grid Assignment"]
        BUILD_GRID["Build Grid\n(equal-area / HEALPix / …)"]
        KDTREE["KDTree Assign\nO(n log m)"]
    end

    subgraph VOD["VOD Retrieval"]
        DELTA["ΔSNRcanopy − ref"]
        TAU["VOD = −ln(T) · cos(θ)"]
    end

    YAML --> PYDANTIC --> CONFIG --> SITE
    SITE --> RINEX_STORE & VOD_STORE

    CONFIG --> MATCHER --> EXPAND --> SCHEDULE

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
