---
title: canVODpy
description: An Open Python Ecosystem for GNSS-Transmissometry Canopy VOD Retrievals
---

<div class="hero" markdown>

# canVODpy

**GNSS Transmissometry · Canopy VOD Retrievals · Open Science**

From raw receiver binary to calibrated vegetation optical depth —
a full Python ecosystem built for reproducibility and scale.

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18636775.svg)](https://doi.org/10.5281/zenodo.18636775)
[![PyPI](https://img.shields.io/pypi/v/canvodpy)](https://pypi.org/project/canvodpy/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[Get started :fontawesome-solid-arrow-right:](guides/getting-started.md){ .md-button .md-button--primary }
[Read the paper :fontawesome-regular-file-lines:](#publications){ .md-button }

</div>

---

## What makes canVODpy different

<div class="grid cards" markdown>

-   :fontawesome-solid-satellite-dish: &nbsp; **No ephemeris needed for SBF**

    ---

    Septentrio SBF files embed satellite azimuth and zenith angles directly.
    Quick-look VOD in one file read, no SP3/CLK download.

    [:octicons-arrow-right-24: SBF Reader](packages/readers/sbf.md)

-   :fontawesome-solid-layer-group: &nbsp; **Single unified dataset format**

    ---

    Both RINEX v3.04 and SBF binary produce identical `(epoch × sid)` xarray Datasets.
    Downstream code is reader-agnostic.

    [:octicons-arrow-right-24: Reader Architecture](packages/readers/architecture.md)

-   :fontawesome-solid-database: &nbsp; **Versioned, cloud-native storage**

    ---

    Icechunk gives every append a git-like commit snapshot.
    Reproducible reads, safe parallel writes, S3-ready.

    [:octicons-arrow-right-24: canvod-store](packages/store/overview.md)

-   :fontawesome-solid-globe: &nbsp; **Hemispheric grid system**

    ---

    Equal-area, HEALPix, geodesic and four more grid types.
    KDTree cell assignment in O(n log m).

    [:octicons-arrow-right-24: canvod-grids](packages/grids/overview.md)

-   :fontawesome-solid-code: &nbsp; **Four API levels**

    ---

    One-liner convenience functions · Fluent workflow · `VODWorkflow` class ·
    Fully stateless functional API for Airflow / Prefect.

    [:octicons-arrow-right-24: Quick Start](#quick-start)

-   :fontawesome-solid-rotate: &nbsp; **Parallel processing pipeline**

    ---

    `ProcessPoolExecutor`-backed pipeline with per-file commit,
    hash deduplication, and cooperative distributed writing.

    [:octicons-arrow-right-24: Architecture](architecture.md)

</div>

---

## Supported receiver formats

<div class="grid" markdown>

!!! success "RINEX v3.04"

    Text-based standard format from all manufacturers.
    Satellite geometry computed from SP3 + CLK precise ephemerides.

    **Reader:** `Rnxv3Obs` — all GNSS constellations, all bands

!!! info "Septentrio Binary Format (SBF)"

    High-rate binary telemetry from AsteRx SB3, mosaic-X5, PolaRx.
    Satellite geometry, PVT quality, DOP and receiver health embedded.

    **Reader:** `SbfReader` — produces obs dataset + metadata dataset in one pass

</div>

---

## Quick Start

```bash
pip install canvodpy
```

=== "Level 1 — Convenience"

    Two lines, everything automatic:

    ```python
    from canvodpy import process_date, calculate_vod

    data = process_date("Rosalia", "2025001")
    vod  = calculate_vod("Rosalia", "canopy_01", "reference_01", "2025001")
    ```

=== "Level 2 — Fluent workflow"

    Deferred execution, chainable steps:

    ```python
    import canvodpy

    result = (
        canvodpy.workflow("Rosalia")
            .read("2025001")
            .preprocess()
            .grid("equal_area", angular_resolution=5.0)
            .vod("canopy_01", "reference_01")
            .result()
    )
    ```

=== "Level 3 — VODWorkflow"

    Eager execution with structured logging:

    ```python
    from canvodpy import VODWorkflow

    wf  = VODWorkflow(site="Rosalia", grid="equal_area")
    vod = wf.calculate_vod("canopy_01", "reference_01", "2025001")
    ```

=== "Level 4 — SBF quick-look"

    Binary file, embedded geometry, no downloads:

    ```python
    from canvodpy import Site

    site     = Site("Rosalia")
    pipeline = site.pipeline(reader="sbf")

    for date, datasets in pipeline.process_range("2025001", "2025007"):
        print(f"{date}: {list(datasets.keys())}")
    ```

---

## Processing Pipeline

```mermaid
flowchart LR
    subgraph ACQ["Acquisition"]
        RINEX["RINEX 3.04"]
        SBF["SBF Binary"]
        SP3["SP3 / CLK"]
    end

    subgraph PREP_R["Preprocessing — RINEX"]
        PARSE["Parse & Hermite interpolation"]
        SCS_R["ECEF → Spherical (θ, φ)"]
    end

    subgraph PREP_S["Preprocessing — SBF (no download)"]
        SCS_S["Embedded geometry (θ, φ)"]
        META["Quality metadata (DOP · PVT · rx_error)"]
    end

    subgraph STORE["Icechunk"]
        ICE["Observations (epoch × sid)"]
        META_S["Metadata store"]
    end

    subgraph GRID["Grid"]
        HGRID["Hemispheric grid"]
        KD["KDTree assignment"]
    end

    subgraph VOD["VOD"]
        PAIR["Canopy / Reference pairing"]
        TAU["Tau-Omega inversion\nVOD = −ln(T)·cos(θ)"]
    end

    OUT["VOD Dataset"]

    RINEX --> PARSE --> SCS_R --> ICE
    SP3 --> PARSE
    SBF --> SCS_S --> ICE
    SBF --> META --> META_S

    ICE --> HGRID --> KD --> PAIR --> TAU --> OUT
```

---

## Packages

<div class="grid cards" markdown>

-   :fontawesome-solid-book-open: &nbsp; **canvod-readers**

    ---

    RINEX v3.04 and SBF binary parsing.
    Unified `(epoch × sid)` output with full validation.

-   :fontawesome-solid-cloud-arrow-down: &nbsp; **canvod-auxiliary**

    ---

    SP3 ephemeris and CLK clock retrieval.
    Hermite spline interpolation, FTP fallback chain.

-   :fontawesome-solid-border-all: &nbsp; **canvod-grids**

    ---

    7 hemispheric grid types — equal-area, HEALPix, geodesic and more.
    KDTree-backed O(n log m) cell assignment.

-   :fontawesome-solid-leaf: &nbsp; **canvod-vod**

    ---

    VOD estimation via zeroth-order tau-omega model.
    Extensible `VODCalculator` ABC.

-   :fontawesome-solid-database: &nbsp; **canvod-store**

    ---

    Icechunk versioned storage with generic metadata dataset API.
    Per-file hash deduplication, S3-compatible backends.

-   :fontawesome-solid-chart-line: &nbsp; **canvod-viz**

    ---

    2D polar hemisphere plots and 3D interactive Plotly surfaces.

-   :fontawesome-solid-gear: &nbsp; **canvod-utils**

    ---

    Pydantic configuration, YYYYDOY date utilities, shared tooling.

-   :fontawesome-solid-circle-nodes: &nbsp; **canvodpy**

    ---

    Umbrella package — four API levels, factory system, unified entry point.

</div>

---

## Technology

<div class="grid" markdown>

!!! abstract "Scientific stack"

    `xarray` · `NumPy` · `SciPy` · `Polars`

!!! abstract "Storage"

    `Icechunk` · `Zarr` · S3-compatible

!!! abstract "Code quality"

    `ruff` · `ty` · `pytest` · `uv`

!!! abstract "Documentation"

    `Zensical` · `beautiful-mermaid` · `marimo` notebooks

</div>

---

## Publications

Bader, N. F. (2026). *canVODpy: An Open Python Ecosystem for GNSS-Transmissometry Canopy VOD Retrievals* (v0.1.0-beta.2).
Zenodo. [https://doi.org/10.5281/zenodo.18636775](https://doi.org/10.5281/zenodo.18636775)

---

## Affiliation

**Climate and Environmental Remote Sensing Research Unit (CLIMERS)**
Department of Geodesy and Geoinformation, TU Wien

[tuwien.at/en/mg/geo/climers](https://www.tuwien.at/en/mg/geo/climers){ .md-button }
