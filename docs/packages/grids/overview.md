# canvod-grids

## Purpose

The `canvod-grids` package provides spatial grid implementations for hemispheric GNSS signal analysis. It discretizes the hemisphere visible from a ground-based receiver into cells — a prerequisite for spatially resolved VOD estimation.

---

## Grid Types

Seven implementations are available, all inheriting from `BaseGridBuilder`:

<div class="grid cards" markdown>

-   :fontawesome-solid-border-all: &nbsp; **EqualAreaBuilder** *(recommended)*

    ---

    Cells of approximately equal solid angle. Ensures uniform spatial
    sampling across the hemisphere, avoiding zenith over-weighting.

-   :fontawesome-solid-table-cells: &nbsp; **EqualAngleBuilder**

    ---

    Regular angular spacing in θ and φ. Cells near zenith are smaller —
    appropriate when you want uniform angular resolution.

-   :fontawesome-solid-grip: &nbsp; **EquirectangularBuilder**

    ---

    Simple rectangular latitude/longitude grid. Fast to compute;
    strong area distortion at low elevations.

-   :fontawesome-solid-circle-dot: &nbsp; **HEALPixBuilder**

    ---

    Hierarchical Equal Area isoLatitude Pixelization. All cells have
    identical areas; supports multi-resolution refinement.
    Requires `healpy`.

-   :fontawesome-solid-dice-d20: &nbsp; **GeodesicBuilder**

    ---

    Icosahedron subdivision. Near-uniform cell area with triangular
    cell boundaries.

-   :fontawesome-solid-seedling: &nbsp; **FibonacciBuilder**

    ---

    Fibonacci-sphere sampling. Aesthetically uniform point
    distribution; ideal for scatter-plot analyses.
    Requires `scipy`.

-   :fontawesome-solid-triangle-exclamation: &nbsp; **HTMBuilder**

    ---

    Hierarchical Triangular Mesh. Recursive triangular decomposition
    of the sphere; supports efficient spatial indexing.

</div>

All builders accept `angular_resolution` (degrees) and `cutoff_theta` (maximum zenith angle) and return a `GridData` object.

---

## Usage

=== "Factory function"

    ```python
    from canvod.grids import create_hemigrid

    grid = create_hemigrid("equal_area", angular_resolution=5.0)
    print(grid.ncells)   # 216
    print(grid.nbands)   # 18
    ```

=== "Builder pattern"

    ```python
    from canvod.grids import EqualAreaBuilder

    builder = EqualAreaBuilder(angular_resolution=5.0, cutoff_theta=10.0)
    grid = builder.build()
    ```

=== "canvodpy factory"

    ```python
    from canvodpy import GridFactory

    builder = GridFactory.create("equal_area", angular_resolution=5.0)
    grid = builder.build()
    print(grid.grid.head())  # Polars DataFrame with cell geometry
    ```

---

## GridData

The `GridData` object returned by all builders provides:

| Attribute | Type | Description |
| --------- | ---- | ----------- |
| `grid` | `polars.DataFrame` | Cell geometry (boundaries, centres, solid angles) |
| `ncells` | `int` | Total number of grid cells |
| `nbands` | `int` | Number of elevation bands |
| `definition` | `str` | Human-readable grid description |

---

## Grid Operations

`canvod.grids.operations` handles the interface between grids and xarray Datasets:

<div class="grid cards" markdown>

-   :fontawesome-solid-map: &nbsp; **Cell Assignment**

    ---

    `add_cell_ids_to_ds_fast` — KDTree-accelerated assignment of each
    observation to a grid cell. O(n log m) for n observations, m cells.

    `add_cell_ids_to_vod_fast` — Same for VOD datasets.

-   :fontawesome-solid-floppy-disk: &nbsp; **Grid Persistence**

    ---

    `store_grid` / `load_grid` — Save and reload grid definitions
    as Zarr/NetCDF for reproducible workflows.

    `grid_to_dataset` — Convert `GridData` to xarray Dataset.

-   :fontawesome-solid-chart-pie: &nbsp; **Aggregation**

    ---

    `aggregate_data_to_grid` — Assign and aggregate per cell.

    `compute_hemisphere_percell` — Per-cell statistics.

    `compute_zenith_percell` — Zenith-weighted statistics.

    `compute_percell_timeseries` — Per-cell time series.

</div>

---

## Analysis Subpackage

`canvod.grids.analysis` provides filtering, weighting, and pattern analysis:

| Module | Purpose |
| ------ | ------- |
| `filtering` | Global IQR, Z-score statistical filters |
| `per_cell_filtering` | Per-cell variants of the above |
| `hampel_filtering` | Hampel (median-MAD) outlier detection |
| `sigma_clip_filter` | Numba-accelerated sigma-clipping |
| `masking` | Spatial and temporal mask construction |
| `weighting` | Per-cell weight calculators |
| `solar` | Solar geometry (elevation, azimuth) |
| `temporal` | Diurnal aggregation and analysis |
| `spatial` | Per-cell spatial statistics |

---

## Coordinate Convention

!!! note "Angles"

    All grids use standard spherical coordinates:

    - **phi** (φ): azimuth 0 → 2π · 0 = North · π/2 = East · clockwise
    - **theta** (θ): polar angle from zenith · 0 = zenith · π/2 = horizon

    This matches the canvod-auxiliary coordinate output — no conversion needed.

---

## Role in the VOD Pipeline

```mermaid
flowchart LR
    A["Augmented Dataset\n(epoch × sid, with θ, φ)"] --> B["Grid Assignment\nadd_cell_ids_to_ds_fast"]
    B --> C["Gridded Dataset\n+ cell_id coordinate"]
    C --> D["VOD per cell\nTau-Omega inversion"]
    D --> E["Hemispherical Map\ncanvod-viz"]
```
