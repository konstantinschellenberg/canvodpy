# canvod-grids

Hemisphere grid discretization and spatiotemporal VOD analysis.

## Key modules

| Module | Purpose |
|---|---|
| `core/` | `GridData` structure, `BaseGridBuilder` ABC, `GridType` enum |
| `grids_impl/` | 7 grid builders: `EqualAreaBuilder`, `EqualAngleBuilder`, `EquirectangularBuilder`, `HTMBuilder`, `GeodesicBuilder`, `HEALPixBuilder`, `FibonacciBuilder` |
| `analysis/` | `TemporalAnalysis`, `VODSpatialAnalyzer`, `PerCellVODAnalyzer`, diurnal/spatial patterns |
| `aggregation.py` | `CellAggregator`, `WeightCalculator`, `SolarPositionCalculator` |
| `workflows/` | `AdaptedVODWorkflow` (store integration) |

## Grid types

The primary grid for GNSS-T is **equal-area hemisphere** (2° cells viewed from
receiver position looking up). Each satellite signal path intersects one cell
based on its (theta, phi) angles.

## Filtering

- `Filter` ABC + `ZScoreFilter`, `IQRFilter`, `RangeFilter`, `PercentileFilter`, `CustomFilter`
- `PerCellFilter` variants for per-grid-cell outlier removal
- Hampel filter and sigma clipping available

## Factory

```python
from canvod.grids import create_hemigrid
grid = create_hemigrid(grid_type=GridType.EQUAL_AREA, resolution=2.0)
```

## Testing

```bash
uv run pytest packages/canvod-grids/tests/
```
