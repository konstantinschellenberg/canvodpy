# canvod-auxiliary

Auxiliary data pipeline: satellite ephemerides, clock corrections, coordinate transforms.

## Key modules

| Module | Purpose |
|---|---|
| `ephemeris/provider.py` | `EphemerisProvider` ABC, `AgencyEphemerisProvider` (SP3/CLK), `SbfBroadcastProvider` |
| `interpolation/` | `Sp3InterpolationStrategy`, `ClockInterpolationStrategy` (Hermite spline) |
| `position/` | `ECEFPosition`, `GeodeticPosition` — coordinate systems |
| `augmentation.py` | `AuxDataAugmenter` pipeline (adds theta, phi, elevation to dataset) |
| `preprocessing.py` | `prep_aux_ds()`, SV→SID mapping, global SID padding |
| `matching/` | `DatasetMatcher` — temporal alignment of aux data to observations |
| `products/` | `ProductRegistry` — SP3/CLK product discovery |
| `clock/` | Clock correction handling |

## Ephemeris providers

| Provider | Source | Speed | Accuracy |
|---|---|---|---|
| `AgencyEphemerisProvider` | SP3 + CLK files (IGS/GFZ) | Slower (interpolation) | ~2 cm |
| `SbfBroadcastProvider` | SBF SatVisibility block | Fast (pre-computed) | ~1 m |

Both produce satellite ECEF positions → spherical coordinates (theta, phi) relative to receiver.

## Known issue: interpolation non-determinism

SP3 Hermite interpolation + `ThreadPoolExecutor` parallelism causes ~20 arcsec
non-deterministic differences in reference angles across runs (floating-point
non-associativity). Does NOT affect VOD (VOD uses canopy angles only).

## Data flow

```
SP3/CLK files → EphemerisProvider → ECEF positions → spherical coords → augmented dataset
                                                      ↗ receiver position (from RINEX header)
```

Receiver position: extracted via `ECEFPosition.from_ds_metadata(ds)`, NOT from config.

## Testing

```bash
uv run pytest packages/canvod-auxiliary/tests/
```
