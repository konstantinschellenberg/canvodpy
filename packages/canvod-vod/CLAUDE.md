# canvod-vod

VOD (Vegetation Optical Depth) retrieval algorithms.

## Key modules

| Module | Purpose |
|---|---|
| `calculator.py` | `VODCalculator` ABC, `TauOmegaZerothOrder` implementation |
| `_internal/` | Internal computation helpers |

## Algorithm

Zeroth-order Tau-Omega radiative transfer model:
- Compares SNR through canopy vs open-sky reference
- `VOD = -ln(SNR_canopy / SNR_reference) / (2 * cos(theta))`
- theta = zenith angle of satellite signal path through canopy

## Input requirements

Dataset must have:
- `obs` / `snr` variables with `(epoch, sid)` dims
- Canopy and reference theta/phi from ephemeris augmentation
- Grid cell assignments (for spatial aggregation)

## Testing

```bash
uv run pytest packages/canvod-vod/tests/
```

VOD is bit-identical between canvodpy and gnssvodpy (verified by canvod-audit Tier 0).
