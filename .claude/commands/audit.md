Run or inspect a canvod-audit verification tier. Ask which tier to run if not specified.

## Audit tiers

| Tier | Command | What it verifies |
|---|---|---|
| 0 | `audit_vs_gnssvodpy` | canvodpy vs gnssvodpy (must be bit-identical VOD) |
| 0 | `audit_api_levels` | All 4 API levels produce identical results |
| 1a | `audit_sbf_vs_rinex` | SBF and RINEX readers agree within tolerance |
| 1b | `audit_ephemeris_sources` | Broadcast vs agency ephemeris comparison |
| 2-freeze | `freeze_checkpoint` | Save current output as regression baseline |
| 2-check | `audit_regression` | Compare current output vs frozen checkpoint |
| 3 | `audit_vs_gnssvod` | Compare vs Humphrey et al. gnssvod tool |
| infra | `audit_store_round_trip`, `audit_temporal_chunking`, `audit_idempotency`, `audit_constellation_filter` | Infrastructure correctness |

## Usage

```python
from canvod.audit.runners import audit_vs_gnssvodpy, freeze_checkpoint
result = audit_vs_gnssvodpy(canvodpy_store_path, gnssvodpy_store_path)
```

## Safety rules

- Audit output dir: `/Volumes/ExtremePro/canvod_audit_output/`
- NEVER modify `main` branch of any store there
- `gnssvodpy_based/` stores are read-only truth references
- Always create new branches for experimental comparisons

## Scripts

Standalone scripts in `packages/canvod-audit/scripts/` — see their README.
