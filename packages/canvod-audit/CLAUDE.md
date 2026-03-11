# canvod-audit

Three-tier verification framework for GNSS-VOD pipeline correctness.

## Key modules

| Module | Purpose |
|---|---|
| `core.py` | `compare_datasets()`, `ComparisonResult`, `AlignmentInfo` |
| `tolerances.py` | `Tolerance`, `ToleranceTier` (EXACT / NUMERICAL / SCIENTIFIC) |
| `rinex_trimmer.py` | `RinexTrimmer` — wraps gfzrnx for obs type filtering |
| `tiers/internal.py` | Tier 1: SBF vs RINEX, broadcast vs agency ephemeris |
| `tiers/regression.py` | Tier 2: `freeze_checkpoint()`, `audit_regression()` |
| `tiers/external.py` | Tier 3: comparison vs gnssvod (Humphrey et al.) |
| `runners/` | CLI-friendly runner functions for each audit |
| `reporting/` | Plotting and report generation |

## Audit tiers

| Tier | What | Status |
|---|---|---|
| 0 | canvodpy vs gnssvodpy (bit-identical VOD) | Done |
| 1a | SBF vs RINEX internal consistency | Done |
| 1b | Broadcast vs agency ephemeris | Done |
| 2 | Regression freeze/check | Done |
| 3 | vs gnssvod (Humphrey et al.) | Needs end-to-end run |

## Audit output

Output dir: `/Volumes/ExtremePro/canvod_audit_output/`
- **CRITICAL**: NEVER modify `main` branch of any store there
- `gnssvodpy_based/` = truth stores (read-only)

## RinexTrimmer

Required for Tier 3: trims to one code per band to eliminate SID vs PRN ambiguity.
Wraps `gfzrnx` (installed at `/usr/local/bin/gfzrnx`).

## Testing

```bash
uv run pytest packages/canvod-audit/tests/
```

19 tests: `test_core.py` (8) + `test_runners.py` (11)
