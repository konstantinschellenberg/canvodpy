"""Tier 2: Compare current store outputs against frozen checkpoints.

Run after any code change to verify outputs haven't regressed.
Checkpoints must exist from a prior ``run_tier2_freeze.py`` run.

Expected results
----------------
- EXACT tier: bit-identical outputs (same algorithm, same data)
- If a deliberate algorithm change was made, re-freeze after manual review.
"""

from __future__ import annotations

from pathlib import Path

from canvod.audit.runners.regression import audit_regression
from canvod.audit.tolerances import ToleranceTier

# ── Configuration ────────────────────────────────────────────────────────────

AUDIT_ROOT = Path("/Volumes/ExtremePro/canvod_audit_output")
CHECKPOINT_DIR = AUDIT_ROOT / "tier2_checkpoints"

STORES = {
    "tier0_rinex": (
        AUDIT_ROOT / "tier0_rinex_vs_gnssvodpy" / "Rosalia" / "canvodpy_RINEX_store",
    ),
    "tier0_vod": (
        AUDIT_ROOT / "tier0_rinex_vs_gnssvodpy" / "Rosalia" / "canvodpy_VOD_store",
    ),
    "tier1_sbf_agency": (
        AUDIT_ROOT / "tier1_sbf_vs_rinex" / "Rosalia" / "canvodpy_SBF_allvars_store",
    ),
    "tier1_sbf_broadcast": (
        AUDIT_ROOT
        / "tier1_broadcast_vs_agency"
        / "Rosalia"
        / "canvodpy_SBF_broadcast_store",
    ),
}


def main() -> None:
    if not CHECKPOINT_DIR.exists():
        print(f"No checkpoints found at {CHECKPOINT_DIR}")
        print("Run run_tier2_freeze.py first to create a baseline.")
        return

    all_passed = True

    for label, (store_path,) in STORES.items():
        cp_dir = CHECKPOINT_DIR / label
        if not cp_dir.exists():
            print(f"SKIP {label}: no checkpoints at {cp_dir}")
            continue
        if not store_path.exists():
            print(f"SKIP {label}: store not found at {store_path}")
            continue

        print("=" * 60)
        print(f"Tier 2 regression: {label}")
        print("=" * 60)

        result = audit_regression(
            store=str(store_path),
            checkpoint_dir=str(cp_dir),
            tier=ToleranceTier.EXACT,
        )

        if not result.passed:
            all_passed = False
            print(f"\n*** REGRESSION DETECTED in {label} ***\n")

        print()

    if all_passed:
        print("ALL TIER 2 REGRESSION CHECKS PASSED")
    else:
        print("SOME REGRESSIONS DETECTED — review above output")


if __name__ == "__main__":
    main()
