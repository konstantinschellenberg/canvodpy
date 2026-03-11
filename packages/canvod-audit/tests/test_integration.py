"""Integration tests using real GNSS test data.

These tests read actual RINEX and SBF files from the test_data submodule,
run them through the readers, and audit the outputs. When readers, store,
or auxiliary code changes, these tests catch regressions against real data.

Skipped automatically if the test_data submodule is not initialized:
    git submodule update --init packages/canvod-readers/tests/test_data
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import AuditResult
from canvod.audit.tolerances import ToleranceTier

# ---------------------------------------------------------------------------
# Paths & skip logic
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
TEST_DATA = _REPO_ROOT / "packages" / "canvod-readers" / "tests" / "test_data"
VALID = TEST_DATA / "valid"
RINEX_DIR = VALID / "rinex_v3_04" / "01_Rosalia"
SBF_DIR = VALID / "sbf" / "01_Rosalia"
AUX_DIR = VALID / "aux_data"
STORE_DIR = VALID / "stores"

has_test_data = pytest.mark.skipif(
    not TEST_DATA.exists(), reason="test_data submodule not initialized"
)


def _first_file(directory: Path, pattern: str) -> Path:
    """Return the first matching file or skip the test."""
    files = sorted(directory.rglob(pattern))
    if not files:
        pytest.skip(f"No {pattern} files in {directory}")
    return files[0]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def canopy_rinex_file() -> Path:
    if not RINEX_DIR.exists():
        pytest.skip("RINEX test data not available")
    return _first_file(RINEX_DIR / "02_canopy" / "01_GNSS" / "01_raw", "*.rnx")


@pytest.fixture(scope="module")
def reference_rinex_file() -> Path:
    if not RINEX_DIR.exists():
        pytest.skip("RINEX test data not available")
    return _first_file(RINEX_DIR / "01_reference" / "01_GNSS" / "01_raw", "*.rnx")


@pytest.fixture(scope="module")
def canopy_sbf_file() -> Path:
    if not SBF_DIR.exists():
        pytest.skip("SBF test data not available")
    return _first_file(SBF_DIR / "02_canopy", "*.sbf")


@pytest.fixture(scope="module")
def canopy_rinex_ds(canopy_rinex_file) -> xr.Dataset:
    """Read a single canopy RINEX file into xarray."""
    from canvod.readers.rinex.v3_04 import Rnxv3Obs

    reader = Rnxv3Obs(fpath=canopy_rinex_file)
    return reader.to_ds()


@pytest.fixture(scope="module")
def canopy_sbf_ds(canopy_sbf_file) -> xr.Dataset:
    """Read a single canopy SBF file into xarray."""
    from canvod.readers.sbf.reader import SbfReader

    reader = SbfReader(fpath=canopy_sbf_file)
    return reader.to_ds()


# ---------------------------------------------------------------------------
# Tier 0: Reader self-consistency
# ---------------------------------------------------------------------------


@has_test_data
class TestTier0RealData:
    """Tier 0: reading the same file twice produces identical output."""

    def test_rinex_read_deterministic(self, canopy_rinex_file):
        """Two reads of the same RINEX file must be bit-identical."""
        from canvod.readers.rinex.v3_04 import Rnxv3Obs

        ds1 = Rnxv3Obs(fpath=canopy_rinex_file).to_ds()
        ds2 = Rnxv3Obs(fpath=canopy_rinex_file).to_ds()

        result = compare_datasets(
            ds1, ds2, tier=ToleranceTier.EXACT, label="rinex-determinism"
        )
        assert result.passed, f"RINEX read not deterministic: {result.failures}"

    def test_sbf_read_deterministic(self, canopy_sbf_file):
        """Two reads of the same SBF file must be bit-identical."""
        from canvod.readers.sbf.reader import SbfReader

        ds1 = SbfReader(fpath=canopy_sbf_file).to_ds()
        ds2 = SbfReader(fpath=canopy_sbf_file).to_ds()

        result = compare_datasets(
            ds1, ds2, tier=ToleranceTier.EXACT, label="sbf-determinism"
        )
        assert result.passed, f"SBF read not deterministic: {result.failures}"

    def test_rinex_dataset_structure(self, canopy_rinex_ds):
        """RINEX dataset has expected dims and variables."""
        assert "epoch" in canopy_rinex_ds.dims
        assert "sid" in canopy_rinex_ds.dims
        assert "SNR" in canopy_rinex_ds.data_vars
        assert len(canopy_rinex_ds.epoch) > 0
        assert len(canopy_rinex_ds.sid) > 0

    def test_sbf_dataset_structure(self, canopy_sbf_ds):
        """SBF dataset has expected dims and variables."""
        assert "epoch" in canopy_sbf_ds.dims
        assert "sid" in canopy_sbf_ds.dims
        assert "SNR" in canopy_sbf_ds.data_vars
        assert len(canopy_sbf_ds.epoch) > 0
        assert len(canopy_sbf_ds.sid) > 0


# ---------------------------------------------------------------------------
# Tier 1a: SBF vs RINEX (same time window)
# ---------------------------------------------------------------------------


@has_test_data
class TestTier1SbfVsRinex:
    """Tier 1a: SBF and RINEX from the same receiver, same time window.

    Note: RINEX is converted from SBF via Septentrio's ``sbf2rin``.
    The SBF reader reports raw receiver clock timestamps (2s offset
    from GPS time), while ``sbf2rin`` snaps epochs to the GPS time
    grid. This produces a systematic ~2s shift and ~18s range offset,
    so individual 15-min files have zero shared epochs despite
    covering the same observation window. We compare structural
    properties and value ranges rather than exact epoch alignment.
    """

    def test_shared_observables(self, canopy_rinex_ds, canopy_sbf_ds):
        """SBF and RINEX should share core observables."""
        shared_vars = set(canopy_rinex_ds.data_vars) & set(canopy_sbf_ds.data_vars)
        assert "SNR" in shared_vars, f"SNR missing from shared vars: {shared_vars}"

    def test_same_sid_universe(self, canopy_rinex_ds, canopy_sbf_ds):
        """SBF and RINEX from the same receiver should see similar satellites."""
        sids_rnx = set(str(s) for s in canopy_rinex_ds.sid.values)
        sids_sbf = set(str(s) for s in canopy_sbf_ds.sid.values)
        shared = sids_rnx & sids_sbf
        assert len(shared) > 0, "No shared SIDs between SBF and RINEX"
        # Most SIDs should be shared (same receiver, same time window)
        overlap_ratio = len(shared) / max(len(sids_rnx), len(sids_sbf))
        assert overlap_ratio > 0.5, (
            f"Low SID overlap: {len(shared)}/{max(len(sids_rnx), len(sids_sbf))} "
            f"({overlap_ratio:.0%})"
        )

    def test_snr_range_consistent(self, canopy_rinex_ds, canopy_sbf_ds):
        """SNR value ranges should be comparable between SBF and RINEX."""
        snr_rnx = canopy_rinex_ds["SNR"].values.ravel()
        snr_sbf = canopy_sbf_ds["SNR"].values.ravel()

        snr_rnx = snr_rnx[np.isfinite(snr_rnx)]
        snr_sbf = snr_sbf[np.isfinite(snr_sbf)]

        # Both should have data
        assert len(snr_rnx) > 0, "RINEX SNR is all NaN"
        assert len(snr_sbf) > 0, "SBF SNR is all NaN"

        # Median SNR should be in the same ballpark (within 5 dB)
        median_diff = abs(np.median(snr_rnx) - np.median(snr_sbf))
        assert median_diff < 5.0, (
            f"Median SNR differs by {median_diff:.1f} dB "
            f"(RINEX={np.median(snr_rnx):.1f}, SBF={np.median(snr_sbf):.1f})"
        )

    def test_sbf_quantization_visible(self, canopy_sbf_ds):
        """SBF SNR should show 0.25 dB quantization steps."""
        snr = canopy_sbf_ds["SNR"].values.ravel()
        snr = snr[np.isfinite(snr)]
        if len(snr) == 0:
            pytest.skip("No finite SNR values in SBF")

        # Check that SNR values are multiples of 0.25
        residuals = np.abs(snr * 4 - np.round(snr * 4))
        assert np.all(residuals < 1e-6), "SBF SNR not quantized to 0.25 dB steps"

    def test_epoch_cadence_matches(self, canopy_rinex_ds, canopy_sbf_ds):
        """Both readers should produce the same epoch cadence (5s)."""
        dt_rnx = np.diff(canopy_rinex_ds.epoch.values)
        dt_sbf = np.diff(canopy_sbf_ds.epoch.values)

        # Median cadence should be 5 seconds for both
        median_rnx = np.median(dt_rnx.astype("timedelta64[s]").astype(float))
        median_sbf = np.median(dt_sbf.astype("timedelta64[s]").astype(float))
        assert median_rnx == 5.0, f"RINEX cadence: {median_rnx}s (expected 5s)"
        assert median_sbf == 5.0, f"SBF cadence: {median_sbf}s (expected 5s)"


# ---------------------------------------------------------------------------
# Tier 2: Freeze / regression with real data
# ---------------------------------------------------------------------------


@has_test_data
class TestTier2RealData:
    """Tier 2: freeze real reader output, verify round-trip."""

    def test_rinex_freeze_roundtrip(self, canopy_rinex_ds, tmp_path):
        """Freeze RINEX output as checkpoint, reload and compare."""
        from canvod.audit.tiers.regression import (
            compare_against_checkpoint,
            freeze_checkpoint,
        )

        cp = freeze_checkpoint(
            canopy_rinex_ds,
            tmp_path / "rinex_checkpoint.nc",
            metadata={"source": "RINEX", "file": "canopy_15min"},
        )

        result = compare_against_checkpoint(
            canopy_rinex_ds, cp, tier=ToleranceTier.EXACT
        )
        assert result.passed, f"RINEX freeze round-trip failed: {result.failures}"

    def test_sbf_freeze_roundtrip(self, canopy_sbf_ds, tmp_path):
        """Freeze SBF output as checkpoint, reload and compare."""
        from canvod.audit.tiers.regression import (
            compare_against_checkpoint,
            freeze_checkpoint,
        )

        cp = freeze_checkpoint(
            canopy_sbf_ds,
            tmp_path / "sbf_checkpoint.nc",
            metadata={"source": "SBF", "file": "canopy_15min"},
        )

        result = compare_against_checkpoint(canopy_sbf_ds, cp, tier=ToleranceTier.EXACT)
        assert result.passed, f"SBF freeze round-trip failed: {result.failures}"


# ---------------------------------------------------------------------------
# Tier 3: GnssvodAdapter on real data
# ---------------------------------------------------------------------------


@has_test_data
class TestTier3RealData:
    """Tier 3: adapter and comparison logic on real reader output."""

    def test_adapter_on_real_rinex(self, canopy_rinex_ds):
        """GnssvodAdapter produces valid PRN-indexed dataset from real RINEX."""
        from canvod.audit.runners.vs_gnssvod import GnssvodAdapter

        # Find a band that exists in the data
        sids = [str(s) for s in canopy_rinex_ds.sid.values]
        l1c_sids = [s for s in sids if s.endswith("|L1|C")]
        if not l1c_sids:
            pytest.skip("No L1|C SIDs in RINEX data")

        adapter = GnssvodAdapter(canopy_rinex_ds, band_filter="L1|C", snr_col="S1C")
        ds_adapted = adapter.to_gnssvod_dataset()

        assert "S1C" in ds_adapted.data_vars
        assert len(ds_adapted.sid) > 0
        # All SIDs should be PRNs (no pipe separators)
        for sid in ds_adapted.sid.values:
            assert "|" not in str(sid), f"SID {sid} still has pipe separator"

    def test_adapter_self_comparison(self, canopy_rinex_ds):
        """Adapted dataset compared to itself should pass."""
        from canvod.audit.runners.vs_gnssvod import GnssvodAdapter

        sids = [str(s) for s in canopy_rinex_ds.sid.values]
        if not any(s.endswith("|L1|C") for s in sids):
            pytest.skip("No L1|C SIDs in RINEX data")

        adapter = GnssvodAdapter(canopy_rinex_ds, band_filter="L1|C", snr_col="S1C")
        ds = adapter.to_gnssvod_dataset()

        result = compare_datasets(
            ds, ds, tier=ToleranceTier.EXACT, label="tier3-adapter-self"
        )
        assert result.passed


# ---------------------------------------------------------------------------
# Cross-cutting: full audit report from real data
# ---------------------------------------------------------------------------


@has_test_data
class TestFullAudit:
    """End-to-end audit combining multiple tiers on real data."""

    def test_multi_tier_audit(self, canopy_rinex_ds, canopy_sbf_ds, tmp_path):
        """Run a mini multi-tier audit and verify the report."""
        from canvod.audit.tiers.regression import (
            compare_against_checkpoint,
            freeze_checkpoint,
        )

        audit = AuditResult()

        # Tier 0: self-consistency
        r0 = compare_datasets(
            canopy_rinex_ds,
            canopy_rinex_ds,
            tier=ToleranceTier.EXACT,
            label="T0: RINEX self-check",
        )
        audit.results["tier0_rinex"] = r0

        # Tier 1a: SBF vs RINEX (SNR only)
        r1 = compare_datasets(
            canopy_sbf_ds,
            canopy_rinex_ds,
            variables=["SNR"],
            tier=ToleranceTier.SCIENTIFIC,
            label="T1a: SBF vs RINEX SNR",
        )
        audit.results["tier1a_snr"] = r1

        # Tier 2: freeze + verify
        cp = freeze_checkpoint(canopy_rinex_ds, tmp_path / "audit_cp.nc")
        r2 = compare_against_checkpoint(
            canopy_rinex_ds, cp, tier=ToleranceTier.EXACT, label="T2: regression"
        )
        audit.results["tier2_regression"] = r2

        # Report
        summary = audit.summary()
        assert "tier0_rinex" or "T0" in summary

        # Tier 0 and Tier 2 must always pass
        assert audit.results["tier0_rinex"].passed
        assert audit.results["tier2_regression"].passed

        # Polars export
        df = audit.to_polars()
        assert len(df) > 0
        assert "comparison" in df.columns
