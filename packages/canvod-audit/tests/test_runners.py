"""Tests for audit runners."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from canvod.audit.runners.common import AuditResult
from canvod.audit.runners.vs_gnssvod import (
    GnssvodAdapter,
    gnssvod_df_to_xarray,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def prn_dataset():
    """canvodpy-style dataset with one code per PRN (trimmed RINEX)."""
    rng = np.random.default_rng(42)
    n_epochs = 50
    sids = ["G01|L1|C", "G02|L1|C", "G03|L1|C"]
    svs = ["G01", "G02", "G03"]
    bands = ["L1", "L1", "L1"]
    codes = ["C", "C", "C"]

    return xr.Dataset(
        {"SNR": (["epoch", "sid"], rng.uniform(20, 50, (n_epochs, len(sids))))},
        coords={
            "epoch": np.arange(
                np.datetime64("2025-01-01"),
                np.datetime64("2025-01-01") + np.timedelta64(n_epochs * 30, "s"),
                np.timedelta64(30, "s"),
            ),
            "sid": sids,
            "sv": ("sid", svs),
            "band": ("sid", bands),
            "code": ("sid", codes),
        },
    )


@pytest.fixture
def multi_code_dataset():
    """canvodpy-style dataset with multiple codes per PRN (untrimmed)."""
    rng = np.random.default_rng(42)
    n_epochs = 50
    sids = ["G01|L1|C", "G01|L1|W", "G02|L1|C"]
    svs = ["G01", "G01", "G02"]
    bands = ["L1", "L1", "L1"]
    codes = ["C", "W", "C"]

    return xr.Dataset(
        {"SNR": (["epoch", "sid"], rng.uniform(20, 50, (n_epochs, len(sids))))},
        coords={
            "epoch": np.arange(
                np.datetime64("2025-01-01"),
                np.datetime64("2025-01-01") + np.timedelta64(n_epochs * 30, "s"),
                np.timedelta64(30, "s"),
            ),
            "sid": sids,
            "sv": ("sid", svs),
            "band": ("sid", bands),
            "code": ("sid", codes),
        },
    )


@pytest.fixture
def gnssvod_dataframe():
    """Simulated gnssvod output as a pandas DataFrame."""
    import pandas as pd

    rng = np.random.default_rng(42)
    epochs = pd.date_range("2025-01-01", periods=50, freq="30s")
    svs = ["G01", "G02", "G03"]

    rows = []
    for epoch in epochs:
        for sv in svs:
            rows.append(
                {
                    "Epoch": epoch,
                    "SV": sv,
                    "S1C": rng.uniform(20, 50),
                    "Azimuth": rng.uniform(0, 360),
                    "Elevation": rng.uniform(5, 90),
                }
            )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests: SID → PRN mapping
# ---------------------------------------------------------------------------


def test_adapter_single_code(prn_dataset):
    """GnssvodAdapter maps SIDs to PRNs when there's one code per PRN."""
    adapter = GnssvodAdapter(prn_dataset, band_filter="L1|C", snr_col="S1C")
    ds = adapter.to_gnssvod_dataset()
    assert list(ds.sid.values) == ["G01", "G02", "G03"]
    assert "S1C" in ds.data_vars
    assert ds["S1C"].shape == prn_dataset["SNR"].shape


def test_adapter_rejects_duplicate_prn():
    """GnssvodAdapter raises when band filter matches multiple SIDs for same PRN."""
    rng = np.random.default_rng(42)
    n_epochs = 10
    # Two SIDs for G01 that both end with "|C" — simulates a bad/broad filter
    sids = ["G01|L1|C", "G01|L2|C", "G02|L1|C"]
    ds = xr.Dataset(
        {"SNR": (["epoch", "sid"], rng.uniform(20, 50, (n_epochs, 3)))},
        coords={
            "epoch": np.arange(
                np.datetime64("2025-01-01"),
                np.datetime64("2025-01-01") + np.timedelta64(n_epochs * 30, "s"),
                np.timedelta64(30, "s"),
            ),
            "sid": sids,
        },
    )
    # Band filter "C" is too broad — matches both G01|L1|C and G01|L2|C
    with pytest.raises(ValueError, match="Duplicate PRNs"):
        GnssvodAdapter(ds, band_filter="C", snr_col="S1C")


def test_adapter_converts_angles(prn_dataset):
    """GnssvodAdapter converts phi/theta to Azimuth/Elevation in degrees."""
    rng = np.random.default_rng(99)
    n_epochs, n_sids = prn_dataset["SNR"].shape
    ds = prn_dataset.assign(
        phi=xr.DataArray(
            rng.uniform(0, 2 * np.pi, (n_epochs, n_sids)),
            dims=["epoch", "sid"],
        ),
        theta=xr.DataArray(
            rng.uniform(0, np.pi / 2, (n_epochs, n_sids)),
            dims=["epoch", "sid"],
        ),
    )
    adapter = GnssvodAdapter(ds, band_filter="L1|C", snr_col="S1C")
    result = adapter.to_gnssvod_dataset()

    assert "Azimuth" in result.data_vars
    assert "Elevation" in result.data_vars
    # Azimuth should be in [0, 360)
    az = result["Azimuth"].values
    assert np.all(az[~np.isnan(az)] >= 0)
    assert np.all(az[~np.isnan(az)] < 360)
    # Elevation should be in [0, 90]
    el = result["Elevation"].values
    assert np.all(el[~np.isnan(el)] >= 0)
    assert np.all(el[~np.isnan(el)] <= 90)


# ---------------------------------------------------------------------------
# Tests: gnssvod DataFrame conversion
# ---------------------------------------------------------------------------


def test_gnssvod_df_to_xarray(gnssvod_dataframe):
    """gnssvod DataFrame converts to xarray with correct dims."""
    ds = gnssvod_df_to_xarray(gnssvod_dataframe)
    assert "epoch" in ds.dims
    assert "sid" in ds.dims
    assert len(ds.sid) == 3
    assert len(ds.epoch) == 50
    assert "S1C" in ds.data_vars
    assert "Azimuth" in ds.data_vars


def test_gnssvod_df_to_xarray_multiindex(gnssvod_dataframe):
    """Handles MultiIndex DataFrames (gnssvod's native format)."""
    df_mi = gnssvod_dataframe.set_index(["Epoch", "SV"])
    ds = gnssvod_df_to_xarray(df_mi)
    assert len(ds.sid) == 3
    assert len(ds.epoch) == 50


def test_gnssvod_df_to_xarray_value_cols(gnssvod_dataframe):
    """Selecting specific value columns works."""
    ds = gnssvod_df_to_xarray(gnssvod_dataframe, value_cols=["S1C"])
    assert list(ds.data_vars) == ["S1C"]


# ---------------------------------------------------------------------------
# Tests: AuditResult
# ---------------------------------------------------------------------------


def test_audit_result_empty():
    """Empty AuditResult passes."""
    r = AuditResult()
    assert r.passed is True
    assert "0/0 passed" in r.summary()


def test_audit_result_collects(synthetic_ds):
    """AuditResult collects multiple ComparisonResults."""
    from canvod.audit.core import compare_datasets
    from canvod.audit.tolerances import ToleranceTier

    r = AuditResult()
    r1 = compare_datasets(
        synthetic_ds, synthetic_ds, tier=ToleranceTier.EXACT, label="a"
    )
    r2 = compare_datasets(
        synthetic_ds, synthetic_ds, tier=ToleranceTier.EXACT, label="b"
    )
    r.results["a"] = r1
    r.results["b"] = r2

    assert r.passed is True
    assert "2/2 passed" in r.summary()


# ---------------------------------------------------------------------------
# Tests: RinexTrimmer (unit tests, no gfzrnx needed)
# ---------------------------------------------------------------------------


def test_trimmer_init():
    """RinexTrimmer validates keep_systems vs keep_obs_codes."""
    from canvod.audit.rinex_trimmer import RinexTrimmer

    t = RinexTrimmer(
        keep_systems=["G"],
        keep_obs_codes={"G": ["C1C", "S1C"]},
    )
    assert t.keep_systems == ["G"]


def test_trimmer_init_missing_codes():
    """RinexTrimmer raises if system has no obs codes."""
    from canvod.audit.rinex_trimmer import RinexTrimmer

    with pytest.raises(ValueError, match="no obs codes"):
        RinexTrimmer(keep_systems=["G", "E"], keep_obs_codes={"G": ["C1C"]})


def test_trimmer_gfzrnx_command():
    """gfzrnx_command builds the correct command."""
    from canvod.audit.rinex_trimmer import RinexTrimmer

    t = RinexTrimmer(
        keep_systems=["E", "G"],
        keep_obs_codes={
            "E": ["C1C", "S1C"],
            "G": ["C1C", "S1C", "C2W"],
        },
    )
    cmd = t.gfzrnx_command(["a.rnx", "b.rnx"], "out.rnx")
    assert cmd[0] == "gfzrnx"
    assert "-finp" in cmd
    assert "-satsys" in cmd
    satsys_idx = cmd.index("-satsys")
    assert cmd[satsys_idx + 1] == "EG"
    obs_idx = cmd.index("-obs_types")
    assert "E:C1C,S1C" in cmd[obs_idx + 1]
    assert "G:C1C,S1C,C2W" in cmd[obs_idx + 1]
    assert "-splice_direct" in cmd


def test_ready_made_configs():
    """Ready-made configs create valid trimmers."""
    from canvod.audit.rinex_trimmer import gps_galileo_l1_l2, gps_l1_only

    t1 = gps_galileo_l1_l2()
    assert "G" in t1.keep_systems
    assert "E" in t1.keep_systems

    t2 = gps_l1_only()
    assert t2.keep_systems == ["G"]
    assert "S1C" in t2.keep_obs_codes["G"]


# ---------------------------------------------------------------------------
# Tests: gnssvod fillna merge replication
# ---------------------------------------------------------------------------


def test_gnssvod_merge_codes_lexicographic_priority():
    """Merge replicates gnssvod's fillna: lex-first code wins, gaps filled."""
    from canvod.audit.runners.vs_gnssvod import gnssvod_merge_codes

    n_epochs = 5
    # G01 has L1|C and L1|W; G02 has only L1|C
    sids = ["G01|L1|C", "G01|L1|W", "G02|L1|C"]
    snr_c = np.array([10.0, np.nan, 30.0, np.nan, 50.0])  # G01 S1C
    snr_w = np.array([np.nan, 22.0, np.nan, 44.0, 55.0])  # G01 S1W
    snr_g02 = np.array([60.0, 70.0, 80.0, 90.0, 100.0])

    ds = xr.Dataset(
        {"SNR": (["epoch", "sid"], np.column_stack([snr_c, snr_w, snr_g02]))},
        coords={
            "epoch": np.arange(n_epochs),
            "sid": sids,
        },
    )

    merged = gnssvod_merge_codes(ds, band_num="1")
    assert list(merged.sid.values) == ["G01", "G02"]

    g01 = merged["SNR"].sel(sid="G01").values
    # epoch 0: C=10 (used), W=NaN → 10
    # epoch 1: C=NaN, W=22 → 22
    # epoch 2: C=30, W=NaN → 30
    # epoch 3: C=NaN, W=44 → 44
    # epoch 4: C=50, W=55 → 50 (C wins, both present)
    np.testing.assert_array_equal(g01, [10.0, 22.0, 30.0, 44.0, 50.0])

    g02 = merged["SNR"].sel(sid="G02").values
    np.testing.assert_array_equal(g02, snr_g02)


# ---------------------------------------------------------------------------
# Tests: band auto-detection and trimmer auto-detection
# ---------------------------------------------------------------------------


def test_detect_bands():
    """_detect_bands correctly groups obs codes by band number."""
    from canvod.audit.rinex_trimmer import _detect_bands

    codes = ["C1C", "L1C", "S1C", "C1W", "L1W", "S1W", "C2W", "L2W", "S2W"]
    bands = _detect_bands(codes)
    assert "1" in bands
    assert "2" in bands
    assert sorted(bands["1"]) == ["C", "W"]
    assert bands["2"] == ["W"]


def test_pick_code_lexicographic():
    """Default code selection is lexicographic (matches gnssvod)."""
    from canvod.audit.rinex_trimmer import _pick_code

    assert _pick_code(["C", "W", "X"], None) == "C"
    assert _pick_code(["W", "X"], None) == "W"


def test_pick_code_preferred():
    """Prefer list overrides lexicographic order."""
    from canvod.audit.rinex_trimmer import _pick_code

    assert _pick_code(["C", "W", "X"], ["W", "C"]) == "W"
    assert _pick_code(["C", "X"], ["W", "C"]) == "C"  # W not available
