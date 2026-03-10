"""Tests for audit runners."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from canvod.audit.runners.common import AuditResult
from canvod.audit.runners.vs_gnssvod import (
    _canvodpy_to_prn,
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


def test_canvodpy_to_prn_single_code(prn_dataset):
    """SID→PRN mapping works when there's one code per PRN."""
    ds = _canvodpy_to_prn(prn_dataset)
    assert list(ds.sid.values) == ["G01", "G02", "G03"]
    assert ds["SNR"].shape == prn_dataset["SNR"].shape


def test_canvodpy_to_prn_rejects_multi_code(multi_code_dataset):
    """SID→PRN mapping raises on duplicate PRNs (untrimmed RINEX)."""
    with pytest.raises(ValueError, match="Duplicate PRNs"):
        _canvodpy_to_prn(multi_code_dataset)


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
