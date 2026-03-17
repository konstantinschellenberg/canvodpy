"""Tests for SIDPatternFilter."""

import numpy as np
import pytest
import xarray as xr

from canvod.grids.analysis.filtering import SIDPatternFilter


@pytest.fixture
def sample_ds():
    """Dataset with realistic SID dimension."""
    sids = ["G01|L1|C", "G01|L2|L", "G05|L1|C", "E01|E1|C", "E05|E5a|Q", "R01|G1|C"]
    epochs = np.arange(10)
    snr = np.random.default_rng(42).standard_normal((len(epochs), len(sids)))
    return xr.Dataset(
        {"SNR": (["epoch", "sid"], snr)},
        coords={"epoch": epochs, "sid": sids},
    )


class TestSIDPatternFilter:
    def test_filter_by_system(self, sample_ds):
        filt = SIDPatternFilter(system="G")
        result = filt.filter_dataset(sample_ds)
        assert result is not None
        assert len(result.sid) == 3
        assert all(str(s).startswith("G") for s in result.sid.values)

    def test_filter_by_band(self, sample_ds):
        filt = SIDPatternFilter(band="L1")
        result = filt.filter_dataset(sample_ds)
        assert result is not None
        assert len(result.sid) == 2
        assert all("|L1|" in str(s) for s in result.sid.values)

    def test_filter_by_code(self, sample_ds):
        filt = SIDPatternFilter(code="C")
        result = filt.filter_dataset(sample_ds)
        assert result is not None
        assert len(result.sid) == 4

    def test_filter_combined(self, sample_ds):
        filt = SIDPatternFilter(system="G", band="L1", code="C")
        result = filt.filter_dataset(sample_ds)
        assert result is not None
        assert len(result.sid) == 2
        assert set(result.sid.values) == {"G01|L1|C", "G05|L1|C"}

    def test_no_match_returns_none(self, sample_ds):
        filt = SIDPatternFilter(system="C")  # BeiDou not in fixture
        result = filt.filter_dataset(sample_ds)
        assert result is None

    def test_no_filters_keeps_all(self, sample_ds):
        filt = SIDPatternFilter()
        result = filt.filter_dataset(sample_ds)
        assert result is not None
        assert len(result.sid) == len(sample_ds.sid)

    def test_compute_mask(self, sample_ds):
        filt = SIDPatternFilter(system="E")
        mask = filt.compute_mask(sample_ds["SNR"])
        assert mask.shape == sample_ds["SNR"].shape
        # Galileo SIDs are at indices 3, 4
        assert mask.isel(epoch=0, sid=3).item() is True
        assert mask.isel(epoch=0, sid=0).item() is False

    def test_apply_method(self, sample_ds):
        filt = SIDPatternFilter(system="G")
        result = filt.apply(sample_ds, "SNR")
        assert f"SNR_filtered_{filt.name}" in result

    def test_missing_sid_dim_raises(self):
        ds = xr.Dataset({"x": (["time"], [1, 2, 3])})
        filt = SIDPatternFilter(system="G")
        with pytest.raises(ValueError, match="sid"):
            filt.filter_dataset(ds)
