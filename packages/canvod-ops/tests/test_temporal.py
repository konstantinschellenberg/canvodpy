"""Tests for temporal aggregation operation."""

import numpy as np
import xarray as xr

from canvod.ops.temporal import TemporalAggregate, temporal_aggregate


class TestTemporalAggregate:
    def test_basic_aggregation(self, sample_ds: xr.Dataset):
        """1-second data aggregated to 1-minute bins should reduce epochs."""
        op = TemporalAggregate(freq="1min", method="mean")
        out, result = op(sample_ds)

        assert result.op_name == "temporal_aggregate"
        # 120 seconds -> 2 full minutes
        assert out.sizes["epoch"] == 2
        assert out.sizes["sid"] == sample_ds.sizes["sid"]
        assert "SNR" in out.data_vars
        assert "no-op" not in result.notes

    def test_preserves_attrs(self, sample_ds: xr.Dataset):
        """Dataset attrs must survive aggregation."""
        op = TemporalAggregate(freq="1min")
        out, _ = op(sample_ds)

        assert out.attrs["File Hash"] == "abc123"
        assert out.attrs["source"] == "test"

    def test_preserves_sid_only_coords(self, sample_ds: xr.Dataset):
        """sid-only coords (like sv) must pass through unchanged."""
        op = TemporalAggregate(freq="1min")
        out, _ = op(sample_ds)

        assert "sv" in out.coords
        assert out.coords["sv"].dims == ("sid",)
        np.testing.assert_array_equal(
            out.coords["sv"].values, sample_ds.coords["sv"].values
        )

    def test_aggregates_phi_theta(self, sample_ds: xr.Dataset):
        """phi and theta (epoch, sid) coords should be aggregated too."""
        op = TemporalAggregate(freq="1min")
        out, _ = op(sample_ds)

        assert "phi" in out.coords
        assert "theta" in out.coords
        assert out.coords["phi"].dims == ("epoch", "sid")
        assert out.coords["theta"].dims == ("epoch", "sid")

    def test_early_exit_coarse_data(self, coarse_ds: xr.Dataset):
        """If data is already at/coarser than freq, should return unchanged."""
        op = TemporalAggregate(freq="1min")
        out, result = op(coarse_ds)

        assert "no-op" in result.notes
        xr.testing.assert_identical(out, coarse_ds)

    def test_median_method(self, sample_ds: xr.Dataset):
        """median method should work without errors."""
        op = TemporalAggregate(freq="1min", method="median")
        out, result = op(sample_ds)

        assert out.sizes["epoch"] == 2
        assert result.parameters["method"] == "median"

    def test_invalid_method_raises(self):
        """Unsupported method should raise ValueError."""
        import pytest

        with pytest.raises(ValueError, match="Unsupported aggregation method"):
            TemporalAggregate(freq="1min", method="sum")

    def test_convenience_function(self, sample_ds: xr.Dataset):
        """temporal_aggregate convenience wrapper should work."""
        out = temporal_aggregate(sample_ds, freq="1min")
        assert out.sizes["epoch"] == 2

    def test_result_shapes(self, sample_ds: xr.Dataset):
        """OpResult should record correct input/output shapes."""
        op = TemporalAggregate(freq="1min")
        _, result = op(sample_ds)

        assert result.input_shape["epoch"] == 120
        assert result.input_shape["sid"] == 3
        assert result.output_shape["epoch"] == 2
        assert result.output_shape["sid"] == 3
        assert result.duration_seconds > 0
