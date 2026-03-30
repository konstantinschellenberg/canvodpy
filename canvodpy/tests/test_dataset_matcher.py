"""Tests for DatasetMatcher — temporal alignment of auxiliary datasets."""

from __future__ import annotations

import unittest.mock

import numpy as np
import pytest
import xarray as xr
from canvodpy.orchestrator.matcher import DatasetMatcher

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ds(
    n_epochs: int = 10,
    n_sids: int = 3,
    freq_s: int = 30,
    start: float = 0.0,
) -> xr.Dataset:
    # Use float seconds so _get_temporal_interval can call float() on the diff.
    epochs = np.arange(start, start + n_epochs * freq_s, freq_s, dtype=float)
    sids = [f"G0{i}|L1|C" for i in range(1, n_sids + 1)]
    return xr.Dataset(
        {"SNR": (["epoch", "sid"], np.ones((n_epochs, n_sids)))},
        coords={"epoch": epochs, "sid": sids},
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestDatasetMatcherValidation:
    def test_no_auxiliary_raises(self):
        matcher = DatasetMatcher()
        canopy = _make_ds()
        with pytest.raises(ValueError, match="At least one auxiliary"):
            matcher.match_datasets(canopy)

    def test_canopy_missing_epoch_dim_raises(self):
        matcher = DatasetMatcher()
        bad = xr.Dataset({"x": (["time", "sid"], np.zeros((3, 2)))})
        aux = _make_ds()
        with pytest.raises(ValueError, match="missing required dimension"):
            matcher.match_datasets(bad, aux=aux)

    def test_auxiliary_missing_sid_dim_raises(self):
        matcher = DatasetMatcher()
        canopy = _make_ds()
        bad = xr.Dataset(
            {"x": (["epoch", "sv"], np.zeros((5, 2)))},
            coords={"epoch": np.arange(5, dtype=float)},
        )
        with pytest.raises(ValueError, match="missing required dimension"):
            matcher.match_datasets(canopy, aux=bad)

    def test_missing_interpolation_config_warns(self):
        matcher = DatasetMatcher()
        canopy = _make_ds(n_epochs=5, freq_s=30)
        # auxiliary at lower resolution (60 s), no interpolator_config attr
        aux = _make_ds(n_epochs=5, freq_s=60)
        with pytest.warns(UserWarning, match="missing interpolation configuration"):
            matcher.match_datasets(canopy, aux=aux)


# ---------------------------------------------------------------------------
# Temporal resolution matching
# ---------------------------------------------------------------------------


class TestDatasetMatcherInterpolation:
    def test_higher_res_auxiliary_resampled_to_canopy_epochs(self):
        """Auxiliary at 5 s resampled onto 30 s canopy grid."""
        matcher = DatasetMatcher()
        canopy = _make_ds(n_epochs=5, freq_s=30)
        aux = _make_ds(n_epochs=30, freq_s=5)  # higher resolution

        result = matcher.match_datasets(canopy, hi_res=aux)

        assert "hi_res" in result
        assert result["hi_res"].sizes["epoch"] == canopy.sizes["epoch"]

    def test_lower_res_no_config_falls_back_to_nearest(self):
        """Lower-res auxiliary without config gets nearest-neighbor + temporal_distance."""
        matcher = DatasetMatcher()
        canopy = _make_ds(n_epochs=10, freq_s=5)
        aux = _make_ds(n_epochs=5, freq_s=30)  # lower resolution, no config

        with pytest.warns(UserWarning):
            result = matcher.match_datasets(canopy, sp3=aux)

        assert "sp3" in result
        assert result["sp3"].sizes["epoch"] == canopy.sizes["epoch"]
        assert "sp3_temporal_distance" in result["sp3"]

    def test_lower_res_with_config_calls_interpolator(self):
        """Lower-res auxiliary with interpolator_config delegates to the interpolator."""
        matcher = DatasetMatcher()
        canopy = _make_ds(n_epochs=10, freq_s=5)
        aux = _make_ds(n_epochs=5, freq_s=30)
        mock_interpolated = _make_ds(n_epochs=10, freq_s=5)
        aux.attrs["interpolator_config"] = {"type": "hermite"}

        mock_interpolator = unittest.mock.MagicMock()
        mock_interpolator.interpolate.return_value = mock_interpolated

        with unittest.mock.patch(
            "canvodpy.orchestrator.matcher.create_interpolator_from_attrs",
            return_value=mock_interpolator,
        ):
            result = matcher.match_datasets(canopy, sp3=aux)

        mock_interpolator.interpolate.assert_called_once()
        assert "sp3" in result

    def test_full_round_trip_returns_all_named_keys(self):
        """Both auxiliary datasets appear in the returned dict."""
        matcher = DatasetMatcher()
        canopy = _make_ds(n_epochs=5, freq_s=30)
        hi = _make_ds(n_epochs=30, freq_s=5)  # higher-res
        lo = _make_ds(n_epochs=3, freq_s=60)  # lower-res, no config

        with pytest.warns(UserWarning):
            result = matcher.match_datasets(canopy, hi_res=hi, lo_res=lo)

        assert set(result.keys()) == {"hi_res", "lo_res"}
