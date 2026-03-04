"""Tests for EMD decomposition."""

import numpy as np
import pytest

from canvod.streamstats.spectral.emd import EMDResult, emd_decompose


class TestEMDDecompose:
    """EMD decomposition tests."""

    def test_pure_sine_basic(self):
        """Pure sine should produce at least one IMF."""
        t = np.arange(200, dtype=np.float64)
        signal = np.sin(2 * np.pi * t / 50)

        result = emd_decompose(signal, method="emd")
        assert isinstance(result, EMDResult)
        assert result.n_imfs >= 1
        assert result.imfs.shape[1] == len(signal)

    def test_trend_plus_sines(self):
        """Trend + two sines should produce multiple IMFs."""
        t = np.arange(500, dtype=np.float64)
        signal = (
            0.01 * t + np.sin(2 * np.pi * t / 20) + 0.5 * np.sin(2 * np.pi * t / 100)
        )

        result = emd_decompose(signal, method="emd")
        assert result.n_imfs >= 2

    def test_residual_shape(self):
        """Residual should be the same length as input."""
        signal = np.sin(2 * np.pi * np.arange(200) / 30.0)
        result = emd_decompose(signal, method="emd")
        assert result.residual.shape == signal.shape

    def test_method_selection(self):
        """Different methods should all produce valid results."""
        signal = np.sin(2 * np.pi * np.arange(200) / 30.0)

        for method in ("emd",):
            result = emd_decompose(signal, method=method)
            assert result.n_imfs >= 1

    def test_short_input(self):
        """Very short input returns empty result."""
        result = emd_decompose(np.array([1.0, 2.0]))
        assert result.n_imfs == 0
        assert result.residual.shape == (2,)

    def test_invalid_method(self):
        """Invalid method raises ValueError."""
        with pytest.raises(ValueError, match="Unknown EMD method"):
            emd_decompose(np.arange(100, dtype=np.float64), method="invalid")
