"""Tests for anomaly z-score and classification functions."""

from __future__ import annotations

import math

import numpy as np
import pytest

from canvod.streamstats._types import AnomalyClassification
from canvod.streamstats.climatology.anomaly import (
    anomaly_zscore,
    anomaly_zscore_batch,
    classify_anomaly,
    classify_anomaly_batch,
)


class TestAnomalyZscore:
    """anomaly_zscore known values."""

    def test_known_zscore(self) -> None:
        assert anomaly_zscore(12.0, 10.0, 2.0) == pytest.approx(1.0)

    def test_negative_zscore(self) -> None:
        assert anomaly_zscore(8.0, 10.0, 2.0) == pytest.approx(-1.0)

    def test_zero_std_returns_nan(self) -> None:
        assert math.isnan(anomaly_zscore(5.0, 5.0, 0.0))

    def test_negative_std_returns_nan(self) -> None:
        assert math.isnan(anomaly_zscore(5.0, 5.0, -1.0))

    def test_nan_std_returns_nan(self) -> None:
        assert math.isnan(anomaly_zscore(5.0, 5.0, float("nan")))


class TestClassifyAnomaly:
    """classify_anomaly thresholds."""

    @pytest.mark.parametrize(
        ("z", "expected"),
        [
            (0.0, AnomalyClassification.NORMAL),
            (0.5, AnomalyClassification.NORMAL),
            (0.99, AnomalyClassification.NORMAL),
            (1.0, AnomalyClassification.MILD),
            (1.5, AnomalyClassification.MILD),
            (2.0, AnomalyClassification.MODERATE),
            (2.5, AnomalyClassification.MODERATE),
            (3.0, AnomalyClassification.SEVERE),
            (3.5, AnomalyClassification.SEVERE),
        ],
    )
    def test_thresholds(self, z: float, expected: AnomalyClassification) -> None:
        assert classify_anomaly(z) == expected

    def test_negative_z_classified_by_abs(self) -> None:
        assert classify_anomaly(-1.5) == AnomalyClassification.MILD
        assert classify_anomaly(-2.5) == AnomalyClassification.MODERATE
        assert classify_anomaly(-3.5) == AnomalyClassification.SEVERE

    def test_nan_returns_normal(self) -> None:
        assert classify_anomaly(float("nan")) == AnomalyClassification.NORMAL


class TestBatchZscore:
    """anomaly_zscore_batch matches scalar."""

    def test_batch_matches_scalar(self) -> None:
        values = np.array([12.0, 8.0, 10.0])
        means = np.array([10.0, 10.0, 10.0])
        stds = np.array([2.0, 2.0, 2.0])

        z = anomaly_zscore_batch(values, means, stds)
        assert z[0] == pytest.approx(anomaly_zscore(12.0, 10.0, 2.0))
        assert z[1] == pytest.approx(anomaly_zscore(8.0, 10.0, 2.0))
        assert z[2] == pytest.approx(anomaly_zscore(10.0, 10.0, 2.0))

    def test_zero_std_produces_nan(self) -> None:
        z = anomaly_zscore_batch(
            np.array([1.0, 2.0]),
            np.array([1.0, 1.0]),
            np.array([0.0, 1.0]),
        )
        assert math.isnan(z[0])
        assert z[1] == pytest.approx(1.0)


class TestBatchClassify:
    """classify_anomaly_batch matches scalar."""

    def test_batch_matches_scalar(self) -> None:
        z_scores = np.array([0.5, 1.5, 2.5, 3.5])
        result = classify_anomaly_batch(z_scores)
        expected = [
            AnomalyClassification.NORMAL,
            AnomalyClassification.MILD,
            AnomalyClassification.MODERATE,
            AnomalyClassification.SEVERE,
        ]
        for r, e in zip(result, expected):
            assert r == e

    def test_nan_propagation(self) -> None:
        z_scores = np.array([float("nan"), 1.5])
        result = classify_anomaly_batch(z_scores)
        # NaN → NORMAL (abs(NaN) comparisons are False)
        assert result[0] == AnomalyClassification.NORMAL
        assert result[1] == AnomalyClassification.MILD
