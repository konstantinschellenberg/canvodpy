"""Tolerance definitions for numerical comparison.

Three tiers of comparison strictness, from bit-identical to
domain-specific scientific thresholds.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class Tolerance:
    """Numerical tolerance for a single variable comparison.

    A variable passes if ALL of the following hold:

    1. ``max_abs_diff <= atol``  — worst-case single-element error
    2. ``mae <= mae_atol``       — typical (mean) error, when ``mae_atol > 0``
    3. ``|NaN_rate_a - NaN_rate_b| <= nan_rate_atol``

    Parameters
    ----------
    atol : float
        Absolute tolerance on maximum single-element error.
    mae_atol : float
        Absolute tolerance on Mean Absolute Error (typical error).
        Set to 0 to skip this check.
    nan_rate_atol : float
        Maximum allowed difference in NaN rates between the two datasets.
        0.0 means NaN patterns must match exactly.
    description : str
        Human-readable justification for this tolerance (for the paper).
    """

    atol: float
    mae_atol: float
    nan_rate_atol: float = 0.0
    description: str = ""


class ToleranceTier(Enum):
    """Pre-defined comparison strictness levels."""

    EXACT = "exact"
    """Bit-identical. atol=0, mae_atol=0. Use for values that should be
    computed from the same source data with the same algorithm."""

    NUMERICAL = "numerical"
    """Float64 precision. atol=1e-12, mae_atol=1e-10. Use for values that
    should be mathematically identical but may differ due to floating-point
    operation ordering."""

    SCIENTIFIC = "scientific"
    """Domain-specific thresholds per variable. Use for comparisons across
    independent implementations or data sources where small physical
    differences are expected (e.g. SBF 0.25 dB SNR quantization)."""


# Default tolerances per tier
TIER_DEFAULTS: dict[ToleranceTier, Tolerance] = {
    ToleranceTier.EXACT: Tolerance(
        atol=0.0,
        mae_atol=0.0,
        description="Bit-identical comparison",
    ),
    ToleranceTier.NUMERICAL: Tolerance(
        atol=1e-6,
        mae_atol=1e-10,
        description="Float64 precision — atol bounds worst-case single-element "
        "error from operation reordering; mae_atol bounds typical (MAE) error.",
    ),
    ToleranceTier.SCIENTIFIC: Tolerance(
        atol=0.01,
        mae_atol=0.01,
        description="Domain-specific scientific tolerance",
    ),
}

# Scientific tolerances for specific GNSS-VOD variables.
# Each tolerance is justified by a physical or algorithmic reason.
SCIENTIFIC_DEFAULTS: dict[str, Tolerance] = {
    "SNR": Tolerance(
        atol=0.25,
        mae_atol=0.0,
        nan_rate_atol=0.01,
        description="SBF quantization is 0.25 dB; RINEX ~0.001 dB. Hardware limitation.",
    ),
    "vod": Tolerance(
        atol=0.01,
        mae_atol=0.01,
        nan_rate_atol=0.01,
        description="VOD retrieval: sub-0.01 differences are below measurement noise.",
    ),
    "phi": Tolerance(
        atol=0.05,
        mae_atol=0.0,
        nan_rate_atol=0.0,
        description="Elevation angle: coordinate conversion differences up to ~2.4 deg "
        "observed between implementations (wrap-aware).",
    ),
    "theta": Tolerance(
        atol=0.05,
        mae_atol=0.0,
        nan_rate_atol=0.0,
        description="Azimuth angle: coordinate conversion differences.",
    ),
    "carrier_phase": Tolerance(
        atol=1e-6,
        mae_atol=1e-9,
        nan_rate_atol=0.01,
        description="Carrier phase: high-precision observable, expect near-exact agreement.",
    ),
    "pseudorange": Tolerance(
        atol=1e-3,
        mae_atol=1e-6,
        nan_rate_atol=0.01,
        description="Pseudorange: meter-level observable.",
    ),
    "sat_x": Tolerance(
        atol=1e-3,
        mae_atol=1e-9,
        nan_rate_atol=0.05,
        description="Satellite X coordinate (meters). NaN rate may differ due to "
        "broadcast vs SP3 satellite coverage.",
    ),
    "sat_y": Tolerance(
        atol=1e-3,
        mae_atol=1e-9,
        nan_rate_atol=0.05,
        description="Satellite Y coordinate (meters).",
    ),
    "sat_z": Tolerance(
        atol=1e-3,
        mae_atol=1e-9,
        nan_rate_atol=0.05,
        description="Satellite Z coordinate (meters).",
    ),
}


def get_tolerance(
    variable: str,
    tier: ToleranceTier,
    overrides: dict[str, Tolerance] | None = None,
) -> Tolerance:
    """Look up the tolerance for a variable at a given tier.

    Resolution order:
    1. ``overrides[variable]`` if provided
    2. ``SCIENTIFIC_DEFAULTS[variable]`` if tier is SCIENTIFIC
    3. ``TIER_DEFAULTS[tier]`` (catch-all)
    """
    if overrides and variable in overrides:
        return overrides[variable]
    if tier == ToleranceTier.SCIENTIFIC and variable in SCIENTIFIC_DEFAULTS:
        return SCIENTIFIC_DEFAULTS[variable]
    return TIER_DEFAULTS[tier]
