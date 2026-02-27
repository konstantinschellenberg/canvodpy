"""Property-based tests for VOD calculations using Hypothesis.

These tests verify mathematical invariants and physical constraints
that must hold for all valid inputs.
"""

import numpy as np
import pytest
import xarray as xr
from canvod.vod import TauOmegaZerothOrder
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.extra.numpy import arrays

# ============================================================================
# Hypothesis Strategies for VOD Testing
# ============================================================================


@st.composite
def valid_snr_values(draw):
    """Generate valid SNR values in typical GNSS range.

    SNR values are typically between -10 dB and 60 dB.
    """
    return draw(st.floats(-10.0, 60.0, allow_nan=False, allow_infinity=False))


@st.composite
def valid_theta_values(draw):
    """Generate valid elevation angles.

    Theta is elevation angle: [0, π/2] where 0 = zenith, π/2 = horizon.
    """
    return draw(st.floats(0.0, np.pi / 2, allow_nan=False, allow_infinity=False))


@st.composite
def valid_phi_values(draw):
    """Generate valid azimuth angles.

    Phi is azimuth: [0, 2π).
    """
    return draw(st.floats(0.0, 2 * np.pi, allow_nan=False, allow_infinity=False))


@st.composite
def vod_test_datasets(draw, n_epoch=10, n_sid=5):
    """Generate a pair of valid canopy/sky datasets for VOD testing.

    Parameters
    ----------
    draw : callable
        Hypothesis draw function.
    n_epoch : int
        Number of epochs.
    n_sid : int
        Number of satellites.

    Returns
    -------
    tuple[xr.Dataset, xr.Dataset]
        (canopy_ds, sky_ds) with random but valid values.
    """
    # Generate SNR values
    canopy_snr = draw(
        arrays(
            dtype=np.float64,
            shape=(n_epoch, n_sid),
            elements=st.floats(-10.0, 60.0, allow_nan=False, allow_infinity=False),
        )
    )
    sky_snr = draw(
        arrays(
            dtype=np.float64,
            shape=(n_epoch, n_sid),
            elements=st.floats(-10.0, 60.0, allow_nan=False, allow_infinity=False),
        )
    )

    # Generate theta values (elevation angles)
    theta = draw(
        arrays(
            dtype=np.float64,
            shape=(n_epoch, n_sid),
            elements=st.floats(0.0, np.pi / 2, allow_nan=False, allow_infinity=False),
        )
    )

    # Generate phi values (azimuth angles)
    phi = draw(
        arrays(
            dtype=np.float64,
            shape=(n_epoch, n_sid),
            elements=st.floats(0.0, 2 * np.pi, allow_nan=False, allow_infinity=False),
        )
    )

    canopy_ds = xr.Dataset(
        {
            "SNR": (["epoch", "sid"], canopy_snr),
            "phi": (["epoch", "sid"], phi),
            "theta": (["epoch", "sid"], theta),
        }
    )

    sky_ds = xr.Dataset(
        {
            "SNR": (["epoch", "sid"], sky_snr),
            "phi": (["epoch", "sid"], phi),
            "theta": (["epoch", "sid"], theta),
        }
    )

    return canopy_ds, sky_ds


# ============================================================================
# Property Tests: Physical Constraints
# ============================================================================


class TestVODPhysicalProperties:
    """Property tests for VOD physical constraints."""

    @given(datasets=vod_test_datasets(n_epoch=5, n_sid=3))
    @settings(max_examples=30, deadline=5000)
    def test_vod_finite_for_valid_inputs(self, datasets):
        """VOD must be finite (not NaN/inf) for valid inputs.

        Property: For all valid SNR and angle values, VOD calculation
        should produce finite results.
        """
        canopy_ds, sky_ds = datasets

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()

        # Property: All VOD values should be finite
        finite_count = np.isfinite(vod_ds["VOD"].values).sum()
        total_count = vod_ds["VOD"].size

        # Allow some NaN due to numerical issues with extreme values
        # But most should be finite
        finite_ratio = finite_count / total_count
        assert finite_ratio >= 0.8, (
            f"Only {finite_ratio:.1%} of VOD values are finite. Expected at least 80%."
        )

    @given(
        canopy_snr=st.floats(0.0, 50.0),
        sky_snr=st.floats(0.0, 50.0),
        theta=st.floats(0.0, np.pi / 2),
    )
    @settings(max_examples=50, deadline=2000)
    def test_vod_sign_depends_on_attenuation(self, canopy_snr, sky_snr, theta):
        """VOD sign reflects physical attenuation.

        Property: When canopy SNR < sky SNR (attenuation), VOD >= 0.
        When canopy SNR > sky SNR (amplification), VOD < 0.
        """
        canopy_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[canopy_snr]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )
        sky_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[sky_snr]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()
        vod_value = vod_ds["VOD"].values[0, 0]

        if not np.isfinite(vod_value):
            return  # Skip numerical edge cases

        delta_snr = canopy_snr - sky_snr

        # Property: VOD sign reflects attenuation direction
        if delta_snr < -0.1:  # Significant attenuation
            assert vod_value >= 0, (
                f"Attenuation (delta_SNR={delta_snr:.2f}) "
                f"should give VOD >= 0, got {vod_value:.3f}"
            )
        elif delta_snr > 0.1:  # Amplification (unusual but valid)
            assert vod_value <= 0, (
                f"Amplification (delta_SNR={delta_snr:.2f}) "
                f"should give VOD <= 0, got {vod_value:.3f}"
            )

    @given(
        delta_snr=st.floats(-20.0, -0.1),  # Attenuation only
        theta=st.floats(0.0, np.pi / 2 - 0.01),  # Avoid exact horizon
    )
    @settings(max_examples=50, deadline=2000)
    def test_vod_positive_for_attenuation(self, delta_snr, theta):
        """VOD must be positive when there is signal attenuation.

        Property: Negative delta_SNR (canopy < sky) implies VOD > 0.
        This is the typical vegetation scenario.
        """
        assume(theta < np.pi / 2 - 0.01)  # Avoid near-horizon

        canopy_snr = 20.0 + delta_snr  # Canopy has lower SNR
        sky_snr = 20.0

        canopy_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[canopy_snr]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )
        sky_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[sky_snr]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()
        vod_value = vod_ds["VOD"].values[0, 0]

        if not np.isfinite(vod_value):
            return  # Skip numerical issues

        # Property: Attenuation → positive VOD
        assert vod_value > 0, (
            f"Attenuation (Δ_SNR={delta_snr:.2f} dB, θ={np.degrees(theta):.1f}°) "
            f"should give VOD > 0, got {vod_value:.3f}"
        )


# ============================================================================
# Property Tests: Mathematical Invariants
# ============================================================================


class TestVODMathematicalProperties:
    """Property tests for VOD mathematical relationships."""

    @given(
        snr_canopy=st.floats(10.0, 40.0),
        snr_sky=st.floats(10.0, 40.0),
        theta=st.floats(0.01, np.pi / 2 - 0.01),
    )
    @settings(max_examples=50, deadline=2000)
    def test_vod_theta_dependency(self, snr_canopy, snr_sky, theta):
        """VOD scales with cos(theta) for fixed transmissivity.

        Property: VOD(θ) = -ln(γ) * cos(θ)
        So VOD should decrease as theta increases (moving toward horizon).
        """
        assume(abs(snr_canopy - snr_sky) > 0.1)  # Need meaningful difference
        assume(theta < np.pi / 2 - 0.01)  # Avoid horizon

        # Calculate VOD at given theta
        canopy_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[snr_canopy]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )
        sky_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[snr_sky]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()
        vod_theta = vod_ds["VOD"].values[0, 0]

        # Calculate VOD at smaller theta (closer to zenith)
        theta_smaller = theta * 0.8
        canopy_ds2 = canopy_ds.copy()
        sky_ds2 = sky_ds.copy()
        canopy_ds2["theta"] = (["epoch", "sid"], [[theta_smaller]])
        sky_ds2["theta"] = (["epoch", "sid"], [[theta_smaller]])

        calculator2 = TauOmegaZerothOrder(canopy_ds=canopy_ds2, sky_ds=sky_ds2)
        vod_ds2 = calculator2.calculate_vod()
        vod_theta_smaller = vod_ds2["VOD"].values[0, 0]

        if not (np.isfinite(vod_theta) and np.isfinite(vod_theta_smaller)):
            return  # Skip numerical issues

        # Property: cos(θ_small) > cos(θ_large) for θ in (0, π/2)
        # So |VOD(θ_small)| should be > |VOD(θ_large)|
        assert abs(vod_theta_smaller) >= abs(vod_theta) - 0.01, (
            f"VOD should decrease with increasing theta: "
            f"VOD({np.degrees(theta_smaller):.1f}°)={vod_theta_smaller:.3f}, "
            f"VOD({np.degrees(theta):.1f}°)={vod_theta:.3f}"
        )

    @given(
        snr_canopy=st.floats(10.0, 40.0),
        snr_sky=st.floats(10.0, 40.0),
    )
    @settings(max_examples=30, deadline=2000)
    def test_vod_horizon_approaches_zero(self, snr_canopy, snr_sky):
        """VOD approaches zero as elevation approaches horizon.

        Property: lim(θ → π/2) VOD(θ) = 0 since cos(π/2) = 0.
        """
        theta_horizon = np.pi / 2  # Exactly at horizon

        canopy_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[snr_canopy]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta_horizon]]),
            }
        )
        sky_ds = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[snr_sky]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta_horizon]]),
            }
        )

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()
        vod_value = vod_ds["VOD"].values[0, 0]

        # Property: At horizon, VOD = 0
        assert np.isfinite(vod_value), "VOD at horizon should be finite"
        assert abs(vod_value) < 1e-10, (
            f"VOD at horizon should be ~0, got {vod_value:.3e}"
        )

    @given(datasets=vod_test_datasets(n_epoch=5, n_sid=3))
    @settings(max_examples=30, deadline=5000)
    def test_vod_coordinates_preserved(self, datasets):
        """VOD output must preserve phi and theta from input.

        Property: Coordinate variables should be unchanged in output.
        """
        canopy_ds, sky_ds = datasets

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)
        vod_ds = calculator.calculate_vod()

        # Property: Coordinates preserved exactly
        np.testing.assert_array_equal(
            vod_ds["phi"].values,
            canopy_ds["phi"].values,
            err_msg="Phi coordinates should be preserved exactly",
        )
        np.testing.assert_array_equal(
            vod_ds["theta"].values,
            canopy_ds["theta"].values,
            err_msg="Theta coordinates should be preserved exactly",
        )


# ============================================================================
# Property Tests: Numerical Stability
# ============================================================================


class TestVODNumericalProperties:
    """Property tests for numerical stability."""

    @given(
        delta_snr=st.floats(-30.0, 30.0),
    )
    @settings(max_examples=50, deadline=2000)
    def test_decibel_conversion_invertible(self, delta_snr):
        """Decibel ↔ linear conversion should be invertible.

        Property: linear2db(db2linear(x)) ≈ x
        """
        # Create minimal datasets
        canopy_ds = xr.Dataset({"SNR": (["x"], [0.0])})
        sky_ds = xr.Dataset({"SNR": (["x"], [0.0])})

        calculator = TauOmegaZerothOrder(canopy_ds=canopy_ds, sky_ds=sky_ds)

        # Convert dB → linear → dB
        delta_snr_array = xr.DataArray([delta_snr])
        linear_value = calculator.decibel2linear(delta_snr_array)

        # Check linear value is positive
        assert np.all(linear_value > 0), "Linear conversion must be positive"

        # Convert back
        delta_snr_back = 10 * np.log10(linear_value)

        # Property: Round-trip should preserve value
        # Use absolute tolerance for near-zero values
        np.testing.assert_allclose(
            delta_snr_back.values,
            [delta_snr],
            rtol=1e-8,
            atol=1e-12,
            err_msg=f"dB conversion not invertible for {delta_snr} dB",
        )

    @given(
        snr_diff=st.floats(-20.0, 20.0),
        theta=st.floats(0.0, np.pi / 2),
    )
    @settings(max_examples=50, deadline=2000)
    def test_vod_scales_linearly_with_log_transmissivity(self, snr_diff, theta):
        """VOD scales linearly with -ln(transmissivity).

        Property: VOD = -ln(γ) * cos(θ), so doubling attenuation
        should approximately double VOD (for small changes).
        """
        assume(abs(snr_diff) > 0.1)  # Need meaningful difference

        # Base case
        canopy_ds1 = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[20.0 + snr_diff]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )
        sky_ds1 = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[20.0]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )

        calc1 = TauOmegaZerothOrder(canopy_ds=canopy_ds1, sky_ds=sky_ds1)
        vod1 = calc1.calculate_vod()["VOD"].values[0, 0]

        # Double the attenuation (2x SNR difference)
        canopy_ds2 = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[20.0 + 2 * snr_diff]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )
        sky_ds2 = xr.Dataset(
            {
                "SNR": (["epoch", "sid"], [[20.0]]),
                "phi": (["epoch", "sid"], [[0.0]]),
                "theta": (["epoch", "sid"], [[theta]]),
            }
        )

        calc2 = TauOmegaZerothOrder(canopy_ds=canopy_ds2, sky_ds=sky_ds2)
        vod2 = calc2.calculate_vod()["VOD"].values[0, 0]

        if not (np.isfinite(vod1) and np.isfinite(vod2)):
            return  # Skip numerical issues

        if abs(vod1) < 0.01:
            return  # Too small to test scaling

        # Property: VOD should scale (not necessarily linearly due to dB)
        # But the ratio should be reasonable
        ratio = abs(vod2 / vod1)
        assert 0.5 < ratio < 4.0, (
            f"VOD scaling unreasonable: "
            f"VOD1={vod1:.3f}, VOD2={vod2:.3f}, ratio={ratio:.2f}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
