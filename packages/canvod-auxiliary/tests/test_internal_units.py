"""
Tests for internal unit utilities.

Tests the UREG unit registry and custom unit definitions.
"""

import pytest
from canvod.auxiliary._internal import SPEEDOFLIGHT, UREG


def test_ureg_import():
    """Test that UREG can be imported and is a valid registry."""
    import pint

    # get_application_registry() returns an ApplicationRegistry
    assert isinstance(
        UREG,
        (
            pint.UnitRegistry,
            pint.registry.LazyRegistry,
            pint.registry.ApplicationRegistry,
        ),
    )
    # Verify it works by accessing a unit
    assert hasattr(UREG, "meter")
    meter = UREG.meter
    assert meter is not None


def test_custom_db_unit():
    """Test that custom dB unit is defined."""
    # dB is an offset unit in Pint - use Quantity constructor
    db_value = UREG.Quantity(10, "dB")
    assert str(db_value.units) == "decibel"


def test_custom_dbhz_unit():
    """Test that custom dBHz unit is defined."""
    # dBHz is a logarithmic unit - use Quantity constructor
    dbhz_value = UREG.Quantity(40, "dBHz")
    assert str(dbhz_value.units) == "dBHz"


def test_speedoflight_constant():
    """Test that SPEEDOFLIGHT constant is defined correctly."""
    # Speed of light should be approximately 299792458 m/s
    assert SPEEDOFLIGHT.magnitude == pytest.approx(299792458, rel=1e-6)

    # Check units are meter/second
    expected_units = UREG.meter / UREG.second
    assert SPEEDOFLIGHT.units == expected_units


def test_kilometer_to_meter_conversion():
    """Test basic unit conversion using UREG."""
    distance_km = 1 * UREG.kilometer
    distance_m = distance_km.to(UREG.meter)
    assert distance_m.magnitude == 1000


def test_microsecond_to_second_conversion():
    """Test time unit conversion using UREG."""
    time_us = 1000 * UREG.microsecond
    time_s = time_us.to(UREG.second)
    assert time_s.magnitude == pytest.approx(0.001)


def test_ureg_dimensionless_operations():
    """Test that dimensionless operations work correctly."""
    value = 100 * UREG.meter
    dimensionless = value.magnitude
    assert dimensionless == 100
    assert isinstance(dimensionless, (int, float))
