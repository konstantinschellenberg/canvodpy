"""Pytest configuration for canvod-readers tests."""

from pathlib import Path

import pytest


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")


@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture providing path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def rinex_files(test_data_dir):
    """Fixture providing list of available RINEX test files."""
    rinex_dir = test_data_dir / "01_Rosalia/02_canopy/01_GNSS/01_raw/25001"
    if not rinex_dir.exists():
        return []
    return sorted(rinex_dir.glob("*.rnx"))


@pytest.fixture
def sample_rinex_file(rinex_files):
    """Fixture providing a single RINEX file for testing."""
    if not rinex_files:
        pytest.skip("No RINEX test files found")
    return rinex_files[0]
