"""
Tests for container module.

Tests GnssData dataclass and FileMetadata.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from canvod.auxiliary.container import FileMetadata, GnssData


class TestFileMetadata:
    """Tests for FileMetadata dataclass."""

    def test_create_metadata(self):
        """Test creating FileMetadata instance."""
        metadata = FileMetadata(
            date="2024015",
            agency="COD",
            product_type="final",
            local_dir=Path("/tmp/data"),
            ftp_server="ftp.example.com",
        )

        assert metadata.date == "2024015"
        assert metadata.agency == "COD"
        assert metadata.product_type == "final"
        assert metadata.local_dir == Path("/tmp/data")
        assert metadata.ftp_server == "ftp.example.com"


class TestGnssData:
    """Tests for GnssData dataclass."""

    def test_create_gnss_data_minimal(self):
        """Test creating GnssData with minimal fields."""
        ds = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1.0, 2.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data = GnssData(dataset=ds)

        assert data.dataset is not None
        assert data.dataframe is None
        assert "x" in data.dataset.data_vars

    def test_create_gnss_data_with_position(self):
        """Test creating GnssData with position data."""
        ds = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1234567.89, 1000000.0]]),
                "y": (["epoch", "sid"], [[9876543.21, 2000000.0]]),
                "z": (["epoch", "sid"], [[5555555.55, 3000000.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data = GnssData(dataset=ds)

        assert data.dataset["x"].values[0, 0] == 1234567.89
        assert data.dataset["y"].values[0, 0] == 9876543.21
        assert data.dataset["z"].values[0, 0] == 5555555.55

    def test_create_gnss_data_with_velocity(self):
        """Test creating GnssData with velocity data."""
        ds = xr.Dataset(
            {
                "vx": (["epoch", "sid"], [[100.0, 110.0]]),
                "vy": (["epoch", "sid"], [[200.0, 210.0]]),
                "vz": (["epoch", "sid"], [[300.0, 310.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data = GnssData(dataset=ds)

        assert data.dataset["vx"].values[0, 0] == 100.0
        assert data.dataset["vy"].values[0, 0] == 200.0
        assert data.dataset["vz"].values[0, 0] == 300.0

    def test_create_gnss_data_with_clock(self):
        """Test creating GnssData with clock correction."""
        ds = xr.Dataset(
            {
                "clock_offset": (["epoch", "sid"], [[1.23e-6, 2.34e-6]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data = GnssData(dataset=ds)

        assert data.dataset["clock_offset"].values[0, 0] == 1.23e-6

    def test_create_gnss_data_with_dataframe(self):
        """Test creating GnssData with both dataset and dataframe."""
        ds = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1.0, 2.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        df = pd.DataFrame({"sid": ["G01", "G02"], "value": [1.0, 2.0]})
        data = GnssData(dataset=ds, dataframe=df)

        assert data.dataset is not None
        assert data.dataframe is not None
        assert len(data.dataframe) == 2


class TestDataclassFeatures:
    """Tests for dataclass-specific features."""

    def test_gnss_data_equality(self):
        """Test GnssData equality comparison."""
        ds1 = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1000.0, 2000.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        ds2 = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1000.0, 2000.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data1 = GnssData(dataset=ds1)
        data2 = GnssData(dataset=ds2)

        # Note: xarray equality needs explicit comparison
        assert data1.dataset.equals(data2.dataset)

    def test_gnss_data_inequality(self):
        """Test GnssData inequality."""
        ds1 = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[1000.0, 2000.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        ds2 = xr.Dataset(
            {
                "x": (["epoch", "sid"], [[3000.0, 4000.0]]),
            },
            coords={
                "epoch": [np.datetime64("2024-01-15T12:00:00")],
                "sid": ["G01", "G02"],
            },
        )
        data1 = GnssData(dataset=ds1)
        data2 = GnssData(dataset=ds2)

        assert not data1.dataset.equals(data2.dataset)

    def test_file_metadata_repr(self):
        """Test FileMetadata string representation."""
        metadata = FileMetadata(
            date="2024015",
            agency="COD",
            product_type="final",
            local_dir=Path("/tmp/data"),
            ftp_server="ftp.example.com",
        )

        repr_str = repr(metadata)
        assert "FileMetadata" in repr_str
        assert "2024015" in repr_str
