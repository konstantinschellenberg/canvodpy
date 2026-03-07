"""Tests for metadata_row_exists temporal overlap detection.

Verifies that the store correctly detects:
- Exact hash matches (file already ingested)
- Temporal overlaps (daily file covering sub-daily intervals)
- Non-overlapping intervals (safe to ingest)
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from canvod.store import create_rinex_store


def _make_dataset(start: str, end: str, n_epochs: int = 10) -> xr.Dataset:
    """Create a minimal dataset with epoch dimension and File Hash attr."""
    epochs = np.linspace(
        np.datetime64(start).astype("int64"),
        np.datetime64(end).astype("int64"),
        n_epochs,
    ).astype("datetime64[ns]")
    return xr.Dataset(
        {"SNR": (["epoch", "sid"], np.random.rand(n_epochs, 3).astype(np.float32))},
        coords={
            "epoch": epochs,
            "sid": ["G01|L1|C", "G02|L1|C", "G03|L1|C"],
        },
        attrs={"File Hash": f"hash_{start}_{end}"},
    )


@pytest.fixture
def rinex_store(tmp_path: Path):
    """Create a RINEX store with one 15-min file already ingested."""
    store = create_rinex_store(tmp_path / "test_rinex")

    # Write initial group with a 15-min dataset
    ds = _make_dataset("2025-01-01T00:00:00", "2025-01-01T00:14:55")
    store.write_initial_group(
        dataset=ds, group_name="canopy_01", commit_message="initial"
    )

    # Append metadata for this file
    store.append_metadata(
        group_name="canopy_01",
        rinex_hash="hash_15min_file_1",
        start=np.datetime64("2025-01-01T00:00:00", "ns"),
        end=np.datetime64("2025-01-01T00:14:55", "ns"),
        snapshot_id="snap1",
        action="write",
        commit_msg="wrote 15min file 1",
        dataset_attrs={"File Hash": "hash_15min_file_1"},
    )

    # Append a second 15-min file's metadata
    store.append_metadata(
        group_name="canopy_01",
        rinex_hash="hash_15min_file_2",
        start=np.datetime64("2025-01-01T00:15:00", "ns"),
        end=np.datetime64("2025-01-01T00:29:55", "ns"),
        snapshot_id="snap2",
        action="write",
        commit_msg="wrote 15min file 2",
        dataset_attrs={"File Hash": "hash_15min_file_2"},
    )

    return store


class TestHashMatch:
    """Test exact hash match detection."""

    def test_exact_hash_detected(self, rinex_store) -> None:
        """File with same hash as existing entry is detected."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_15min_file_1",
            start=np.datetime64("2025-01-01T00:00:00", "ns"),
            end=np.datetime64("2025-01-01T00:14:55", "ns"),
        )
        assert exists is True
        assert matches.height == 1

    def test_unknown_hash_no_overlap(self, rinex_store) -> None:
        """File with new hash and non-overlapping interval passes."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_new_file",
            start=np.datetime64("2025-01-01T00:30:00", "ns"),
            end=np.datetime64("2025-01-01T00:44:55", "ns"),
        )
        assert exists is False
        assert matches.is_empty()


class TestTemporalOverlap:
    """Test temporal overlap detection (the daily-file scenario)."""

    def test_daily_file_overlapping_15min_files(self, rinex_store) -> None:
        """Daily file covering existing 15-min intervals is detected."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_daily_file",
            start=np.datetime64("2025-01-01T00:00:00", "ns"),
            end=np.datetime64("2025-01-01T23:59:55", "ns"),
        )
        assert exists is True
        assert matches.height == 2  # Both 15-min files overlap

    def test_partial_overlap_start(self, rinex_store) -> None:
        """File that partially overlaps at the start is detected."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_partial",
            start=np.datetime64("2025-01-01T00:10:00", "ns"),
            end=np.datetime64("2025-01-01T00:20:00", "ns"),
        )
        assert exists is True
        assert matches.height == 2  # Overlaps both files

    def test_partial_overlap_end(self, rinex_store) -> None:
        """File that overlaps only the second 15-min file is detected."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_partial_end",
            start=np.datetime64("2025-01-01T00:20:00", "ns"),
            end=np.datetime64("2025-01-01T00:40:00", "ns"),
        )
        assert exists is True
        assert matches.height == 1  # Only second file overlaps

    def test_adjacent_intervals_no_overlap(self, rinex_store) -> None:
        """File immediately after existing intervals is not flagged."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="canopy_01",
            rinex_hash="hash_adjacent",
            start=np.datetime64("2025-01-01T00:30:00", "ns"),
            end=np.datetime64("2025-01-01T00:44:55", "ns"),
        )
        assert exists is False

    def test_nonexistent_group(self, rinex_store) -> None:
        """Query for a group that doesn't exist returns False."""
        exists, matches = rinex_store.metadata_row_exists(
            group_name="nonexistent_group",
            rinex_hash="hash_whatever",
            start=np.datetime64("2025-01-01T00:00:00", "ns"),
            end=np.datetime64("2025-01-01T00:14:55", "ns"),
        )
        assert exists is False
        assert matches.is_empty()
