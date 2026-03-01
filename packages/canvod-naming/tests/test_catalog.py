"""Tests for canvod.naming.catalog."""

import pytest

from canvod.naming.catalog import FilenameCatalog
from canvod.naming.convention import CanVODFilename, FileType, ReceiverType
from canvod.naming.mapping import VirtualFile


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / ".canvod" / "filename_catalog.duckdb"


@pytest.fixture
def sample_file(tmp_path):
    """Create a physical file and return a VirtualFile."""
    p = tmp_path / "rosl001a.25o"
    p.write_bytes(b"test data")
    cn = CanVODFilename(
        site="ROS",
        receiver_type=ReceiverType.REFERENCE,
        receiver_number=1,
        agency="TUW",
        year=2025,
        doy=1,
    )
    return VirtualFile(physical_path=p, conventional_name=cn)


@pytest.fixture
def sample_file_2(tmp_path):
    """Create a second physical file."""
    p = tmp_path / "rosl002a.25o"
    p.write_bytes(b"test data 2")
    cn = CanVODFilename(
        site="ROS",
        receiver_type=ReceiverType.REFERENCE,
        receiver_number=1,
        agency="TUW",
        year=2025,
        doy=2,
    )
    return VirtualFile(physical_path=p, conventional_name=cn)


@pytest.fixture
def canopy_file(tmp_path):
    """Create a canopy receiver file."""
    p = tmp_path / "canopy001.sbf"
    p.write_bytes(b"canopy data")
    cn = CanVODFilename(
        site="ROS",
        receiver_type=ReceiverType.ACTIVE,
        receiver_number=1,
        agency="TUW",
        year=2025,
        doy=1,
        file_type=FileType.SBF,
    )
    return VirtualFile(physical_path=p, conventional_name=cn)


class TestFilenameCatalog:
    def test_context_manager(self, db_path):
        with FilenameCatalog(db_path) as cat:
            assert cat.count() == 0

    def test_record_and_count(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            assert cat.count() == 1

    def test_record_batch(self, db_path, sample_file, sample_file_2):
        with FilenameCatalog(db_path) as cat:
            cat.record_batch([sample_file, sample_file_2])
            assert cat.count() == 2

    def test_record_idempotent(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            cat.record(sample_file)  # Same file again
            assert cat.count() == 1  # Should update, not duplicate

    def test_lookup_by_conventional(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            path = cat.lookup_by_conventional("ROSR01TUW_R_20250010000_01D_05S_AA.rnx")
            assert path is not None
            assert path == sample_file.physical_path

    def test_lookup_by_conventional_not_found(self, db_path):
        with FilenameCatalog(db_path) as cat:
            assert cat.lookup_by_conventional("nonexistent") is None

    def test_lookup_by_physical(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            cn = cat.lookup_by_physical(sample_file.physical_path)
            assert cn is not None
            assert cn.name == "ROSR01TUW_R_20250010000_01D_05S_AA.rnx"

    def test_lookup_by_physical_not_found(self, db_path, tmp_path):
        with FilenameCatalog(db_path) as cat:
            assert cat.lookup_by_physical(tmp_path / "nope") is None

    def test_query_date_range(self, db_path, sample_file, sample_file_2):
        with FilenameCatalog(db_path) as cat:
            cat.record_batch([sample_file, sample_file_2])
            # Query for DOY 1 only
            vfs = cat.query_date_range(2025, 1, 2025, 1)
            assert len(vfs) == 1
            assert vfs[0].conventional_name.doy == 1

    def test_query_date_range_both(self, db_path, sample_file, sample_file_2):
        with FilenameCatalog(db_path) as cat:
            cat.record_batch([sample_file, sample_file_2])
            vfs = cat.query_date_range(2025, 1, 2025, 2)
            assert len(vfs) == 2

    def test_query_date_range_receiver_filter(self, db_path, sample_file, canopy_file):
        with FilenameCatalog(db_path) as cat:
            cat.record_batch([sample_file, canopy_file])
            # Filter reference only
            vfs = cat.query_date_range(2025, 1, 2025, 1, receiver_type="R")
            assert len(vfs) == 1
            assert vfs[0].conventional_name.receiver_type == ReceiverType.REFERENCE
            # Filter canopy only
            vfs = cat.query_date_range(2025, 1, 2025, 1, receiver_type="A")
            assert len(vfs) == 1
            assert vfs[0].conventional_name.receiver_type == ReceiverType.ACTIVE

    def test_verify_integrity_all_present(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            missing = cat.verify_integrity()
            assert missing == []

    def test_verify_integrity_missing(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            # Delete the physical file
            sample_file.physical_path.unlink()
            missing = cat.verify_integrity()
            assert len(missing) == 1

    def test_persistence(self, db_path, sample_file):
        """Catalog survives close and reopen."""
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)

        with FilenameCatalog(db_path) as cat2:
            assert cat2.count() == 1
            cn = cat2.lookup_by_physical(sample_file.physical_path)
            assert cn is not None


class TestFilenameCatalogMetadata:
    def test_record_with_metadata(self, db_path, tmp_path):
        p = tmp_path / "rosl001a.25o"
        p.write_bytes(b"test data")
        cn = CanVODFilename(
            site="ROS",
            receiver_type=ReceiverType.REFERENCE,
            receiver_number=1,
            agency="TUW",
            year=2025,
            doy=1,
        )
        meta = {"species": "Fagus sylvatica", "antenna_height": 1.5}
        vf = VirtualFile(physical_path=p, conventional_name=cn, receiver_metadata=meta)

        with FilenameCatalog(db_path) as cat:
            cat.record(vf)
            # Query back and verify metadata round-trips
            vfs = cat.query_date_range(2025, 1, 2025, 1)
            assert len(vfs) == 1
            assert vfs[0].receiver_metadata == meta

    def test_record_without_metadata(self, db_path, sample_file):
        with FilenameCatalog(db_path) as cat:
            cat.record(sample_file)
            vfs = cat.query_date_range(2025, 1, 2025, 1)
            assert len(vfs) == 1
            assert vfs[0].receiver_metadata is None

    def test_metadata_persists_across_sessions(self, db_path, tmp_path):
        p = tmp_path / "rosl001a.25o"
        p.write_bytes(b"test data")
        cn = CanVODFilename(
            site="ROS",
            receiver_type=ReceiverType.REFERENCE,
            receiver_number=1,
            agency="TUW",
            year=2025,
            doy=1,
        )
        meta = {"site_url": "https://example.com", "is_permanent": True}
        vf = VirtualFile(physical_path=p, conventional_name=cn, receiver_metadata=meta)

        with FilenameCatalog(db_path) as cat:
            cat.record(vf)

        with FilenameCatalog(db_path) as cat2:
            vfs = cat2.query_date_range(2025, 1, 2025, 1)
            assert vfs[0].receiver_metadata == meta


class TestFilenameCatalogPolars:
    def test_to_polars(self, db_path, sample_file, sample_file_2):
        pytest.importorskip("polars")
        with FilenameCatalog(db_path) as cat:
            cat.record_batch([sample_file, sample_file_2])
            df = cat.to_polars()
            assert len(df) == 2
            assert "conventional_name" in df.columns
            assert "physical_path" in df.columns
