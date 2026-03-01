"""DuckDB-backed metadata catalog for file mappings.

The ``FilenameCatalog`` persists the mapping between physical files and
their canVOD conventional names, enabling fast lookups and date-range
queries without re-scanning the filesystem.

Catalog location: ``{gnss_site_data_root}/.canvod/filename_catalog.duckdb``
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .convention import CanVODFilename
from .mapping import VirtualFile

_CREATE_TABLE_SQL = """\
CREATE TABLE IF NOT EXISTS file_mapping (
    id                 INTEGER PRIMARY KEY,
    -- Physical file
    physical_path      TEXT NOT NULL UNIQUE,
    physical_name      TEXT NOT NULL,
    file_size_bytes    BIGINT,
    file_mtime         TIMESTAMP,
    -- Conventional name
    conventional_name  TEXT NOT NULL,
    site_id            TEXT NOT NULL,
    receiver_type      TEXT NOT NULL,
    receiver_number    INTEGER NOT NULL,
    agency             TEXT NOT NULL,
    year               INTEGER NOT NULL,
    doy                INTEGER NOT NULL,
    hour               INTEGER NOT NULL DEFAULT 0,
    minute             INTEGER NOT NULL DEFAULT 0,
    period             TEXT NOT NULL,
    sampling           TEXT NOT NULL,
    content            TEXT NOT NULL DEFAULT 'AA',
    file_type          TEXT NOT NULL,
    compression        TEXT,
    -- Tracking
    file_hash          TEXT,
    first_seen_at      TIMESTAMP NOT NULL,
    last_verified_at   TIMESTAMP NOT NULL,
    -- Receiver metadata (freeform JSON from config)
    receiver_metadata  TEXT
);
"""

_CREATE_SEQUENCE_SQL = """\
CREATE SEQUENCE IF NOT EXISTS file_mapping_id_seq START 1;
"""


def _compute_file_hash(path: Path, prefix_len: int = 16) -> str | None:
    """Compute SHA-256 hash of the first 64 KiB of a file."""
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            h.update(f.read(65536))
        return h.hexdigest()[:prefix_len]
    except OSError:
        return None


def _file_stat(path: Path) -> tuple[int | None, datetime | None]:
    """Get file size and mtime."""
    try:
        stat = path.stat()
        return (
            stat.st_size,
            datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        )
    except OSError:
        return None, None


class FilenameCatalog:
    """DuckDB-backed catalog of file name mappings.

    Parameters
    ----------
    db_path
        Path to the DuckDB database file. Created if it doesn't exist.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._conn.execute(_CREATE_SEQUENCE_SQL)
        self._conn.execute(_CREATE_TABLE_SQL)

    def record(self, vf: VirtualFile) -> None:
        """Insert or update a single file mapping."""
        now = datetime.now(tz=UTC)
        cn = vf.conventional_name
        size, mtime = _file_stat(vf.physical_path)
        file_hash = _compute_file_hash(vf.physical_path)
        phys_str = str(vf.physical_path)
        meta_json = json.dumps(vf.receiver_metadata) if vf.receiver_metadata else None

        existing = self._conn.execute(
            "SELECT id FROM file_mapping WHERE physical_path = ?", [phys_str]
        ).fetchone()

        if existing:
            self._conn.execute(
                """\
                UPDATE file_mapping SET
                    physical_name = ?, file_size_bytes = ?, file_mtime = ?,
                    conventional_name = ?, site_id = ?, receiver_type = ?,
                    receiver_number = ?, agency = ?, year = ?, doy = ?,
                    hour = ?, minute = ?, period = ?, sampling = ?,
                    content = ?, file_type = ?, compression = ?,
                    file_hash = ?, last_verified_at = ?, receiver_metadata = ?
                WHERE id = ?""",
                [
                    vf.physical_path.name,
                    size,
                    mtime,
                    cn.name,
                    cn.site,
                    cn.receiver_type.value,
                    cn.receiver_number,
                    cn.agency,
                    cn.year,
                    cn.doy,
                    cn.hour,
                    cn.minute,
                    cn.period,
                    cn.sampling,
                    cn.content,
                    cn.file_type.value,
                    cn.compression,
                    file_hash,
                    now,
                    meta_json,
                    existing[0],
                ],
            )
        else:
            self._conn.execute(
                """\
                INSERT INTO file_mapping (
                    id, physical_path, physical_name, file_size_bytes, file_mtime,
                    conventional_name, site_id, receiver_type, receiver_number,
                    agency, year, doy, hour, minute, period, sampling,
                    content, file_type, compression,
                    file_hash, first_seen_at, last_verified_at, receiver_metadata
                ) VALUES (
                    nextval('file_mapping_id_seq'),
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?
                )""",
                [
                    phys_str,
                    vf.physical_path.name,
                    size,
                    mtime,
                    cn.name,
                    cn.site,
                    cn.receiver_type.value,
                    cn.receiver_number,
                    cn.agency,
                    cn.year,
                    cn.doy,
                    cn.hour,
                    cn.minute,
                    cn.period,
                    cn.sampling,
                    cn.content,
                    cn.file_type.value,
                    cn.compression,
                    file_hash,
                    now,
                    now,
                    meta_json,
                ],
            )

    def record_batch(self, vfs: list[VirtualFile]) -> None:
        """Insert or update a batch of file mappings."""
        for vf in vfs:
            self.record(vf)

    def lookup_by_conventional(self, name: str) -> Path | None:
        """Look up a physical path by conventional name.

        Returns None if not found.
        """
        row = self._conn.execute(
            "SELECT physical_path FROM file_mapping WHERE conventional_name = ?",
            [name],
        ).fetchone()
        return Path(row[0]) if row else None

    def lookup_by_physical(self, path: Path) -> CanVODFilename | None:
        """Look up a conventional name by physical path.

        Returns None if not found.
        """
        row = self._conn.execute(
            "SELECT conventional_name FROM file_mapping WHERE physical_path = ?",
            [str(path)],
        ).fetchone()
        if row is None:
            return None
        return CanVODFilename.from_filename(row[0])

    def query_date_range(
        self,
        start_year: int,
        start_doy: int,
        end_year: int,
        end_doy: int,
        *,
        receiver_type: str | None = None,
    ) -> list[VirtualFile]:
        """Query file mappings within a date range.

        Parameters
        ----------
        start_year, start_doy
            Start of range (inclusive).
        end_year, end_doy
            End of range (inclusive).
        receiver_type
            Optional filter: ``"R"`` or ``"A"``.
        """
        sql = """\
            SELECT physical_path, conventional_name, receiver_metadata
            FROM file_mapping
            WHERE (year * 1000 + doy) BETWEEN ? AND ?
        """
        params: list = [
            start_year * 1000 + start_doy,
            end_year * 1000 + end_doy,
        ]

        if receiver_type is not None:
            sql += " AND receiver_type = ?"
            params.append(receiver_type)

        sql += " ORDER BY year, doy, hour, minute"

        rows = self._conn.execute(sql, params).fetchall()
        results = []
        for phys_str, conv_name, meta_json in rows:
            cn = CanVODFilename.from_filename(conv_name)
            meta = json.loads(meta_json) if meta_json else None
            results.append(
                VirtualFile(
                    physical_path=Path(phys_str),
                    conventional_name=cn,
                    receiver_metadata=meta,
                )
            )
        return results

    def verify_integrity(self) -> list[str]:
        """Check that all cataloged physical files still exist.

        Returns
        -------
        list[str]
            List of physical paths that no longer exist on disk.
        """
        rows = self._conn.execute("SELECT physical_path FROM file_mapping").fetchall()
        missing = []
        for (phys_str,) in rows:
            if not Path(phys_str).exists():
                missing.append(phys_str)
        return missing

    def count(self) -> int:
        """Return total number of cataloged files."""
        row = self._conn.execute("SELECT COUNT(*) FROM file_mapping").fetchone()
        return row[0]

    def to_polars(self):
        """Export the catalog to a Polars DataFrame via DuckDB-Arrow bridge.

        Returns
        -------
        polars.DataFrame
        """
        import polars as pl

        arrow_table = self._conn.execute(
            "SELECT * FROM file_mapping"
        ).fetch_arrow_table()
        return pl.from_arrow(arrow_table)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> FilenameCatalog:
        return self

    def __exit__(self, *args) -> None:
        self.close()
