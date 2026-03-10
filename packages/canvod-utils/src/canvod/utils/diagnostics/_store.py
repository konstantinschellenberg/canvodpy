"""Global metrics store shared across all diagnostics modules.

All diagnostics modules record into this single store. Records go to both
an in-memory list (for current-session queries) and an optional SQLite
database (for cross-session persistence and dashboards).

Configure the database path::

    from canvod.utils.diagnostics import configure_db

    configure_db("~/.canvod/metrics.db")          # explicit path
    configure_db()                                  # default: ~/.canvod/metrics.db
    # or set env var: CANVOD_METRICS_DB=path/to/db

Disable persistence::

    configure_db(None)  # in-memory only
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import UTC
from pathlib import Path
from typing import Any

# ── In-memory store (current session) ──────────────────────────────

_timings: list[dict[str, Any]] = []

# ── SQLite persistence ─────────────────────────────────────────────

_db_path: Path | None = None
_db_conn: sqlite3.Connection | None = None
_db_lock = threading.Lock()
_db_configured = False

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    operation   TEXT    NOT NULL,
    duration_s  REAL    NOT NULL,
    timestamp   TEXT    NOT NULL,
    metric_type TEXT,
    status      TEXT,
    peak_memory_mb  REAL,
    n_epochs    INTEGER,
    n_sids      INTEGER,
    n_variables INTEGER,
    size_mb     REAL,
    batch       TEXT,
    extras      TEXT
);
CREATE INDEX IF NOT EXISTS idx_metrics_operation ON metrics(operation);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_type      ON metrics(metric_type);
"""

# Known columns that get their own DB column (not stuffed into extras JSON)
_KNOWN_COLUMNS = {
    "operation",
    "duration_s",
    "timestamp",
    "metric_type",
    "status",
    "peak_memory_mb",
    "n_epochs",
    "n_sids",
    "n_variables",
    "size_mb",
    "batch",
}


def configure_db(path: str | Path | None = "~/.canvod/metrics.db") -> None:
    """Configure the SQLite database for persistent metrics storage.

    Parameters
    ----------
    path : str, Path, or None
        Path to the SQLite database file. ``None`` disables persistence.
        Default: ``~/.canvod/metrics.db``.
    """
    global _db_path, _db_conn, _db_configured

    with _db_lock:
        # Close existing connection
        if _db_conn is not None:
            _db_conn.close()
            _db_conn = None

        if path is None:
            _db_path = None
            _db_configured = True
            return

        _db_path = Path(path).expanduser()
        _db_path.parent.mkdir(parents=True, exist_ok=True)
        _db_conn = sqlite3.connect(str(_db_path), check_same_thread=False)
        _db_conn.executescript(_SCHEMA)
        _db_configured = True


def _get_db() -> sqlite3.Connection | None:
    """Get or lazily initialize the database connection."""
    global _db_configured

    if _db_configured:
        return _db_conn

    # Auto-configure from env var or default
    env_path = os.environ.get("CANVOD_METRICS_DB")
    if env_path == "none" or env_path == "":
        configure_db(None)
    elif env_path:
        configure_db(env_path)
    else:
        configure_db()  # default path

    return _db_conn


def _persist(row: dict[str, Any]) -> None:
    """Write a single record to SQLite (if configured)."""
    conn = _get_db()
    if conn is None:
        return

    # Separate known columns from extras
    known = {k: row.get(k) for k in _KNOWN_COLUMNS if k in row}
    extras = {k: v for k, v in row.items() if k not in _KNOWN_COLUMNS}

    known["extras"] = json.dumps(extras) if extras else None

    cols = list(known.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    with _db_lock:
        try:
            conn.execute(
                f"INSERT INTO metrics ({col_names}) VALUES ({placeholders})",
                [known[c] for c in cols],
            )
            conn.commit()
        except Exception:
            pass  # never let DB errors break the pipeline


# ── Public API ─────────────────────────────────────────────────────


def record(operation: str, duration: float, **extras: Any) -> None:
    """Append a metric record to both in-memory and persistent stores."""
    from datetime import datetime

    row = {
        "operation": operation,
        "duration_s": round(duration, 6),
        "timestamp": datetime.now(UTC).isoformat(),
        **extras,
    }
    _timings.append(row)
    _persist(row)


def get_timings() -> Any:
    """Return current-session metrics as a polars DataFrame."""
    import polars as pl

    if not _timings:
        return pl.DataFrame(
            schema={
                "operation": pl.Utf8,
                "duration_s": pl.Float64,
                "timestamp": pl.Utf8,
            }
        )
    return pl.DataFrame(_timings)


def get_timings_raw() -> list[dict[str, Any]]:
    """Return raw current-session metric records as a list of dicts."""
    return list(_timings)


def reset_timings() -> None:
    """Clear current-session in-memory metrics (does not delete DB records)."""
    _timings.clear()


def query_db(
    *,
    since: str | None = None,
    operation: str | None = None,
    metric_type: str | None = None,
    limit: int = 10000,
) -> Any:
    """Query the persistent metrics database.

    Parameters
    ----------
    since : str, optional
        ISO timestamp. Only return records after this time.
    operation : str, optional
        Filter by operation name (supports SQL LIKE with %).
    metric_type : str, optional
        Filter by metric_type (e.g. "task", "memory", "dataset").
    limit : int
        Maximum number of records.

    Returns
    -------
    polars.DataFrame

    Examples
    --------
    ::

        from canvod.utils.diagnostics import query_db

        # Last 24 hours
        df = query_db(since="2026-03-09T00:00:00")

        # All store writes
        df = query_db(operation="store.%")

        # Task metrics only
        df = query_db(metric_type="task")
    """
    import polars as pl

    conn = _get_db()
    if conn is None:
        return pl.DataFrame(
            schema={
                "operation": pl.Utf8,
                "duration_s": pl.Float64,
                "timestamp": pl.Utf8,
            }
        )

    sql = "SELECT * FROM metrics WHERE 1=1"
    params: list[Any] = []

    if since:
        sql += " AND timestamp >= ?"
        params.append(since)
    if operation:
        sql += " AND operation LIKE ?"
        params.append(operation)
    if metric_type:
        sql += " AND metric_type = ?"
        params.append(metric_type)

    sql += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)

    with _db_lock:
        cursor = conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

    if not rows:
        return pl.DataFrame(
            schema={
                "operation": pl.Utf8,
                "duration_s": pl.Float64,
                "timestamp": pl.Utf8,
            }
        )

    return pl.DataFrame(
        [dict(zip(columns, row)) for row in rows],
    ).drop("id", "extras", strict=False)


def db_path() -> Path | None:
    """Return the current database file path, or None if persistence is off."""
    _get_db()  # ensure configured
    return _db_path


def log_timing(operation: str, elapsed: float, extras: dict[str, Any]) -> None:
    """Emit a structlog message with timing info."""
    try:
        import structlog

        log = structlog.get_logger(__name__)
        log.info("perf", operation=operation, duration_s=round(elapsed, 3), **extras)
    except ImportError:
        pass
