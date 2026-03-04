"""Airflow-compatible task functions for GNSS daily processing pipeline.

Each function accepts only primitives (str, dict, list, None) and returns
JSON-serializable dicts suitable for XCom.  They delegate to existing
canvodpy machinery — no pipeline rewrite.

Typical DAG topology::

    [check_rinex] ──> [fetch_aux_data] ──> [process_rinex] ──> [calculate_vod]

``check_rinex`` discovers whether RINEX files exist for **both** canopy
and reference receivers on the requested date (via ``PairDataDirMatcher``).
If they do, ``fetch_aux_data`` downloads SP3+CLK orbit/clock products — a
step that is expected to fail for recent dates (products lag by 1-14 days).
When aux data is available, ``process_rinex`` augments the observations and
writes to Icechunk, then ``calculate_vod`` computes vegetation optical depth.
"""

from __future__ import annotations

import datetime
import logging
import shutil
from pathlib import Path

import numpy as np
import xarray as xr

from canvod.auxiliary.pipeline import AuxDataPipeline
from canvod.auxiliary.position import ECEFPosition
from canvod.readers import MatchedDirs
from canvod.readers.matching.dir_matcher import _has_rinex_files
from canvod.utils.config import load_config
from canvod.utils.tools import YYYYDOY
from canvodpy.orchestrator.interpolator import (
    ClockConfig,
    ClockInterpolationStrategy,
    Sp3Config,
    Sp3InterpolationStrategy,
)

logger = logging.getLogger(__name__)

# RINEX glob patterns (mirrored from processor._get_rinex_files)
_RINEX_PATTERNS: list[str] = ["*.??o", "*.??O", "*.rnx", "*.RNX", "*.??_"]


# ---------------------------------------------------------------------------
# Utility extracted from RinexDataProcessor._parse_sampling_interval_from_filename
# ---------------------------------------------------------------------------


def parse_sampling_interval_from_filename(filename: str) -> float | None:
    """Extract sampling interval from a RINEX v3 long filename.

    RINEX v3.04 long filenames encode the data frequency at a fixed
    position, e.g. ``ROSA01TUW_R_20250020000_01D_05S_AA.rnx`` where
    ``05S`` means 5-second sampling.

    Parameters
    ----------
    filename : str
        RINEX filename (stem or full name).

    Returns
    -------
    float or None
        Sampling interval in seconds, or ``None`` if parsing fails.
    """
    import re

    parts = Path(filename).stem.split("_")
    if len(parts) >= 5:
        freq = parts[4]  # e.g. "05S", "30S", "01Z" (1 Hz)
        m = re.match(r"^(\d+)([SMHDZC])$", freq)
        if m:
            value, unit = int(m.group(1)), m.group(2)
            multipliers = {"S": 1, "M": 60, "H": 3600, "D": 86400}
            if unit == "Z":  # Hz -> seconds
                return 1.0 / value if value else None
            if unit in multipliers:
                return float(value * multipliers[unit])
    return None


def _resolve_date(yyyydoy: str) -> YYYYDOY:
    """Accept ``YYYYDDD`` *or* Airflow ``ds`` (``YYYY-MM-DD``)."""
    if "-" in yyyydoy:
        return YYYYDOY.from_date(datetime.date.fromisoformat(yyyydoy))
    return YYYYDOY.from_str(yyyydoy)


def _get_rinex_files(directory: Path) -> list[Path]:
    """Glob RINEX files from *directory* using standard patterns."""
    from natsort import natsorted

    files: list[Path] = []
    if not directory.exists():
        return files
    for pattern in _RINEX_PATTERNS:
        files.extend(directory.glob(pattern))
    return natsorted(files)


# ---------------------------------------------------------------------------
# Task 1 — check_rinex
# ---------------------------------------------------------------------------


def check_rinex(site: str, yyyydoy: str) -> dict:
    """Check whether RINEX files exist for both receivers on the given date.

    Uses the same ``PairDataDirMatcher`` directory scanning logic that the
    orchestrator uses: the ``receiver.directory`` field from ``sites.yaml``
    is the relative path from ``gnss_site_data_root`` to the RINEX
    directories.  Date subdirectories are ``YYDDD`` (5-digit).

    Parameters
    ----------
    site : str
        Research site name (must exist in config).
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "ready": bool, "receivers": {...}}``

    Raises
    ------
    RuntimeError
        If RINEX files are missing for any receiver (stops the DAG run
        so Airflow can retry later).
    """
    config = load_config()
    site_cfg = config.sites.sites[site]
    date_obj = _resolve_date(yyyydoy)
    base = site_cfg.get_base_path()

    receivers: dict[str, dict] = {}
    all_ready = True

    for name, rcfg in site_cfg.receivers.items():
        recv_dir = base / rcfg.directory / date_obj.yydoy
        has_files = _has_rinex_files(recv_dir)
        files = _get_rinex_files(recv_dir) if has_files else []
        receivers[name] = {
            "directory": str(recv_dir),
            "has_files": has_files,
            "files": [str(f) for f in files],
            "count": len(files),
        }
        if not has_files:
            all_ready = False

    result = {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "ready": all_ready,
        "receivers": receivers,
    }

    if not all_ready:
        missing = [n for n, r in receivers.items() if not r["has_files"]]
        msg = (
            f"RINEX files not yet available for {site} {date_obj.to_str()}: "
            f"missing receivers {missing}"
        )
        logger.warning(msg)
        raise RuntimeError(msg)

    logger.info("check_rinex: %s %s — all receivers ready", site, date_obj.to_str())
    return result


# ---------------------------------------------------------------------------
# Task 2 — fetch_aux_data
# ---------------------------------------------------------------------------


def fetch_aux_data(
    site: str,
    yyyydoy: str,
    agency: str | None = None,
    product_type: str | None = None,
    sampling_interval_s: float | None = None,
) -> dict:
    """Download SP3+CLK, Hermite-interpolate, and write to a temp Zarr store.

    SP3/CLK products are published with a delay (rapid ~1 day,
    final ~12-14 days).  When products are not yet available the FTP
    download raises ``RuntimeError("Failed to download …")``.  This
    exception is **not** caught here — it propagates to Airflow so the
    task is marked as failed and retried on the next scheduled run.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).
    agency : str, optional
        Analysis centre code (e.g. ``"COD"``).  Defaults to config value.
    product_type : str, optional
        ``"final"`` or ``"rapid"``.  Defaults to config value.
    sampling_interval_s : float, optional
        Observation sampling interval in seconds.  Auto-detected from
        filename if ``None``.

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "aux_zarr_path", "sampling_interval_s",
        "n_epochs", "n_sids"}``
    """
    config = load_config()
    site_cfg = config.sites.sites[site]
    date_obj = _resolve_date(yyyydoy)
    keep_sids = config.sids.get_sids()

    # Resolve aux_file_path
    configured_aux_dir = config.processing.storage.aux_data_dir
    if configured_aux_dir is not None:
        aux_file_path = configured_aux_dir
    else:
        aux_file_path = site_cfg.get_base_path()

    user_email = config.nasa_earthdata_acc_mail
    base = site_cfg.get_base_path()

    # Build a MatchedDirs for the date (pipeline reads .yyyydoy from it)
    canopy_dir = reference_dir = base
    for _name, rcfg in site_cfg.receivers.items():
        if rcfg.type == "canopy" and canopy_dir == base:
            canopy_dir = base / rcfg.directory
        elif rcfg.type == "reference" and reference_dir == base:
            reference_dir = base / rcfg.directory
    matched_dirs = MatchedDirs(
        canopy_data_dir=canopy_dir,
        reference_data_dir=reference_dir,
        yyyydoy=date_obj,
    )

    # 1. Create and load pipeline (downloads SP3 + CLK via FTP)
    #    RuntimeError propagates to Airflow if products not yet available
    pipeline = AuxDataPipeline.create_standard(
        matched_dirs=matched_dirs,
        aux_file_path=aux_file_path,
        agency=agency,
        product_type=product_type,
        user_email=user_email,
        keep_sids=keep_sids,
    )
    pipeline.load_all()

    ephem_ds = pipeline.get("ephemerides")
    clock_ds = pipeline.get("clock")

    # 2. Detect sampling interval from RINEX filename
    if sampling_interval_s is None:
        for _name, rcfg in site_cfg.receivers.items():
            recv_dir = base / rcfg.directory / date_obj.yydoy
            rnx_files = _get_rinex_files(recv_dir)
            if rnx_files:
                sampling_interval_s = parse_sampling_interval_from_filename(
                    rnx_files[0].name,
                )
                if sampling_interval_s is not None:
                    break
    if sampling_interval_s is None:
        sampling_interval_s = 30.0  # safe default

    # 3. Generate full-day target epoch grid
    day_start = np.datetime64(date_obj.date, "D")
    n_epochs = int(24 * 3600 / sampling_interval_s)
    target_epochs = day_start + np.arange(n_epochs) * np.timedelta64(
        int(sampling_interval_s),
        "s",
    )

    # 4. Hermite interpolation for ephemerides
    sp3_interp = Sp3InterpolationStrategy(
        config=Sp3Config(use_velocities=True, fallback_method="linear"),
    )
    ephem_interp = sp3_interp.interpolate(ephem_ds, target_epochs)
    ephem_interp.attrs["interpolator_config"] = sp3_interp.to_attrs()

    # 5. Piecewise-linear interpolation for clocks
    clock_interp = ClockInterpolationStrategy(
        config=ClockConfig(window_size=9, jump_threshold=1e-6),
    )
    clock_interp_ds = clock_interp.interpolate(clock_ds, target_epochs)
    clock_interp_ds.attrs["interpolator_config"] = clock_interp.to_attrs()

    # 6. Merge and write to Zarr
    aux_processed = xr.merge([ephem_interp, clock_interp_ds])
    aux_dir = config.processing.storage.get_aux_data_dir()
    aux_zarr_path = aux_dir / f"aux_{date_obj.to_str()}.zarr"

    if aux_zarr_path.exists():
        shutil.rmtree(aux_zarr_path)
    aux_processed.to_zarr(aux_zarr_path, mode="w")

    logger.info(
        "fetch_aux_data: wrote %s  dims=%s",
        aux_zarr_path,
        dict(aux_processed.sizes),
    )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "aux_zarr_path": str(aux_zarr_path),
        "sampling_interval_s": sampling_interval_s,
        "n_epochs": int(aux_processed.sizes["epoch"]),
        "n_sids": int(aux_processed.sizes["sid"]),
    }


# ---------------------------------------------------------------------------
# Task 3 — process_rinex
# ---------------------------------------------------------------------------


def process_rinex(
    site: str,
    yyyydoy: str,
    aux_zarr_path: str,
    receiver_files: dict | None = None,
) -> dict:
    """Read RINEX, augment with aux data, and write to Icechunk RINEX store.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).
    aux_zarr_path : str
        Path to the pre-processed auxiliary Zarr store (from ``fetch_aux_data``).
    receiver_files : dict, optional
        ``{receiver_name: {"files": [str, ...], "count": N}}`` from
        ``check_rinex``.  When ``None``, files are discovered from disk.

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "receivers_processed": [...], "files_written": N}``
    """
    from pydantic import ValidationError

    from canvod.readers.rinex.v3_04 import Rnxv3Header
    from canvod.store import GnssResearchSite
    from canvodpy.orchestrator.processor import preprocess_with_hermite_aux

    config = load_config()
    site_cfg = config.sites.sites[site]
    date_obj = _resolve_date(yyyydoy)
    keep_vars = config.processing.processing.keep_rnx_vars
    keep_sids = config.sids.get_sids()
    base = site_cfg.get_base_path()
    aux_path = Path(aux_zarr_path)

    research_site = GnssResearchSite(site)
    receivers_processed: list[str] = []
    total_files_written = 0

    # Iterate over configured receivers
    for recv_name, rcfg in site_cfg.receivers.items():
        recv_type = rcfg.type
        recv_dir = base / rcfg.directory / date_obj.yydoy

        # Determine store groups for this receiver
        if recv_type == "canopy":
            store_groups = [recv_name]
        else:
            # Reference receivers write to {ref}_{canopy} store groups
            canopy_names = site_cfg.resolve_scs_from(recv_name)
            store_groups = [f"{recv_name}_{cn}" for cn in canopy_names]

        # Resolve RINEX files
        if receiver_files and recv_name in receiver_files:
            rnx_files = [Path(f) for f in receiver_files[recv_name]["files"]]
        else:
            rnx_files = _get_rinex_files(recv_dir)

        if not rnx_files:
            logger.warning("process_rinex: no files for %s, skipping", recv_name)
            continue

        # Compute receiver position from first RINEX header
        position: ECEFPosition | None = None
        for ff in rnx_files:
            try:
                header = Rnxv3Header.from_file(ff)
                position = ECEFPosition(
                    x=header.approx_position[0].magnitude,
                    y=header.approx_position[1].magnitude,
                    z=header.approx_position[2].magnitude,
                )
                break
            except (ValidationError, OSError, RuntimeError, ValueError) as exc:
                logger.warning("Header parse failed for %s: %s", ff.name, exc)

        if position is None:
            logger.error("No valid RINEX header for %s — skipping", recv_name)
            continue

        # Process each file sequentially (Airflow handles parallelism across sites)
        for rnx_file in rnx_files:
            try:
                _path, augmented_ds, _aux_ds, _sid_issues = preprocess_with_hermite_aux(
                    rnx_file=rnx_file,
                    keep_vars=keep_vars,
                    aux_zarr_path=aux_path,
                    receiver_position=position,
                    receiver_type=recv_name,
                    keep_sids=keep_sids,
                )
            except Exception:
                logger.exception("Failed to process %s", rnx_file.name)
                continue

            # Write to each store group for this receiver
            for group in store_groups:
                file_hash = augmented_ds.attrs.get("File Hash")

                # Dedup: skip if this file hash already exists
                existing = research_site.rinex_store.batch_check_existing(
                    group,
                    [file_hash] if file_hash else [],
                )
                if file_hash and file_hash in existing:
                    logger.info(
                        "Skipping duplicate %s in group %s", rnx_file.name, group
                    )
                    continue

                research_site.rinex_store.write_or_append_group(
                    dataset=augmented_ds,
                    group_name=group,
                    commit_message=f"Airflow ingest {rnx_file.name}",
                )
                total_files_written += 1

        receivers_processed.append(recv_name)
        logger.info(
            "process_rinex: %s processed %d files -> groups %s",
            recv_name,
            len(rnx_files),
            store_groups,
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "receivers_processed": receivers_processed,
        "files_written": total_files_written,
    }


# ---------------------------------------------------------------------------
# Task 4 — calculate_vod
# ---------------------------------------------------------------------------


def calculate_vod(site: str, yyyydoy: str) -> dict:
    """Compute VOD for all active analysis pairs and write to the VOD store.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "analyses": {name: {"mean_vod", "std_vod",
        "n_epochs"}}}``
    """
    from canvod.store import GnssResearchSite

    date_obj = _resolve_date(yyyydoy)

    research_site = GnssResearchSite(site)

    # Build time range for this day
    day_date = date_obj.date
    start_time = datetime.datetime.combine(day_date, datetime.time.min)
    end_time = datetime.datetime.combine(day_date, datetime.time.max)
    time_range = (start_time, end_time)

    analyses_result: dict[str, dict] = {}
    for analysis_name in research_site.active_vod_analyses:
        logger.info("calculate_vod: running %s for %s", analysis_name, site)

        vod_ds = research_site.calculate_vod(
            analysis_name=analysis_name,
            time_range=time_range,
        )

        research_site.store_vod_analysis(
            vod_dataset=vod_ds,
            analysis_name=analysis_name,
            commit_message=f"Airflow VOD {analysis_name} {date_obj.to_str()}",
        )

        # Collect stats
        tau_values = vod_ds["tau"].values if "tau" in vod_ds else None
        analyses_result[analysis_name] = {
            "mean_vod": float(np.nanmean(tau_values))
            if tau_values is not None
            else None,
            "std_vod": float(np.nanstd(tau_values)) if tau_values is not None else None,
            "n_epochs": int(vod_ds.sizes.get("epoch", 0)),
        }

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "analyses": analyses_result,
    }


# ---------------------------------------------------------------------------
# Task 5 — update_statistics
# ---------------------------------------------------------------------------


def update_statistics(site: str, yyyydoy: str) -> dict:
    """Update streaming statistics for all receivers at a site.

    Feeds the day's observations into Welford / GK / Histogram accumulators
    stored in a per-site Zarr statistics store.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "receivers_updated", "total_keys",
        "total_observations"}``
    """
    import zarr

    from canvod.ops.statistics import ProfileRegistry, StatisticsStore, UpdateStatistics
    from canvod.store import GnssResearchSite

    config = load_config()
    date_obj = _resolve_date(yyyydoy)

    research_site = GnssResearchSite(site)
    store_path = config.processing.storage.get_statistics_store_path(site)

    # Open (or create) the statistics Zarr store
    root = zarr.open_group(str(store_path), mode="a")
    stats_store = StatisticsStore(root)

    day_date = date_obj.date
    start_str = str(day_date)
    end_str = str(day_date + datetime.timedelta(days=1))

    receivers_updated: list[str] = []
    total_keys = 0
    total_observations = 0

    start_time = datetime.datetime.combine(day_date, datetime.time.min)
    end_time = datetime.datetime.combine(day_date, datetime.time.max)
    time_range = (start_time, end_time)

    site_config = config.sites.sites[site]
    for rx_name, rx_meta in site_config.receivers.items():
        rx_type = rx_meta.receiver_type

        # Idempotency: skip if this epoch range was already processed
        if stats_store.is_epoch_range_processed(rx_type, start_str, end_str):
            logger.info(
                "update_statistics: %s/%s already processed for %s — skipping",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Load or create the registry for this receiver type
        try:
            registry = stats_store.load(rx_type)
        except KeyError:
            registry = ProfileRegistry()

        # Load the day's data from the RINEX Icechunk store
        try:
            day_ds = research_site.load_rinex_data(
                receiver_name=rx_name,
                time_range=time_range,
            )
        except Exception:
            logger.warning(
                "update_statistics: no data for %s/%s on %s",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Determine variables to profile (all float data vars)
        variables = [
            v for v in day_ds.data_vars if np.issubdtype(day_ds[v].dtype, np.floating)
        ]

        # Run the statistics op
        op = UpdateStatistics(
            registry=registry,
            receiver_type=rx_type,
            variables=variables,
        )
        _, result = op(day_ds)

        # Save updated registry and record epoch range
        stats_store.save(registry, rx_type)
        stats_store.record_epoch_range(rx_type, start_str, end_str)

        receivers_updated.append(rx_name)
        total_keys += len(registry)
        total_observations += sum(
            acc.welford.count for acc in registry._accumulators.values()
        )

        logger.info(
            "update_statistics: %s/%s — %s",
            site,
            rx_name,
            result.notes,
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "receivers_updated": receivers_updated,
        "total_keys": total_keys,
        "total_observations": total_observations,
    }
