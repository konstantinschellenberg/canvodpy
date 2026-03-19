"""Airflow-compatible task functions for GNSS daily processing pipeline.

Each function accepts only primitives (str, dict, list, None) and returns
JSON-serializable dicts suitable for XCom.  They delegate to existing
canvodpy machinery — no pipeline rewrite.

DAG topology::

    check_rinex → fetch_aux → process_rinex → calculate_vod → update_statistics
      → update_climatology → detect_anomalies      ─┐
                            → detect_changepoints   ─┤
                                                      → snapshot_statistics

``detect_anomalies`` and ``detect_changepoints`` run in parallel (both
depend only on ``update_climatology``).  ``snapshot_statistics`` waits for
both.
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


# GNSS file glob patterns — sourced from canvod-virtualiconvname BUILTIN_PATTERNS
def _get_gnss_globs() -> list[str]:
    from canvod.virtualiconvname.patterns import BUILTIN_PATTERNS, auto_match_order

    globs: set[str] = set()
    for name in auto_match_order():
        globs.update(BUILTIN_PATTERNS[name].file_globs)
    return sorted(globs)


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
    """Glob GNSS data files from *directory* using BUILTIN_PATTERNS globs."""
    from natsort import natsorted

    if not directory.exists():
        return []

    files: list[Path] = []
    seen: set[Path] = set()
    for pattern in _get_gnss_globs():
        for path in directory.glob(pattern):
            if path.is_file() and path not in seen:
                seen.add(path)
                files.append(path)
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
# Task 1b — validate_data_dirs
# ---------------------------------------------------------------------------


def _resolve_recipe(recipe_name: str) -> Path:
    """Resolve a recipe name to its YAML file path.

    Searches ``config/recipes/`` relative to the monorepo root.
    """
    from canvod.utils.config.loader import find_monorepo_root

    recipe_path = find_monorepo_root() / "config" / "recipes" / f"{recipe_name}.yaml"
    if not recipe_path.exists():
        msg = (
            f"Recipe file not found: {recipe_path}\n"
            f"Create it with: just naming-init {recipe_name}"
        )
        raise FileNotFoundError(msg)
    return recipe_path


def _validate_receiver_with_recipe(
    recipe_name: str,
    receiver_base_dir: Path,
    reader_format: str | None,
) -> dict:
    """Validate a receiver's data directory using a NamingRecipe.

    Returns a result dict with status, counts, and sample canonical names.
    Raises ValueError on validation failure.
    """
    from natsort import natsorted

    from canvod.virtualiconvname.recipe import NamingRecipe

    recipe_path = _resolve_recipe(recipe_name)
    recipe = NamingRecipe.load(recipe_path)

    # Discover files using the recipe's glob pattern
    if not receiver_base_dir.exists():
        return {
            "status": "valid",
            "matched": 0,
            "skipped_format": 0,
            "unmatched": 0,
            "overlaps": 0,
            "warnings": [f"Directory does not exist: {receiver_base_dir}"],
            "sample_canonical_names": [],
        }

    # Walk subdirectories or flat depending on layout
    all_files: list[Path] = []
    for f in receiver_base_dir.rglob(recipe.glob):
        if f.is_file():
            all_files.append(f)
    all_files = natsorted(all_files)

    matched = []
    skipped = []
    unmatched = []
    errors = []

    for f in all_files:
        if not recipe.matches(f.name):
            skipped.append(f)
            continue
        try:
            vf = recipe.to_virtual_file(f)
            matched.append(vf)
        except ValueError as exc:
            unmatched.append(f)
            errors.append(f"  {f.name}: {exc}")

    # Check for temporal overlaps (same canonical name = duplicate)
    canonical_counts: dict[str, list[Path]] = {}
    for vf in matched:
        key = vf.canonical_str
        canonical_counts.setdefault(key, []).append(vf.physical_path)
    duplicates = {k: v for k, v in canonical_counts.items() if len(v) > 1}

    warnings: list[str] = []
    if duplicates:
        for cn, paths in duplicates.items():
            warnings.append(
                f"Duplicate canonical name {cn}: " + ", ".join(p.name for p in paths)
            )

    if unmatched:
        detail = "\n".join(errors[:20])
        if len(errors) > 20:
            detail += f"\n  ... and {len(errors) - 20} more"
        raise ValueError(
            f"{len(unmatched)} files could not be parsed by recipe "
            f"'{recipe_name}':\n{detail}"
        )

    return {
        "status": "valid",
        "matched": len(matched),
        "skipped": len(skipped),
        "unmatched": 0,
        "overlaps": len(duplicates),
        "warnings": warnings,
        "sample_canonical_names": [vf.canonical_str for vf in matched[:5]],
    }


def validate_data_dirs(site: str) -> dict:
    """Pre-flight validation of all receiver data directories for a site.

    Checks every receiver's data directory against the naming convention:
    - All files must map to a canonical ``CanVODFilename``
    - No temporal overlaps (e.g. daily + sub-daily files for the same day)
    - Duplicate canonical names are flagged

    Supports two validation modes per receiver:
    - **Recipe mode**: when ``recipe`` is set in the receiver config,
      loads a ``NamingRecipe`` from ``config/recipes/{recipe}.yaml``
    - **Legacy mode**: when ``naming`` dict is set, uses
      ``SiteNamingConfig`` + ``ReceiverNamingConfig`` + ``DataDirectoryValidator``

    Run this **before** starting a processing campaign to catch data
    quality issues early.

    Parameters
    ----------
    site : str
        Research site name (must exist in config).

    Returns
    -------
    dict
        ``{"site": str, "valid": bool, "receivers": {name: {status, ...}}}``

    Raises
    ------
    ValueError
        If any receiver directory has validation errors.
    """
    config = load_config()
    available = list(config.sites.sites.keys())
    if site not in config.sites.sites:
        msg = f"Unknown site '{site}'. Available sites: {', '.join(available) or '(none)'}"
        raise KeyError(msg)
    site_cfg = config.sites.sites[site]
    base = site_cfg.get_base_path()

    receivers_result: dict[str, dict] = {}
    all_valid = True
    errors: list[str] = []

    for name, rcfg in site_cfg.receivers.items():
        receiver_base_dir = base / rcfg.directory
        reader_format = rcfg.reader_format

        # Recipe-based validation (preferred)
        if rcfg.recipe:
            try:
                receivers_result[name] = _validate_receiver_with_recipe(
                    recipe_name=rcfg.recipe,
                    receiver_base_dir=receiver_base_dir,
                    reader_format=reader_format,
                )
                logger.info(
                    "validate_data_dirs: %s/%s — %d files via recipe '%s'",
                    site,
                    name,
                    receivers_result[name]["matched"],
                    rcfg.recipe,
                )
            except (ValueError, FileNotFoundError) as exc:
                all_valid = False
                errors.append(f"[{name}] {exc}")
                receivers_result[name] = {"status": "invalid", "error": str(exc)}
                logger.error("validate_data_dirs: %s/%s — FAILED: %s", site, name, exc)
            continue

        # Legacy naming-dict validation
        if rcfg.naming:
            from canvod.virtualiconvname import (
                DataDirectoryValidator,
                ReceiverNamingConfig,
                SiteNamingConfig,
            )

            if not site_cfg.naming:
                msg = (
                    f"Receiver '{name}' uses naming dict but site '{site}' "
                    "has no site-level naming config."
                )
                raise ValueError(msg)

            site_naming = SiteNamingConfig(**site_cfg.naming)
            receiver_naming = ReceiverNamingConfig(**rcfg.naming)
            validator = DataDirectoryValidator()

            try:
                report = validator.validate_receiver(
                    site_naming=site_naming,
                    receiver_naming=receiver_naming,
                    receiver_type=rcfg.type,
                    receiver_base_dir=receiver_base_dir,
                    reader_format=reader_format,
                )
                receivers_result[name] = {
                    "status": "valid",
                    "matched": len(report.matched),
                    "skipped_format": len(report.skipped_format),
                    "unmatched": 0,
                    "overlaps": 0,
                    "warnings": report.warnings,
                    "sample_canonical_names": [
                        vf.canonical_str for vf in report.matched[:5]
                    ],
                }
                logger.info(
                    "validate_data_dirs: %s/%s — %d files, all valid",
                    site,
                    name,
                    len(report.matched),
                )
            except ValueError as exc:
                all_valid = False
                errors.append(f"[{name}] {exc}")
                receivers_result[name] = {"status": "invalid", "error": str(exc)}
                logger.error("validate_data_dirs: %s/%s — FAILED: %s", site, name, exc)
            continue

        # No recipe and no naming — skip
        receivers_result[name] = {
            "status": "skipped",
            "reason": "no recipe or naming config",
        }
        logger.warning("Receiver '%s' has no recipe or naming config, skipping", name)

    result = {
        "site": site,
        "valid": all_valid,
        "receivers": receivers_result,
    }

    if not all_valid:
        full_report = "\n\n".join(errors)
        raise ValueError(
            f"Data directory validation failed for site '{site}':\n\n{full_report}"
        )

    logger.info("validate_data_dirs: %s — all receivers valid", site)
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


# ---------------------------------------------------------------------------
# Task 6 — update_climatology
# ---------------------------------------------------------------------------


def update_climatology(site: str, yyyydoy: str) -> dict:
    """Update climatology grids for all receivers at a site.

    For each receiver and each float variable, extracts DOY and
    hour-of-day from epoch timestamps and feeds them into
    :class:`ClimatologyGrid` accumulators.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "receivers_updated", "variables_updated",
        "total_observations"}``
    """
    import zarr

    from canvod.ops.statistics import StatisticsStore
    from canvod.store import GnssResearchSite
    from canvod.streamstats import ClimatologyGrid

    config = load_config()
    date_obj = _resolve_date(yyyydoy)

    research_site = GnssResearchSite(site)
    store_path = config.processing.storage.get_statistics_store_path(site)

    root = zarr.open_group(str(store_path), mode="a")
    stats_store = StatisticsStore(root)

    day_date = date_obj.date
    start_str = str(day_date)
    end_str = str(day_date + datetime.timedelta(days=1))

    receivers_updated: list[str] = []
    variables_updated: list[str] = []
    total_observations = 0

    start_time = datetime.datetime.combine(day_date, datetime.time.min)
    end_time = datetime.datetime.combine(day_date, datetime.time.max)
    time_range = (start_time, end_time)

    site_config = config.sites.sites[site]
    for rx_name, rx_meta in site_config.receivers.items():
        rx_type = rx_meta.receiver_type

        if stats_store.is_climatology_range_processed(rx_type, start_str, end_str):
            logger.info(
                "update_climatology: %s/%s already processed for %s — skipping",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Load day's data
        try:
            day_ds = research_site.load_rinex_data(
                receiver_name=rx_name,
                time_range=time_range,
            )
        except Exception:
            logger.warning(
                "update_climatology: no data for %s/%s on %s",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Extract DOY and hour-of-day from epoch timestamps
        epochs = day_ds.coords["epoch"].values  # datetime64
        epoch_dt = epochs.astype("datetime64[s]").astype("int64")
        # DOY: day-of-year (1-366)
        day_starts = epochs.astype("datetime64[D]")
        year_starts = day_starts.astype("datetime64[Y]")
        doys = (day_starts - year_starts).astype("timedelta64[D]").astype(np.int32) + 1
        # Hour-of-day as float
        seconds_of_day = (
            (epochs - day_starts).astype("timedelta64[s]").astype(np.float64)
        )
        hours = seconds_of_day / 3600.0

        # Determine float variables
        variables = [
            v for v in day_ds.data_vars if np.issubdtype(day_ds[v].dtype, np.floating)
        ]

        # Load or create climatology grids
        grids = stats_store.load_climatology(rx_type)

        n_sids = day_ds.sizes.get("sid", 1)

        for var in variables:
            if var not in grids:
                grids[var] = ClimatologyGrid()

            values_2d = day_ds[var].values  # (epoch, sid)
            # Flatten across (epoch, sid), repeating doys/hours for each sid
            flat_doys = np.tile(doys, n_sids)
            flat_hours = np.tile(hours, n_sids)
            flat_values = values_2d.ravel()

            # Filter out NaN
            valid = np.isfinite(flat_values)
            if valid.any():
                grids[var].update_batch(
                    flat_doys[valid],
                    flat_hours[valid],
                    flat_values[valid],
                )
                total_observations += int(valid.sum())

            if var not in variables_updated:
                variables_updated.append(var)

        stats_store.save_climatology(grids, rx_type)
        stats_store.record_climatology_range(rx_type, start_str, end_str)
        receivers_updated.append(rx_name)

        logger.info(
            "update_climatology: %s/%s — %d variables, %d obs",
            site,
            rx_name,
            len(variables),
            total_observations,
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "receivers_updated": receivers_updated,
        "variables_updated": variables_updated,
        "total_observations": total_observations,
    }


# ---------------------------------------------------------------------------
# Task 7 — detect_anomalies
# ---------------------------------------------------------------------------


def detect_anomalies(site: str, yyyydoy: str) -> dict:
    """Detect anomalies using climatology z-scores for each variable.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "anomaly_summary": {rx_type: {var: {...}}}}``
    """
    import zarr

    from canvod.ops.statistics import StatisticsStore
    from canvod.store import GnssResearchSite
    from canvod.streamstats import anomaly_zscore_batch, classify_anomaly_batch

    config = load_config()
    date_obj = _resolve_date(yyyydoy)

    research_site = GnssResearchSite(site)
    store_path = config.processing.storage.get_statistics_store_path(site)

    root = zarr.open_group(str(store_path), mode="a")
    stats_store = StatisticsStore(root)

    day_date = date_obj.date
    start_str = str(day_date)
    end_str = str(day_date + datetime.timedelta(days=1))
    date_str = str(day_date)

    start_time = datetime.datetime.combine(day_date, datetime.time.min)
    end_time = datetime.datetime.combine(day_date, datetime.time.max)
    time_range = (start_time, end_time)

    anomaly_summary: dict[str, dict] = {}

    site_config = config.sites.sites[site]
    for rx_name, rx_meta in site_config.receivers.items():
        rx_type = rx_meta.receiver_type

        if stats_store.is_anomaly_range_processed(rx_type, start_str, end_str):
            logger.info(
                "detect_anomalies: %s/%s already processed for %s — skipping",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Load climatology grids
        grids = stats_store.load_climatology(rx_type)
        if not grids:
            logger.warning(
                "detect_anomalies: no climatology for %s/%s — skipping",
                site,
                rx_name,
            )
            continue

        # Load day's data
        try:
            day_ds = research_site.load_rinex_data(
                receiver_name=rx_name,
                time_range=time_range,
            )
        except Exception:
            logger.warning(
                "detect_anomalies: no data for %s/%s on %s",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Extract DOY and hour-of-day
        epochs = day_ds.coords["epoch"].values
        day_starts = epochs.astype("datetime64[D]")
        year_starts = day_starts.astype("datetime64[Y]")
        doys = (day_starts - year_starts).astype("timedelta64[D]").astype(np.int32) + 1
        seconds_of_day = (
            (epochs - day_starts).astype("timedelta64[s]").astype(np.float64)
        )
        hours = seconds_of_day / 3600.0

        n_sids = day_ds.sizes.get("sid", 1)
        rx_summary: dict[str, dict] = {}
        var_summaries: dict[str, tuple] = {}

        for var, grid in grids.items():
            if var not in day_ds.data_vars:
                continue

            values_2d = day_ds[var].values  # (epoch, sid)
            flat_values = values_2d.ravel()
            flat_doys = np.tile(doys, n_sids)
            flat_hours = np.tile(hours, n_sids)

            # Compute climatology mean/std for each observation
            means = np.empty_like(flat_values)
            stds = np.empty_like(flat_values)
            for i in range(len(flat_values)):
                m, s, _c = grid.climatology_at(int(flat_doys[i]), float(flat_hours[i]))
                means[i] = m
                stds[i] = s

            z_scores = anomaly_zscore_batch(flat_values, means, stds)
            classifications = classify_anomaly_batch(z_scores)

            # Count per classification level
            valid_z = z_scores[np.isfinite(z_scores)]
            n_normal = int(np.sum(classifications == "NORMAL"))
            n_mild = int(np.sum(classifications == "MILD"))
            n_moderate = int(np.sum(classifications == "MODERATE"))
            n_severe = int(np.sum(classifications == "SEVERE"))
            mean_abs_z = float(np.mean(np.abs(valid_z))) if len(valid_z) > 0 else 0.0
            max_abs_z = float(np.max(np.abs(valid_z))) if len(valid_z) > 0 else 0.0

            rx_summary[var] = {
                "n_normal": n_normal,
                "n_mild": n_mild,
                "n_moderate": n_moderate,
                "n_severe": n_severe,
                "mean_abs_z": mean_abs_z,
                "max_abs_z": max_abs_z,
            }
            var_summaries[var] = (
                n_normal,
                n_mild,
                n_moderate,
                n_severe,
                mean_abs_z,
                max_abs_z,
            )

        # Persist to Zarr
        if var_summaries:
            stats_store.save_anomaly_summary(rx_type, date_str, var_summaries)
        stats_store.record_anomaly_range(rx_type, start_str, end_str)
        anomaly_summary[rx_type] = rx_summary

        logger.info(
            "detect_anomalies: %s/%s — %d variables analysed",
            site,
            rx_name,
            len(rx_summary),
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "anomaly_summary": anomaly_summary,
    }


# ---------------------------------------------------------------------------
# Task 8 — detect_changepoints
# ---------------------------------------------------------------------------


def detect_changepoints(site: str, yyyydoy: str) -> dict:
    """Run BOCPD changepoint detection on daily means for each variable.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "changepoints": {rx_type: {var: {...}}}}``
    """
    import zarr

    from canvod.ops.statistics import StatisticsStore
    from canvod.store import GnssResearchSite
    from canvod.streamstats import BOCPDAccumulator

    config = load_config()
    date_obj = _resolve_date(yyyydoy)

    research_site = GnssResearchSite(site)
    store_path = config.processing.storage.get_statistics_store_path(site)

    root = zarr.open_group(str(store_path), mode="a")
    stats_store = StatisticsStore(root)

    day_date = date_obj.date
    start_str = str(day_date)
    end_str = str(day_date + datetime.timedelta(days=1))

    start_time = datetime.datetime.combine(day_date, datetime.time.min)
    end_time = datetime.datetime.combine(day_date, datetime.time.max)
    time_range = (start_time, end_time)

    changepoints: dict[str, dict] = {}

    site_config = config.sites.sites[site]
    for rx_name, rx_meta in site_config.receivers.items():
        rx_type = rx_meta.receiver_type

        if stats_store.is_bocpd_range_processed(rx_type, start_str, end_str):
            logger.info(
                "detect_changepoints: %s/%s already processed for %s — skipping",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        # Load day's data
        try:
            day_ds = research_site.load_rinex_data(
                receiver_name=rx_name,
                time_range=time_range,
            )
        except Exception:
            logger.warning(
                "detect_changepoints: no data for %s/%s on %s",
                site,
                rx_name,
                date_obj.to_str(),
            )
            continue

        variables = [
            v for v in day_ds.data_vars if np.issubdtype(day_ds[v].dtype, np.floating)
        ]

        # Load or create BOCPD accumulators
        accumulators = stats_store.load_bocpd(rx_type)
        rx_cp: dict[str, dict] = {}

        for var in variables:
            if var not in accumulators:
                accumulators[var] = BOCPDAccumulator()

            # One observation per day: daily mean over all valid obs
            values = day_ds[var].values.ravel()
            daily_mean = float(np.nanmean(values))
            if np.isfinite(daily_mean):
                accumulators[var].update(daily_mean)

            res = accumulators[var].result
            rx_cp[var] = {
                "cp_prob": float(res.changepoint_prob),
                "run_len": int(res.map_run_length),
                "pred_mean": float(res.predictive_mean),
                "pred_std": float(res.predictive_std),
            }

        stats_store.save_bocpd(accumulators, rx_type)
        stats_store.record_bocpd_range(rx_type, start_str, end_str)
        changepoints[rx_type] = rx_cp

        logger.info(
            "detect_changepoints: %s/%s — %d variables",
            site,
            rx_name,
            len(variables),
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "changepoints": changepoints,
    }


# ---------------------------------------------------------------------------
# Task 9 — snapshot_statistics
# ---------------------------------------------------------------------------


def snapshot_statistics(site: str, yyyydoy: str) -> dict:
    """Record pipeline completion for all receivers.

    Verifies that all sub-pipeline stages (statistics, climatology, anomaly,
    BOCPD) have been recorded for this epoch range, then writes a
    pipeline-completed marker.

    Parameters
    ----------
    site : str
        Research site name.
    yyyydoy : str
        Date in ``YYYYDDD`` format **or** Airflow ``ds`` (``YYYY-MM-DD``).

    Returns
    -------
    dict
        ``{"site", "yyyydoy", "receivers_completed", "status": "ok"}``
    """
    import zarr

    from canvod.ops.statistics import StatisticsStore

    config = load_config()
    date_obj = _resolve_date(yyyydoy)

    store_path = config.processing.storage.get_statistics_store_path(site)

    root = zarr.open_group(str(store_path), mode="a")
    stats_store = StatisticsStore(root)

    day_date = date_obj.date
    start_str = str(day_date)
    end_str = str(day_date + datetime.timedelta(days=1))

    receivers_completed: list[str] = []

    site_config = config.sites.sites[site]
    for rx_name, rx_meta in site_config.receivers.items():
        rx_type = rx_meta.receiver_type

        if stats_store.is_pipeline_completed(rx_type, start_str, end_str):
            logger.info(
                "snapshot_statistics: %s/%s already completed for %s — skipping",
                site,
                rx_name,
                date_obj.to_str(),
            )
            receivers_completed.append(rx_name)
            continue

        # Verify all sub-pipeline stages are recorded
        checks = {
            "statistics": stats_store.is_epoch_range_processed(
                rx_type, start_str, end_str
            ),
            "climatology": stats_store.is_climatology_range_processed(
                rx_type, start_str, end_str
            ),
            "anomaly": stats_store.is_anomaly_range_processed(
                rx_type, start_str, end_str
            ),
            "bocpd": stats_store.is_bocpd_range_processed(rx_type, start_str, end_str),
        }
        missing = [k for k, v in checks.items() if not v]
        if missing:
            logger.warning(
                "snapshot_statistics: %s/%s missing stages %s for %s",
                site,
                rx_name,
                missing,
                date_obj.to_str(),
            )
            continue

        stats_store.record_pipeline_completed(rx_type, start_str, end_str)
        receivers_completed.append(rx_name)
        logger.info(
            "snapshot_statistics: %s/%s completed for %s",
            site,
            rx_name,
            date_obj.to_str(),
        )

    return {
        "site": site,
        "yyyydoy": date_obj.to_str(),
        "receivers_completed": receivers_completed,
        "status": "ok",
    }
