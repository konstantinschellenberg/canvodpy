"""RINEX processing orchestration and Icechunk writing helpers."""

from __future__ import annotations

import contextlib
import json
import os
import time
from collections.abc import Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

try:
    from dask.distributed import Client
    from dask.distributed import as_completed as dask_as_completed

    _HAS_DISTRIBUTED = True
except ImportError:
    _HAS_DISTRIBUTED = False
    Client = None  # type: ignore[assignment,misc]
    dask_as_completed = None  # type: ignore[assignment]
from datetime import UTC, datetime
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import polars as pl
import pydantic_core
import xarray as xr
import zarr
from icechunk.session import ForkSession
from icechunk.xarray import to_icechunk
from natsort import natsorted
from pydantic import ValidationError
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from canvod.auxiliary.pipeline import AuxDataPipeline
from canvod.auxiliary.position import (
    ECEFPosition,
    add_spherical_coords_to_dataset,
    compute_spherical_coordinates,
)
from canvod.readers import DataDirMatcher, MatchedDirs
from canvod.store import GnssResearchSite
from canvod.utils.config import load_config
from canvod.utils.tools import get_version_from_pyproject
from canvodpy.logging import get_logger
from canvodpy.orchestrator.interpolator import (
    ClockConfig,
    ClockInterpolationStrategy,
    Sp3Config,
    Sp3InterpolationStrategy,
)
from canvodpy.utils.telemetry import trace_icechunk_write

# ============================================================================
# MODULE-LEVEL FUNCTIONS (Required for Dask / ProcessPoolExecutor serialization)
# ============================================================================


def _processing_progress() -> Progress:
    """Create a Rich progress bar for RINEX processing tasks."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("eta"),
        TimeRemainingColumn(),
    )


def preprocess_with_hermite_aux(
    rnx_file: Path,
    keep_vars: list[str] | None,
    aux_zarr_path: Path,
    receiver_position: ECEFPosition,
    receiver_type: str,
    keep_sids: list[str] | None = None,
    reader_name: str = "rinex3",
    use_sbf_geometry: bool = False,
    store_radial_distance: bool = False,
) -> tuple[Path, xr.Dataset, dict[str, xr.Dataset], dict[str, list[str]]]:
    """Read RINEX and compute coordinates using Hermite-interpolated aux data from Zarr.

    This function runs in separate processes, so it must be at module level.
    The aux data has already been interpolated using proper Hermite splines.

    Parameters
    ----------
    rnx_file : Path
        RINEX file path
    keep_vars : List[str]
        Variables to keep
    aux_zarr_path : Path
        Path to preprocessed aux data Zarr store (with Hermite interpolation)
    receiver_position : ECEFPosition
        Receiver position (computed once)
    receiver_type : str
        Receiver type
    keep_sids : list[str] | None, default None
        List of specific SIDs to keep. If None, keeps all possible SIDs.
    use_sbf_geometry : bool, default False
        If True and reader_name is "sbf", skip external orbit/clock downloads
        and transfer theta/phi directly from SBF SatVisibility blocks.
    store_radial_distance : bool, default False
        If True, keep the radial distance variable ``r`` in the output.

    Returns
    -------
    tuple[Path, xr.Dataset, dict[str, xr.Dataset], dict[str, list[str]]]
        File path, augmented dataset with phi/theta/r, auxiliary datasets dict,
        and SID issue dict with keys ``not_in_global_space``, ``dropped_by_filter``,
        ``dropped_no_ephemeris``.

    """
    _ = receiver_type
    log = get_logger(__name__).bind(
        file=str(rnx_file.name), receiver_type=receiver_type
    )

    # Try to use OpenTelemetry tracing if available
    try:
        from canvodpy.utils.telemetry import trace_rinex_processing

        tracer_context = trace_rinex_processing(file_name=rnx_file.name)
    except ImportError:
        from contextlib import nullcontext

        tracer_context = nullcontext()

    with tracer_context:
        try:
            t0 = time.perf_counter()
            log.info("rinex_preprocessing_started")

            # 1. Read GNSS file (reader selected via factory)
            log.debug("reading_gnss_file", file=str(rnx_file.name), reader=reader_name)
            from canvodpy.factories import ReaderFactory

            rnx = ReaderFactory.create(reader_name, fpath=rnx_file)
            ds, aux_datasets = rnx.to_ds_and_auxiliary(
                keep_data_vars=keep_vars,
                write_global_attrs=True,
                keep_sids=keep_sids,
            )
            ds.attrs["File Hash"] = rnx.file_hash
            t_rinex = time.perf_counter()

            # SBF-geometry fast path: use receiver-reported theta/phi, skip ephemeris
            if reader_name == "sbf" and use_sbf_geometry:
                meta_ds = aux_datasets.get("sbf_obs")
                if meta_ds is not None and "theta" in meta_ds and "phi" in meta_ds:
                    ds = ds.assign_coords(theta=meta_ds["theta"], phi=meta_ds["phi"])
                from canvod.auxiliary.preprocessing import flush_sid_accumulators

                sid_issues = flush_sid_accumulators()
                sid_issues["dropped_no_ephemeris"] = []
                return rnx_file, ds, aux_datasets, sid_issues
            log.debug(
                "rinex_loaded",
                dims=dict(ds.sizes),
                data_vars=list(ds.data_vars.keys()),
                coords=list(ds.coords.keys()),
                epochs=len(ds.epoch),
                sids=len(ds.sid),
            )

            # Filter variables
            if keep_vars:
                available_vars = [var for var in keep_vars if var in ds.data_vars]
                if available_vars:
                    ds = ds[available_vars]

            # 2. Open preprocessed aux data and select matching epochs
            aux_store = xr.open_zarr(aux_zarr_path, decode_timedelta=True)
            aux_slice = aux_store.sel(epoch=ds.epoch, method="nearest")

            # Eagerly load aux slice — batches all Zarr reads (X, Y, Z, clock)
            # in one pass instead of 3-4 separate lazy loads later
            aux_slice = aux_slice.load()
            t_aux = time.perf_counter()

            # 3. Find common SIDs between RINEX and aux data (inner join)
            rinex_sids = set(ds.sid.values)
            aux_sids = set(aux_slice.sid.values)
            common_sids = sorted(rinex_sids.intersection(aux_sids))

            if not common_sids:
                log.error(
                    "sid_intersection_empty",
                    rinex_sids=len(rinex_sids),
                    aux_sids=len(aux_sids),
                )
                raise ValueError(
                    f"No common SIDs found between RINEX ({len(rinex_sids)} sids) "
                    f"and aux data ({len(aux_sids)} sids)"
                )

            # Filter both datasets to common SIDs
            rinex_only = rinex_sids - aux_sids
            aux_only = aux_sids - rinex_sids
            ds = ds.sel(sid=common_sids)
            aux_slice = aux_slice.sel(sid=common_sids)
            t_sid = time.perf_counter()

            log.debug(
                "sid_filtering_complete",
                rinex_sids=len(rinex_sids),
                aux_sids=len(aux_sids),
                common_sids=len(common_sids),
                rinex_only=len(rinex_only),
                aux_only=len(aux_only),
            )
            if rinex_only:
                log.warning(
                    "sids_dropped_no_ephemeris",
                    file=str(rnx_file.name),
                    count=len(rinex_only),
                    sids=sorted(rinex_only),
                    hint=(
                        "These SIDs were observed in the file but have no matching "
                        "entry in the ephemeris/clock aux data and will be absent "
                        "from the stored dataset."
                    ),
                )

            # 4. Compute spherical coordinates (phi, theta, r) from ephemerides
            log.debug("computing_spherical_coordinates")
            ds_augmented = _compute_spherical_coords_fast(
                ds,
                aux_slice,
                receiver_position,
            )
            if not store_radial_distance and "r" in ds_augmented:
                ds_augmented = ds_augmented.drop_vars("r")
            t_coords = time.perf_counter()

            log.info(
                "rinex_preprocessing_complete",
                total_seconds=round(t_coords - t0, 2),
                rinex_read_seconds=round(t_rinex - t0, 2),
                aux_load_seconds=round(t_aux - t_rinex, 2),
                sid_filter_seconds=round(t_sid - t_aux, 4),
                coords_seconds=round(t_coords - t_sid, 2),
                dataset_size=dict(ds_augmented.sizes),
            )
        except (OSError, RuntimeError, ValueError, ValidationError) as e:
            log.error(
                "rinex_preprocessing_failed",
                error=str(e),
                exception=type(e).__name__,
                file=str(rnx_file.name),
                traceback_available=True,
            )
            raise

    from canvod.auxiliary.preprocessing import flush_sid_accumulators

    sid_issues = flush_sid_accumulators()
    sid_issues["dropped_no_ephemeris"] = sorted(rinex_only)
    return rnx_file, ds_augmented, aux_datasets, sid_issues


def _compute_spherical_coords_fast(
    rinex_ds: xr.Dataset,
    aux_ds: xr.Dataset,
    rx_pos: ECEFPosition,
) -> xr.Dataset:
    """Compute spherical coordinates using shared utility function.

    This function is used by the parallel processor and must remain
    at module level for Dask / ProcessPoolExecutor serialization.
    """
    # Get satellite positions (already interpolated with Hermite splines)
    sat_x = aux_ds["X"].values
    sat_y = aux_ds["Y"].values
    sat_z = aux_ds["Z"].values

    # Compute using shared function
    r, theta, phi = compute_spherical_coordinates(sat_x, sat_y, sat_z, rx_pos)

    # Add to dataset using shared function
    rinex_ds = add_spherical_coords_to_dataset(rinex_ds, r, theta, phi)

    # Optionally add clock corrections if available
    if "clock" in aux_ds.data_vars:
        rinex_ds = rinex_ds.assign({"clock": aux_ds["clock"]})

    return rinex_ds


# ============================================================================
# Coordinated Parallel Writing to Icechunk
# ============================================================================


def _sanitize_ds_for_write(ds: xr.Dataset) -> xr.Dataset:
    # Make a shallow copy and strip obviously non-serializable attrs
    ds = ds.copy()
    # Keep only simple types in .attrs
    clean_attrs = {}
    for k, v in list(ds.attrs.items()):
        if isinstance(v, (str, int, float, bool, type(None), np.generic)):
            clean_attrs[k] = v
        # allow numpy scalars
        elif isinstance(v, (np.integer, np.floating, np.bool_)):
            clean_attrs[k] = v.item()
        # else drop it silently
    ds.attrs = clean_attrs

    # Normalize encodings to be conservative (avoid dtype surprises)
    for vname in ds.data_vars:
        var = ds[vname]
        enc = var.encoding or {}
        # ensure dtype is a concrete numpy dtype if present
        if "dtype" in enc:
            enc["dtype"] = np.dtype(var.dtype)
        # drop object encodings we don't control
        for bad in ("compressor", "filters", "chunks", "preferred_chunks"):
            enc.pop(bad, None)
        var.encoding = enc
    return ds


def write_initial_rinex_ds_to_store(
    *,
    ds: xr.Dataset,
    fork: ForkSession,
    group: str,
) -> ForkSession:
    """Write a new receiver group to the store."""
    ds = _sanitize_ds_for_write(ds)
    ds.to_zarr(
        fork.store,
        group=group,
        consolidated=False,
        mode="w",  # create group
    )
    return fork


def append_rinex_ds_to_store(
    *,
    ds: xr.Dataset,
    fork: ForkSession,
    group: str,
) -> ForkSession:
    """Append to an existing receiver group in the store."""
    ds = _sanitize_ds_for_write(ds)
    ds.to_zarr(
        fork.store,
        region="auto",
        group=group,
        consolidated=False,
        mode="a",
    )
    return fork


def worker_task(
    rinex_file: Path,
    keep_vars: list[str],
    aux_zarr_path: Path,
    receiver_position: ECEFPosition,
    receiver_type: str,
    receiver_name: str,
    fork: ForkSession,
    is_first: bool,
    keep_sids: list[str] | None = None,
    reader_name: str = "rinex3",
) -> tuple[Path, ForkSession]:
    """Build an augmented dataset and write it to the given fork."""
    # 1) build augmented dataset
    fname, ds_augmented, _aux, _sids = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
        reader_name,
    )

    # 2) write to this fork (initial or append)
    if is_first:
        write_initial_rinex_ds_to_store(
            ds=ds_augmented,
            fork=fork,
            group=receiver_name,
        )
    else:
        append_rinex_ds_to_store(
            ds=ds_augmented,
            fork=fork,
            group=receiver_name,
        )

    # 3) return ONLY pickleable things (Path + ForkSession)
    return fname, fork


def worker_task_append_only(
    rinex_file: Path,
    keep_vars: list[str],
    aux_zarr_path: Path,
    receiver_position: ECEFPosition,
    receiver_type: str,
    receiver_name: str,
    fork: ForkSession,
    keep_sids: list[str] | None = None,
    reader_name: str = "rinex3",
) -> tuple[Path, ForkSession]:
    """Worker that only appends (group already exists)."""
    fname, ds_augmented, _aux, _sids = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
        reader_name,
    )

    ds_clean = _sanitize_ds_for_write(ds_augmented)
    ds_clean.to_zarr(
        fork.store,
        group=receiver_name,
        mode="a",
        append_dim="epoch",
    )

    return fname, fork


def worker_task_with_region_auto(
    rinex_file: Path,
    keep_vars: list[str],
    aux_zarr_path: Path,
    receiver_position: ECEFPosition,
    receiver_type: str,
    receiver_name: str,
    fork: ForkSession,
    keep_sids: list[str] | None = None,
    reader_name: str = "rinex3",
) -> ForkSession:
    """Worker uses region='auto' to write to correct position."""
    _fname, ds, _aux, _sids = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
        reader_name,
    )

    ds_clean = _sanitize_ds_for_write(ds)
    ds_clean.to_zarr(
        fork.store,
        group=receiver_name,
        mode="a",
        region="auto",  # ✅ Let xarray infer the region
        consolidated=False,
    )

    return fork  # Return the modified fork


# ============================================================================
# MAIN (HALF-PARALLEL) PROCESSOR CLASS
# ============================================================================


class RinexDataProcessor:
    """Orchestrates RINEX data processing with optimized parallelization.

    Pipeline:
    1. Initialize auxiliary data (ephemerides, clock) - ONCE
    2. Preprocess aux data with Hermite splines to disk - ONCE per day
    3. Parallel process RINEX files via Dask distributed (or ProcessPoolExecutor fallback)
    4. Each worker reads its time slice from preprocessed Zarr
    5. Compute spherical coordinates and append to Icechunk store
    6. Yield final daily datasets

    Parameters
    ----------
    matched_data_dirs : MatchedDirs
        Matched directories for canopy and reference data
    site : GnssResearchSite
        Research site with Icechunk stores
    aux_file_path : Path, optional
        Root path for auxiliary files
    n_max_workers : int | None, default None
        Maximum parallel workers (CPUs) for RINEX processing.
        ``None`` lets ``ProcessPoolExecutor`` auto-detect via
        ``os.cpu_count()``.
    dask_client : dask.distributed.Client, optional
        Dask distributed client for parallel task submission.
        When provided, tasks are submitted to the long-lived cluster.
        When ``None``, falls back to a short-lived ``ProcessPoolExecutor``.

    """

    def __init__(
        self,
        matched_data_dirs: MatchedDirs,
        site: GnssResearchSite,
        aux_file_path: Path | None = None,
        n_max_workers: int | None = None,
        dask_client: Client | None = None,
        reader_name: str = "rinex3",
        use_sbf_geometry: bool = False,
    ) -> None:
        t_init_start = time.perf_counter()

        self.matched_data_dirs = matched_data_dirs
        self.site = site
        self.aux_file_path = aux_file_path
        if n_max_workers is not None:
            self.n_max_workers = min(n_max_workers, os.cpu_count() or n_max_workers)
        else:
            self.n_max_workers = None
        self._dask_client = dask_client
        self._reader_name = reader_name  # fallback; prefer per-receiver reader_format
        self.use_sbf_geometry = use_sbf_geometry
        self._logger = get_logger(__name__).bind(
            site=site.site_name,
            workers=self.n_max_workers or os.cpu_count(),
            component="processor",  # Enable component-specific logging
        )
        # Dedicated logger for icechunk store operations
        self._icechunk_log = get_logger(__name__).bind(
            site=site.site_name,
            component="icechunk",
        )

        t_config_start = time.perf_counter()
        config = load_config()
        self._config = config  # cache to avoid re-reading YAML in methods
        self.keep_sids = config.sids.get_sids()

        # Cache config values formerly in globals
        aux_cfg = config.processing.aux_data
        self._agency = aux_cfg.agency
        self._product_type = aux_cfg.product_type
        servers = aux_cfg.get_ftp_servers(config.nasa_earthdata_acc_mail)
        self._ftp_server = servers[0][0]
        self._rinex_store_strategy = config.processing.storage.rinex_store_strategy
        t_config_end = time.perf_counter()

        self._logger.info(
            "processor_initialized",
            aux_file_path=str(aux_file_path) if aux_file_path else None,
            sid_filtering=len(self.keep_sids) if self.keep_sids else "all",
            cpu_count=os.cpu_count(),
            config_load_seconds=round(t_config_end - t_config_start, 4),
        )

        # Initialize auxiliary data pipeline (loads SP3 and CLK files)
        self.aux_pipeline = self._initialize_aux_pipeline()

        t_init_end = time.perf_counter()
        self._logger.info(
            "processor_init_complete",
            total_init_seconds=round(t_init_end - t_init_start, 2),
            config_seconds=round(t_config_end - t_config_start, 4),
            aux_pipeline_seconds=round(t_init_end - t_config_end, 2),
        )

    def _initialize_aux_pipeline(self) -> AuxDataPipeline:
        """Initialize and load auxiliary data pipeline.

        Returns
        -------
        AuxDataPipeline
            Loaded pipeline with ephemerides and clock data

        """
        t0 = time.perf_counter()
        self._logger.info(
            "aux_pipeline_initialization_started",
            agency=self._agency,
            product_type=self._product_type,
        )

        # Use cached config (avoids re-reading YAML)
        config = self._config
        user_email = config.nasa_earthdata_acc_mail

        # Determine aux_file_path: explicit > config aux_data_dir > site data root
        aux_file_path = self.aux_file_path
        if aux_file_path is None:
            configured_aux_dir = config.processing.storage.aux_data_dir
            if configured_aux_dir is not None:
                aux_file_path = configured_aux_dir
            else:
                aux_file_path = Path(self.site.site_config["gnss_site_data_root"])

        t1 = time.perf_counter()
        pipeline = AuxDataPipeline.create_standard(
            matched_dirs=self.matched_data_dirs,
            aux_file_path=aux_file_path,
            agency=self._agency,
            product_type=self._product_type,
            ftp_server=self._ftp_server,
            user_email=user_email,
            keep_sids=self.keep_sids,
        )
        t2 = time.perf_counter()

        self._logger.info(
            "aux_pipeline_create_standard_complete",
            duration_seconds=round(t2 - t1, 2),
        )

        # Load all auxiliary files into memory
        pipeline.load_all()
        t3 = time.perf_counter()

        self._logger.info(
            "aux_pipeline_initialization_complete",
            total_seconds=round(t3 - t0, 2),
            create_standard_seconds=round(t2 - t1, 2),
            load_all_seconds=round(t3 - t2, 2),
            products=list(pipeline._cache.keys())
            if hasattr(pipeline, "_cache")
            else [],
        )
        return pipeline

    def _make_reader(self, fpath: Path, reader_format: str | None = None):
        """Instantiate the configured GNSS reader for *fpath*.

        Parameters
        ----------
        fpath : Path
            GNSS data file.
        reader_format : str | None
            Reader format override.  Falls back to ``self._reader_name``.

        """
        from canvodpy.factories import ReaderFactory

        return ReaderFactory.create(reader_format or self._reader_name, fpath=fpath)

    @staticmethod
    def _parse_sampling_interval_from_filename(filename: str) -> float | None:
        """Extract sampling interval from RINEX v3 long filename.

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
            Sampling interval in seconds, or None if parsing fails.

        """
        import re

        # RINEX v3 long filename: XXXXNNXXX_R_YYYYDDDHHMM_DUR_FREQ_AA.rnx
        # The frequency field is the 5th underscore-separated component
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

    def _preprocess_aux_data_with_hermite(
        self,
        rinex_files: list[Path],
        output_path: Path,
        reader_format: str | None = None,
    ) -> float:
        """Preprocess auxiliary data using proper interpolation strategies."""
        t0 = time.perf_counter()
        self._logger.info(
            "aux_preprocessing_started",
            rinex_files=len(rinex_files),
            output_path=str(output_path),
            interpolation_method="hermite_cubic",
        )

        # 1. Detect sampling interval from filename (fast path)
        sampling_interval = self._parse_sampling_interval_from_filename(
            rinex_files[0].name,
        )
        # Derive day_start from the YYYYDOY we already know
        day_date = self.matched_data_dirs.yyyydoy.date
        day_start = np.datetime64(day_date, "D")

        if sampling_interval is not None:
            t1 = time.perf_counter()
            self._logger.info(
                "sampling_detected",
                sampling_interval_seconds=sampling_interval,
                source="filename",
                detection_seconds=round(t1 - t0, 4),
            )
        else:
            # Fallback: read first GNSS file (slow path)
            self._logger.debug(
                "sampling_detection_started",
                sample_file=rinex_files[0].name,
                reason="filename_parse_failed",
            )
            first_rnx = self._make_reader(rinex_files[0], reader_format)
            first_ds = first_rnx.to_ds(
                keep_data_vars=[],
                write_global_attrs=True,
            )
            t1 = time.perf_counter()
            time_diff = (first_ds.epoch[1] - first_ds.epoch[0]).values
            sampling_interval = float(time_diff / np.timedelta64(1, "s"))
            first_epoch = first_ds.epoch.values[0]
            day_start = np.datetime64(first_epoch.astype("datetime64[D]"))
            self._logger.info(
                "sampling_detected",
                sampling_interval_seconds=sampling_interval,
                source="full_rinex_read",
                rinex_read_seconds=round(t1 - t0, 2),
            )

        self._logger.debug(
            "day_boundaries_detected",
            day_start=str(day_start),
            sampling_interval=sampling_interval,
        )

        n_epochs = int(24 * 3600 / sampling_interval)
        target_epochs = day_start + np.arange(n_epochs) * np.timedelta64(
            int(sampling_interval), "s"
        )

        self._logger.info(
            "epoch_grid_generated",
            n_epochs=len(target_epochs),
            day_start=str(target_epochs[0]),
            day_end=str(target_epochs[-1]),
            coverage_hours=24,
        )

        # 4. Get auxiliary datasets from pipeline
        t2 = time.perf_counter()
        self._logger.debug("fetching_auxiliary_datasets")
        ephem_ds = self.aux_pipeline.get("ephemerides")
        clock_ds = self.aux_pipeline.get("clock")
        t3 = time.perf_counter()
        self._logger.debug(
            "auxiliary_datasets_fetched",
            duration_seconds=round(t3 - t2, 4),
            ephemeris_dims=dict(ephem_ds.sizes) if ephem_ds else None,
            clock_dims=dict(clock_ds.sizes) if clock_ds else None,
            ephemeris_vars=list(ephem_ds.data_vars.keys()) if ephem_ds else [],
            clock_vars=list(clock_ds.data_vars.keys()) if clock_ds else [],
        )

        # 5. Interpolate ephemerides using Hermite splines
        self._logger.info(
            "ephemeris_interpolation_started",
            method="hermite_cubic_with_velocities",
            target_epochs=len(target_epochs),
        )
        sp3_config = Sp3Config(use_velocities=True, fallback_method="linear")
        sp3_interpolator = Sp3InterpolationStrategy(config=sp3_config)

        t4 = time.perf_counter()
        ephem_interp = sp3_interpolator.interpolate(ephem_ds, target_epochs)
        t5 = time.perf_counter()

        self._logger.info(
            "ephemeris_interpolation_complete",
            duration_seconds=round(t5 - t4, 2),
            output_shape=dict(ephem_interp.sizes),
            sids=len(ephem_interp.sid),
        )

        # Store interpolation metadata
        ephem_interp.attrs["interpolator_config"] = sp3_interpolator.to_attrs()

        # 6. Interpolate clock corrections using piecewise linear
        self._logger.info(
            "clock_interpolation_started",
            method="piecewise_linear",
            target_epochs=len(target_epochs),
        )
        clock_config = ClockConfig(window_size=9, jump_threshold=1e-6)
        clock_interpolator = ClockInterpolationStrategy(config=clock_config)

        t6 = time.perf_counter()
        clock_interp = clock_interpolator.interpolate(clock_ds, target_epochs)
        t7 = time.perf_counter()

        self._logger.info(
            "clock_interpolation_complete",
            duration_seconds=round(t7 - t6, 2),
            output_shape=dict(clock_interp.sizes),
        )

        # Store interpolation metadata
        clock_interp.attrs["interpolator_config"] = clock_interpolator.to_attrs()

        # 7. Merge ephemerides and clock into single dataset
        self._logger.debug("merging_auxiliary_datasets")
        aux_processed = xr.merge([ephem_interp, clock_interp])
        t8 = time.perf_counter()
        self._logger.debug(
            "merge_complete",
            duration_seconds=round(t8 - t7, 4),
            final_dims=dict(aux_processed.sizes),
            final_vars=list(aux_processed.data_vars.keys()),
        )

        # 8. Write to Zarr
        self._logger.info(
            "aux_zarr_write_started",
            output_path=str(output_path),
            data_size=dict(aux_processed.sizes),
        )
        aux_processed.to_zarr(output_path, mode="w")
        t9 = time.perf_counter()

        self._logger.info(
            "aux_preprocessing_complete",
            total_seconds=round(t9 - t0, 2),
            rinex_read_seconds=round(t1 - t0, 2),
            aux_fetch_seconds=round(t3 - t2, 4),
            ephem_interp_seconds=round(t5 - t4, 2),
            clock_interp_seconds=round(t7 - t6, 2),
            merge_seconds=round(t8 - t7, 4),
            zarr_write_seconds=round(t9 - t8, 2),
            data_size=dict(aux_processed.sizes),
            output_path=str(output_path),
        )

        return sampling_interval

    def _get_rinex_files(
        self, rinex_dir: Path, reader_format: str | None = None
    ) -> list[Path]:
        """Get sorted list of GNSS data files from directory.

        Parameters
        ----------
        rinex_dir : Path
            Directory to search.
        reader_format : str | None
            If provided, only return files matching the format's glob
            patterns (from ``FORMAT_GLOB_PATTERNS``).  ``None`` returns
            all recognized GNSS files.

        """
        if not rinex_dir.exists():
            self._logger.warning("Directory does not exist: %s", rinex_dir)
            return []

        from canvod.readers.gnss_specs.constants import FORMAT_GLOB_PATTERNS

        if reader_format and reader_format in FORMAT_GLOB_PATTERNS:
            patterns = list(FORMAT_GLOB_PATTERNS[reader_format])
        else:
            patterns = ["*.??o", "*.??O", "*.rnx", "*.RNX", "*.??_"]

        rinex_files = []
        for pattern in patterns:
            files = list(rinex_dir.glob(pattern))
            rinex_files.extend(files)

        return natsorted(rinex_files)

    def _compute_receiver_position(
        self,
        position_files: list[Path],
        receiver_name: str,
        reader_format: str | None = None,
    ) -> ECEFPosition | None:
        """Compute ECEF position from the first valid GNSS file.

        Uses the ``ReaderFactory`` to create a minimal dataset (header-only)
        and extracts the ECEF position from its attributes.  This works for
        any registered reader format (RINEX, SBF, …) without format-specific
        logic.

        Parameters
        ----------
        position_files : list[Path]
            GNSS files to try (first valid one wins).
        receiver_name : str
            Receiver name for logging.
        reader_format : str | None
            Reader format name (e.g. ``"rinex3"``, ``"sbf"``).
            Falls back to ``self._reader_name`` when *None*.

        Returns
        -------
        ECEFPosition | None
            Computed position, or None if no valid file found.

        """
        fmt = reader_format or self._reader_name

        for ff in position_files:
            try:
                reader = self._make_reader(ff, reader_format=fmt)
                ds = reader.to_ds(keep_data_vars=[], write_global_attrs=True)
                receiver_position = ECEFPosition.from_ds_metadata(ds)
                self._logger.info(
                    "Computed receiver position for %s: %s",
                    receiver_name,
                    receiver_position,
                )
                return receiver_position
            except (ValidationError, pydantic_core.ValidationError) as e:
                self._logger.warning(
                    "Validation error for %s: %s",
                    ff.name,
                    e,
                )
            except (KeyError, OSError, RuntimeError, ValueError) as e:
                self._logger.warning(
                    "Could not extract position from %s: %s",
                    ff.name,
                    e,
                )

        self._logger.error(
            "No valid GNSS files found for position extraction for %s",
            receiver_name,
        )
        return None

    def _ensure_aux_data_preprocessed(
        self,
        canopy_files: list[Path],
        date_str: str,
        reader_format: str | None = None,
    ) -> Path:
        """Ensure auxiliary data is preprocessed and available.

        Parameters
        ----------
        canopy_files : list[Path]
            GNSS files for sampling detection
        date_str : str
            Date string (e.g., '2025213')
        reader_format : str | None
            Reader format for the files (used in sampling fallback).

        Returns
        -------
        Path
            Path to the preprocessed aux zarr file

        Raises
        ------
        RuntimeError
            If preprocessing fails or file doesn't exist after preprocessing
        """
        import shutil

        t0 = time.perf_counter()
        aux_base_dir = self._config.processing.storage.get_aux_data_dir()
        aux_zarr_path = aux_base_dir / f"aux_{date_str}.zarr"

        # Always reprocess from raw SP3/CLK files — the Hermite interpolation
        # is cheap and this avoids stale caches when SIDs change.
        had_cache = aux_zarr_path.exists()
        if had_cache:
            shutil.rmtree(aux_zarr_path)

        t1 = time.perf_counter()
        self._logger.info(
            "aux_preprocessing_required",
            output_path=str(aux_zarr_path),
            interpolation="hermite_cubic",
            cache_cleared=had_cache,
            rmtree_seconds=round(t1 - t0, 4) if had_cache else 0,
        )
        try:
            self._preprocess_aux_data_with_hermite(
                canopy_files, aux_zarr_path, reader_format=reader_format
            )

            if not aux_zarr_path.exists():
                raise RuntimeError(
                    f"Aux preprocessing completed but file not found: {aux_zarr_path}"
                )

            self._logger.info(
                "aux_preprocessing_verified",
                file_exists=True,
                path=str(aux_zarr_path),
            )
        except Exception as e:
            self._logger.error(
                "aux_preprocessing_failed",
                error=str(e),
                exception=type(e).__name__,
                path=str(aux_zarr_path),
            )
            raise

        return aux_zarr_path

    def _parallel_process_rinex(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
        reader_format: str | None = None,
    ) -> tuple[
        list[tuple[Path, xr.Dataset]],
        dict[Path, dict[str, xr.Dataset]],
        dict[str, list[str]],
    ]:
        """Parallel process RINEX files using Dask or ProcessPoolExecutor fallback.

        Uses TRUE parallelism (no GIL) with separate processes.
        Each worker reads only its time slice from the Zarr store.

        When a Dask client is available (``self._dask_client``), tasks are
        submitted to the long-lived cluster. Otherwise falls back to a
        short-lived ``ProcessPoolExecutor``.

        Parameters
        ----------
        rinex_files : list[Path]
            List of RINEX files to process
        keep_vars : list[str]
            Variables to keep
        aux_zarr_path : Path
            Path to preprocessed aux Zarr store (with Hermite interpolation)
        receiver_position : ECEFPosition
            Receiver position (computed once)
        receiver_type : str
            Receiver type
        reader_format : str | None
            Per-receiver reader format. Falls back to ``self._reader_name``.

        Returns
        -------
        tuple
            (augmented_datasets, aux_datasets_by_file, sid_issues) where
            augmented_datasets is sorted chronologically by filename.

        """
        effective_reader = reader_format or self._reader_name
        store_r = self._config.processing.processing.store_radial_distance
        if self._dask_client is not None and _HAS_DISTRIBUTED:
            return self._parallel_process_rinex_dask(
                rinex_files,
                keep_vars,
                aux_zarr_path,
                receiver_position,
                receiver_type,
                effective_reader,
                store_r,
            )
        return self._parallel_process_rinex_pool(
            rinex_files,
            keep_vars,
            aux_zarr_path,
            receiver_position,
            receiver_type,
            effective_reader,
            store_r,
        )

    def _parallel_process_rinex_dask(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
        reader_format: str | None = None,
        store_radial_distance: bool = False,
    ) -> tuple[
        list[tuple[Path, xr.Dataset]],
        dict[Path, dict[str, xr.Dataset]],
        dict[str, list[str]],
    ]:
        """Process RINEX files via the Dask distributed client."""
        start_time = time.time()
        client = self._dask_client

        self._logger.info(
            "parallel_processing_started",
            workers=self.n_max_workers,
            files=len(rinex_files),
            receiver_type=receiver_type,
            executor_type="dask.distributed",
        )

        effective_workers = self.n_max_workers or os.cpu_count() or 1
        self._logger.debug(
            "parallel_config",
            max_workers=self.n_max_workers,
            cpu_count=os.cpu_count(),
            files_per_worker=round(len(rinex_files) / effective_workers, 1),
        )

        results: list[tuple[Path, xr.Dataset]] = []
        aux_datasets_by_file: dict[Path, dict[str, xr.Dataset]] = {}
        sid_issues_agg: dict[str, set] = {}
        task_submission_start = time.time()

        # Submit all tasks to the Dask cluster
        effective_reader = reader_format or self._reader_name
        future_to_file = {
            client.submit(
                preprocess_with_hermite_aux,
                rinex_file,
                keep_vars,
                aux_zarr_path,
                receiver_position,
                receiver_type,
                self.keep_sids,
                effective_reader,
                self.use_sbf_geometry,
                store_radial_distance,
                pure=False,
            ): rinex_file
            for rinex_file in rinex_files
        }

        task_submission_time = time.time() - task_submission_start
        self._logger.debug(
            "tasks_submitted",
            task_count=len(future_to_file),
            submission_time_seconds=round(task_submission_time, 3),
        )

        # Collect results with progress bar
        completed_count = 0
        failed_count = 0

        yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
        desc = f"{yyyydoy} {receiver_type}"
        with _processing_progress() as progress:
            task = progress.add_task(desc, total=len(future_to_file))
            for fut in dask_as_completed(future_to_file):
                try:
                    fname, ds_augmented, aux, sids = fut.result()
                    results.append((fname, ds_augmented))
                    aux_datasets_by_file[fname] = aux
                    for key, vals in sids.items():
                        sid_issues_agg.setdefault(key, set()).update(vals)
                    completed_count += 1

                    if completed_count % 10 == 0:
                        self._logger.debug(
                            "processing_progress",
                            completed=completed_count,
                            total=len(future_to_file),
                            failed=failed_count,
                            progress_pct=round(
                                100 * completed_count / len(future_to_file), 1
                            ),
                        )
                except (OSError, RuntimeError, ValueError) as e:
                    failed_file = future_to_file[fut].name
                    failed_count += 1
                    self._logger.error(
                        "file_processing_failed",
                        file=failed_file,
                        error=str(e),
                        exception=type(e).__name__,
                        failed_count=failed_count,
                    )
                progress.advance(task)

        # Sort chronologically by filename
        self._logger.debug("sorting_results_chronologically")
        results.sort(key=lambda x: x[0].name)

        duration = time.time() - start_time
        self._logger.info(
            "parallel_processing_complete",
            files_processed=len(results),
            files_total=len(rinex_files),
            files_failed=len(rinex_files) - len(results),
            duration_seconds=round(duration, 2),
            avg_time_per_file=round(duration / len(rinex_files), 2)
            if rinex_files
            else 0,
            throughput_files_per_sec=round(len(results) / duration, 2)
            if duration > 0
            else 0,
        )
        sid_issues_final = {k: sorted(v) for k, v in sid_issues_agg.items()}
        return results, aux_datasets_by_file, sid_issues_final

    def _parallel_process_rinex_pool(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
        reader_format: str | None = None,
        store_radial_distance: bool = False,
    ) -> tuple[
        list[tuple[Path, xr.Dataset]],
        dict[Path, dict[str, xr.Dataset]],
        dict[str, list[str]],
    ]:
        """Fallback: process RINEX files via ProcessPoolExecutor."""
        start_time = time.time()
        self._logger.info(
            "parallel_processing_started",
            workers=self.n_max_workers,
            files=len(rinex_files),
            receiver_type=receiver_type,
            executor_type="ProcessPoolExecutor",
        )

        effective_workers = self.n_max_workers or os.cpu_count() or 1
        self._logger.debug(
            "parallel_config",
            max_workers=self.n_max_workers,
            cpu_count=os.cpu_count(),
            files_per_worker=round(len(rinex_files) / effective_workers, 1),
        )

        results: list[tuple[Path, xr.Dataset]] = []
        aux_datasets_by_file: dict[Path, dict[str, xr.Dataset]] = {}
        sid_issues_agg: dict[str, set] = {}
        task_submission_start = time.time()

        effective_reader = reader_format or self._reader_name
        with ProcessPoolExecutor(max_workers=self.n_max_workers) as executor:
            futures = {
                executor.submit(
                    preprocess_with_hermite_aux,
                    rinex_file,
                    keep_vars,
                    aux_zarr_path,
                    receiver_position,
                    receiver_type,
                    self.keep_sids,
                    effective_reader,
                    self.use_sbf_geometry,
                    store_radial_distance,
                ): rinex_file
                for rinex_file in rinex_files
            }

            task_submission_time = time.time() - task_submission_start
            self._logger.debug(
                "tasks_submitted",
                task_count=len(futures),
                submission_time_seconds=round(task_submission_time, 3),
            )

            completed_count = 0
            failed_count = 0

            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            desc = f"{yyyydoy} {receiver_type}"
            with _processing_progress() as progress:
                task = progress.add_task(desc, total=len(futures))
                for fut in as_completed(futures):
                    try:
                        fname, ds_augmented, aux, sids = fut.result()
                        results.append((fname, ds_augmented))
                        aux_datasets_by_file[fname] = aux
                        for key, vals in sids.items():
                            sid_issues_agg.setdefault(key, set()).update(vals)
                        completed_count += 1

                        if completed_count % 10 == 0:
                            self._logger.debug(
                                "processing_progress",
                                completed=completed_count,
                                total=len(futures),
                                failed=failed_count,
                                progress_pct=round(
                                    100 * completed_count / len(futures), 1
                                ),
                            )
                    except (OSError, RuntimeError, ValueError) as e:
                        failed_file = futures[fut].name
                        failed_count += 1
                        self._logger.error(
                            "file_processing_failed",
                            file=failed_file,
                            error=str(e),
                            exception=type(e).__name__,
                            failed_count=failed_count,
                        )
                    progress.advance(task)

        # Sort chronologically by filename
        self._logger.debug("sorting_results_chronologically")
        results.sort(key=lambda x: x[0].name)

        duration = time.time() - start_time
        self._logger.info(
            "parallel_processing_complete",
            files_processed=len(results),
            files_total=len(rinex_files),
            files_failed=len(rinex_files) - len(results),
            duration_seconds=round(duration, 2),
            avg_time_per_file=round(duration / len(rinex_files), 2)
            if rinex_files
            else 0,
            throughput_files_per_sec=round(len(results) / duration, 2)
            if duration > 0
            else 0,
        )
        sid_issues_final = {k: sorted(v) for k, v in sid_issues_agg.items()}
        return results, aux_datasets_by_file, sid_issues_final

    def _check_store_vars_consistency(
        self,
        session: ForkSession,
        receiver_name: str,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
    ) -> None:
        """Warn if the store has different variables than the current batch.

        Detects stale variables from previous runs with different keep_rnx_vars.
        """
        try:
            ds_store = xr.open_zarr(
                session.store, group=receiver_name, consolidated=False
            )
            store_vars = set(ds_store.data_vars)
        except (KeyError, zarr.errors.GroupNotFoundError):
            return  # New group, nothing to check

        if not augmented_datasets:
            return
        _, first_ds = augmented_datasets[0]
        batch_vars = set(first_ds.data_vars)

        stale_vars = store_vars - batch_vars
        missing_vars = batch_vars - store_vars

        if stale_vars:
            self._logger.warning(
                "store_has_stale_variables",
                receiver=receiver_name,
                stale_vars=sorted(stale_vars),
                hint=(
                    "The store contains variables not in the current keep_rnx_vars. "
                    "This causes dimension conflicts on read-back. "
                    "With rinex_store_strategy='overwrite', stale vars will be "
                    "dropped automatically. Otherwise delete the store and reprocess."
                ),
            )
        if missing_vars:
            self._logger.info(
                "store_missing_new_variables",
                receiver=receiver_name,
                new_vars=sorted(missing_vars),
            )

    def _prepare_store_for_overwrite(
        self,
        session: ForkSession,
        receiver_name: str,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        existing_hashes: set[str],
        file_hash_map: dict[Path, str | None],
    ) -> None:
        """Remove epochs that will be overwritten and drop stale variables.

        Reads the existing group, masks out temporal ranges of files being
        overwritten, drops data_vars not present in the incoming batch,
        then rewrites the group with mode="w".
        """
        log = self._logger

        # 1. Read existing store data
        try:
            ds_store = xr.open_zarr(
                session.store, group=receiver_name, consolidated=False
            ).load()  # .load() to detach from session store before mode="w"
        except (KeyError, zarr.errors.GroupNotFoundError):
            return  # New group, nothing to prepare

        # 2. Collect epoch ranges to remove (files that exist and will be overwritten)
        epochs_to_remove = []
        for fname, ds in augmented_datasets:
            h = file_hash_map.get(fname)
            if h and h in existing_hashes:
                start = np.datetime64(ds.epoch.min().values)
                end = np.datetime64(ds.epoch.max().values)
                epochs_to_remove.append((start, end))

        if not epochs_to_remove:
            return  # Nothing to overwrite

        log.info(
            "prepare_overwrite",
            receiver=receiver_name,
            ranges_to_remove=len(epochs_to_remove),
        )

        # 3. Build combined mask: keep epochs NOT covered by any overwrite range
        epoch_vals = ds_store.epoch.values
        keep_mask = np.ones(len(epoch_vals), dtype=bool)
        for start, end in epochs_to_remove:
            keep_mask &= (epoch_vals < start) | (epoch_vals > end)

        ds_filtered = ds_store.isel(epoch=keep_mask)

        # 4. Drop stale variables not in current batch
        if augmented_datasets:
            _, first_ds = augmented_datasets[0]
            batch_vars = set(first_ds.data_vars)
            stale_vars = set(ds_filtered.data_vars) - batch_vars
            if stale_vars:
                log.warning(
                    "dropping_stale_variables",
                    receiver=receiver_name,
                    stale_vars=sorted(stale_vars),
                )
                ds_filtered = ds_filtered.drop_vars(stale_vars)

        # 5. Backup metadata, rewrite group, restore metadata
        metadata_backup = self.site.rinex_store.backup_metadata_table(
            receiver_name, session
        )

        ds_filtered = self.site.rinex_store._normalize_encodings(ds_filtered)

        if ds_filtered.sizes.get("epoch", 0) > 0:
            to_icechunk(ds_filtered, session, group=receiver_name, mode="w")
        else:
            # No epochs remain — write empty structure from first incoming dataset
            _, first_ds = augmented_datasets[0]
            empty = self.site.rinex_store._normalize_encodings(first_ds.isel(epoch=[]))
            to_icechunk(empty, session, group=receiver_name, mode="w")

        if metadata_backup is not None:
            self.site.rinex_store.restore_metadata_table(
                receiver_name, metadata_backup, session
            )

    def _append_to_icechunk_slow(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
    ) -> None:
        """Sequentially append augmented datasets to Icechunk store.

        Uses the IcechunkDataReader pattern for proper deduplication
        and metadata tracking.

        Parameters
        ----------
        augmented_datasets : list[tuple[Path, xr.Dataset]]
            List of (filename, dataset) tuples
        receiver_name : str
            Receiver name (e.g., 'canopy', 'reference')
        rinex_files : list[Path]
            Original list of RINEX files (for context)

        """
        _ = rinex_files
        start_time = time.time()
        self._logger.info(
            "icechunk_write_started",
            receiver=receiver_name,
            datasets=len(augmented_datasets),
            strategy=self._rinex_store_strategy,
            mode="sequential",
        )

        groups = self.site.rinex_store.list_groups() or []
        version = get_version_from_pyproject()

        self._icechunk_log.debug(
            "store_opened",
            operation="list_groups",
            group_count=len(groups),
            groups=groups[:10]
            if len(groups) > 10
            else groups,  # Sample for large stores
        )

        self._logger.debug(
            "icechunk_store_info",
            existing_groups=groups,
            group_count=len(groups),
            target_group=receiver_name,
            version=version,
        )

        write_count = 0
        skip_count = 0
        append_count = 0

        yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
        desc = f"{yyyydoy} Appending {receiver_name}"
        _progress = _processing_progress()
        _progress.start()
        _task = _progress.add_task(desc, total=len(augmented_datasets))
        for idx, (fname, ds) in enumerate(augmented_datasets):
            log = self._logger.bind(file=fname.name)

            if idx % 20 == 0:  # Log progress every 20 files
                self._logger.debug(
                    "icechunk_write_progress",
                    processed=idx,
                    total=len(augmented_datasets),
                    written=write_count,
                    skipped=skip_count,
                    appended=append_count,
                )

            try:
                rel_path = self.site.rinex_store.rel_path_for_commit(fname)

                self._icechunk_log.debug(
                    "computing_rel_path",
                    file=fname.name,
                    rel_path=str(rel_path),
                )

                log.debug(
                    "processing_dataset",
                    index=idx,
                    total=len(augmented_datasets),
                    dims=dict(ds.sizes),
                )

                # Get file metadata
                rinex_hash = ds.attrs.get("File Hash")
                if not rinex_hash:
                    log.warning(
                        "file_missing_hash",
                        file=fname.name,
                        action="skip",
                    )
                    continue

                start_epoch = np.datetime64(ds.epoch.min().values)
                end_epoch = np.datetime64(ds.epoch.max().values)

                log.debug(
                    "dataset_metadata",
                    hash=rinex_hash[:16],
                    start_epoch=str(start_epoch),
                    end_epoch=str(end_epoch),
                    n_epochs=len(ds.epoch),
                    n_sids=len(ds.sid) if "sid" in ds.dims else 0,
                )

                # Check if file already exists
                check_start = time.time()
                self._icechunk_log.debug(
                    "checking_existence",
                    group=receiver_name,
                    hash=rinex_hash[:16],
                    start_epoch=str(start_epoch),
                    end_epoch=str(end_epoch),
                )
                exists, _matches = self.site.rinex_store.metadata_row_exists(
                    receiver_name, rinex_hash, start_epoch, end_epoch
                )
                check_duration = time.time() - check_start

                self._icechunk_log.debug(
                    "existence_check_result",
                    exists=exists,
                    matches=_matches,
                    duration_ms=round(check_duration * 1000, 1),
                )

                log.debug(
                    "existence_check_complete",
                    exists=exists,
                    check_duration_ms=round(check_duration * 1000, 1),
                )

                # Cleanse dataset attributes
                cleanse_start = time.time()
                ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                    ds,
                )
                self._icechunk_log.debug(
                    "dataset_cleansed",
                    duration_ms=round((time.time() - cleanse_start) * 1000, 1),
                    attrs_before=len(ds.attrs),
                    attrs_after=len(ds_clean.attrs),
                )

                # Handle different strategies based on self._rinex_store_strategy
                match (exists, self._rinex_store_strategy):
                    case (False, _) if receiver_name not in groups and idx == 0:
                        # Initial commit
                        log.debug("performing_initial_write", group=receiver_name)
                        msg = f"[v{version}] Initial commit: {rel_path}"

                        write_start = time.time()
                        self._icechunk_log.info(
                            "store_write_initial_start",
                            group=receiver_name,
                            file=fname.name,
                            commit_message=msg,
                        )
                        self.site.rinex_store.write_initial_group(
                            dataset=ds_clean,
                            group_name=receiver_name,
                            commit_message=msg,
                        )
                        write_duration = time.time() - write_start
                        self._icechunk_log.info(
                            "store_write_initial_complete",
                            group=receiver_name,
                            duration_seconds=round(write_duration, 2),
                        )
                        groups.append(receiver_name)
                        log.info("initial_write", file=fname.name, group=receiver_name)
                        write_count += 1

                    case (True, "skip"):
                        log.debug(
                            "file_skipped", file=fname.name, reason="already_exists"
                        )
                        self._icechunk_log.debug(
                            "store_metadata_append",
                            action="skip",
                            group=receiver_name,
                            hash=rinex_hash[:16],
                        )
                        self.site.rinex_store.append_metadata(
                            group_name=receiver_name,
                            rinex_hash=rinex_hash,
                            start=start_epoch,
                            end=end_epoch,
                            snapshot_id="none",
                            action="skip",
                            commit_msg=f"Skipped {rel_path}",
                            dataset_attrs=ds_clean.attrs,
                        )
                        skip_count += 1

                    case (True, "append"):
                        log.debug("performing_append", strategy="append")
                        msg = f"[v{version}] Appended {rel_path}"

                        append_start = time.time()
                        self._icechunk_log.info(
                            "store_append_start",
                            group=receiver_name,
                            file=fname.name,
                            append_dim="epoch",
                        )
                        self.site.rinex_store.append_to_group(
                            dataset=ds_clean,
                            group_name=receiver_name,
                            append_dim="epoch",
                            action="append",
                            commit_message=msg,
                        )
                        append_duration = time.time() - append_start
                        self._icechunk_log.info(
                            "store_append_complete",
                            group=receiver_name,
                            duration_seconds=round(append_duration, 2),
                        )
                        log.info("file_appended", file=fname.name)
                        append_count += 1

                    case (False, _):
                        msg = f"[v{version}] Wrote {rel_path}"

                        write_start = time.time()
                        self._icechunk_log.info(
                            "store_write_start",
                            group=receiver_name,
                            file=fname.name,
                            append_dim="epoch",
                        )
                        self.site.rinex_store.append_to_group(
                            dataset=ds_clean,
                            group_name=receiver_name,
                            append_dim="epoch",
                            action="write",
                            commit_message=msg,
                        )
                        write_duration = time.time() - write_start
                        self._icechunk_log.info(
                            "store_write_complete",
                            group=receiver_name,
                            duration_seconds=round(write_duration, 2),
                        )
                        log.info("file_written", file=fname.name)
                        write_count += 1

                    case (True, "overwrite"):
                        log.debug("performing_overwrite", strategy="overwrite")
                        msg = f"[v{version}] Overwrote {rel_path}"

                        overwrite_start = time.time()
                        self.site.rinex_store.overwrite_file_in_group(
                            dataset=ds_clean,
                            group_name=receiver_name,
                            rinex_hash=rinex_hash,
                            start=start_epoch,
                            end=end_epoch,
                            commit_message=msg,
                        )
                        overwrite_duration = time.time() - overwrite_start
                        self._icechunk_log.info(
                            "store_overwrite_complete",
                            group=receiver_name,
                            duration_seconds=round(overwrite_duration, 2),
                        )
                        log.info("file_overwritten", file=fname.name)
                        write_count += 1

                    case _:
                        log.warning(
                            "unhandled_strategy",
                            exists=exists,
                            strategy=self._rinex_store_strategy,
                            file=fname.name,
                        )

            except (OSError, RuntimeError, ValueError) as e:
                self._icechunk_log.error(
                    "store_operation_failed",
                    file=fname.name,
                    error=str(e),
                    exception=type(e).__name__,
                )
                log.error(
                    "icechunk_write_failed",
                    error=str(e),
                    exception=type(e).__name__,
                )
            _progress.advance(_task)
        _progress.stop()

        duration = time.time() - start_time

        self._icechunk_log.info(
            "store_session_complete",
            receiver=receiver_name,
            total_operations=len(augmented_datasets),
            written=write_count,
            appended=append_count,
            skipped=skip_count,
            duration_seconds=round(duration, 2),
            throughput_files_per_sec=round(len(augmented_datasets) / duration, 2)
            if duration > 0
            else 0,
        )

        self._logger.info(
            "icechunk_write_complete",
            receiver=receiver_name,
            duration_seconds=round(duration, 2),
            files_written=write_count,
            files_appended=append_count,
            files_skipped=skip_count,
            total_files=len(augmented_datasets),
        )

    def _append_to_icechunk_incrementally(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
    ) -> None:
        """Batch append with single commit.

        This method:
        1. Opens ONE session for all data writes
        2. Uses only to_icechunk() within the session (no nested sessions)
        3. Makes ONE commit for all data
        4. Writes metadata separately after commit succeeds
        """
        _ = rinex_files
        log = self._logger
        version = get_version_from_pyproject()

        t_start = time.time()

        # STEP 1: Batch check which files exist
        log.info("Batch checking %s files...", len(augmented_datasets))
        t1 = time.time()

        file_hash_map = {
            fname: ds.attrs.get("File Hash") for fname, ds in augmented_datasets
        }

        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )

        t2 = time.time()
        log.info(
            "Batch check complete in %.2fs: %s/%s existing",
            t2 - t1,
            len(existing_hashes),
            len(augmented_datasets),
        )

        # STEP 2: Branch-stage-promote for overwrite; direct main for others
        is_overwrite = self._rinex_store_strategy == "overwrite"
        temp_branch = None

        if is_overwrite:
            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            temp_branch = f"overwrite_{receiver_name}_{yyyydoy}"
            current_snapshot = next(
                self.site.rinex_store.repo.ancestry(branch="main")
            ).id
            try:
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            except Exception:
                self.site.rinex_store.repo.delete_branch(temp_branch)
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            log.info(
                "Created temp branch '%s' for overwrite (snapshot: %s...)",
                temp_branch,
                current_snapshot[:8],
            )
            branch = temp_branch
        else:
            branch = "main"

        log.info("Opening Icechunk session...")
        t3 = time.time()
        with self.site.rinex_store.writable_session(branch) as session:
            groups = self.site.rinex_store.list_groups() or []
            t4 = time.time()
            log.info("Session opened in %.2fs", t4 - t3)

            # Prepare store for overwrite (remove old epochs, drop stale vars)
            if is_overwrite and receiver_name in groups:
                self._prepare_store_for_overwrite(
                    session,
                    receiver_name,
                    augmented_datasets,
                    existing_hashes,
                    file_hash_map,
                )

            actions = {
                "initial": 0,
                "skipped": 0,
                "appended": 0,
                "written": 0,
                "overwritten": 0,
            }
            metadata_records = []  # Collect metadata to write after commit

            try:
                # STEP 3: Process all datasets using ONLY to_icechunk()
                log.info(
                    "Processing %s datasets...",
                    len(augmented_datasets),
                )
                t5 = time.time()

                for idx, (fname, ds) in enumerate(augmented_datasets):
                    # Progress logging
                    if idx % 20 == 0 and idx > 0:
                        elapsed = time.time() - t5
                        rate = idx / elapsed if elapsed > 0 else 0
                        log.info(
                            "  Progress: %s/%s (%.1f files/s)",
                            idx,
                            len(augmented_datasets),
                            rate,
                        )

                    file_log = log.bind(file=fname.name)
                    try:
                        rel_path = self.site.rinex_store.rel_path_for_commit(fname)
                        rinex_hash = file_hash_map[fname]

                        if not rinex_hash:
                            file_log.debug("no_hash_skipping")
                            continue

                        # Get time range for metadata
                        start_epoch = np.datetime64(ds.epoch.min().values)
                        end_epoch = np.datetime64(ds.epoch.max().values)

                        # Fast hash check
                        exists = rinex_hash in existing_hashes

                        # Cleanse dataset
                        ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                            ds,
                        )

                        # Collect metadata for ALL files (write later)
                        metadata_records.append(
                            {
                                "fname": fname,
                                "rinex_hash": rinex_hash,
                                "start": start_epoch,
                                "end": end_epoch,
                                "dataset_attrs": ds.attrs.copy(),
                                "exists": exists,
                                "rel_path": rel_path,
                            }
                        )

                        # Handle data writes using ONLY to_icechunk() with our session
                        match (exists, self._rinex_store_strategy):
                            case (False, _) if receiver_name not in groups and idx == 0:
                                # Initial group creation
                                size_mb = ds_clean.nbytes / (1024 * 1024)
                                with trace_icechunk_write(
                                    group_name=receiver_name,
                                    dataset_size_mb=size_mb,
                                    num_variables=len(ds_clean.data_vars),
                                ):
                                    to_icechunk(ds_clean, session, group=receiver_name)
                                groups.append(receiver_name)
                                actions["initial"] += 1
                                file_log.debug("write_initial", path=rel_path)

                            case (True, "skip"):
                                # File exists, skip writing data
                                actions["skipped"] += 1
                                file_log.debug("write_skipped", path=rel_path)

                            case (True, "append"):
                                # File exists but append anyway
                                size_mb = ds_clean.nbytes / (1024 * 1024)
                                with trace_icechunk_write(
                                    group_name=receiver_name,
                                    dataset_size_mb=size_mb,
                                    num_variables=len(ds_clean.data_vars),
                                ):
                                    to_icechunk(
                                        ds_clean,
                                        session,
                                        group=receiver_name,
                                        append_dim="epoch",
                                    )
                                actions["appended"] += 1
                                file_log.debug("write_appended", path=rel_path)

                            case (False, _):
                                # New file, write it
                                size_mb = ds_clean.nbytes / (1024 * 1024)
                                with trace_icechunk_write(
                                    group_name=receiver_name,
                                    dataset_size_mb=size_mb,
                                    num_variables=len(ds_clean.data_vars),
                                ):
                                    to_icechunk(
                                        ds_clean,
                                        session,
                                        group=receiver_name,
                                        append_dim="epoch",
                                    )
                                actions["written"] += 1
                                file_log.debug("write_complete", path=rel_path)

                            case (True, "overwrite"):
                                # Old data already removed by _prepare_store_for_overwrite
                                size_mb = ds_clean.nbytes / (1024 * 1024)
                                with trace_icechunk_write(
                                    group_name=receiver_name,
                                    dataset_size_mb=size_mb,
                                    num_variables=len(ds_clean.data_vars),
                                ):
                                    to_icechunk(
                                        ds_clean,
                                        session,
                                        group=receiver_name,
                                        append_dim="epoch",
                                    )
                                actions["overwritten"] += 1
                                file_log.debug("write_overwritten", path=rel_path)

                            case _:
                                file_log.warning(
                                    "unhandled_strategy",
                                    exists=exists,
                                    strategy=self._rinex_store_strategy,
                                    path=rel_path,
                                )

                    except (OSError, RuntimeError, ValueError):
                        file_log.exception("Failed to process file")

                t6 = time.time()
                log.info("Dataset processing complete in %.2fs", t6 - t5)

                # STEP 4: Single commit for all data
                summary = ", ".join(f"{k}={v}" for k, v in actions.items() if v > 0)
                commit_msg = (
                    f"[v{version}] {receiver_name} "
                    f"{self.matched_data_dirs.yyyydoy}: {summary}"
                )

                log.info("Committing: %s", summary)
                t7 = time.time()
                try:
                    snapshot_id = session.commit(commit_msg)
                    t8 = time.time()
                    log.info(
                        "Commit complete in %.2fs (snapshot: %s...)",
                        t8 - t7,
                        snapshot_id[:8],
                    )
                except Exception as e:
                    t8 = time.time()
                    if "no changes" in str(e).lower():
                        log.info(
                            "no_changes_to_commit (all files already ingested): %.2fs",
                            t8 - t7,
                        )
                    else:
                        raise

                # STEP 5: Write metadata (separate transactions after data commit)
                log.info(
                    "Writing metadata for %s files...",
                    len(metadata_records),
                )
                t9 = time.time()

                for record in metadata_records:
                    action = "skip" if record["exists"] else "write"
                    try:
                        self.site.rinex_store.append_metadata(
                            group_name=receiver_name,
                            rinex_hash=record["rinex_hash"],
                            start=record["start"],
                            end=record["end"],
                            snapshot_id=snapshot_id,
                            action=action,
                            commit_msg=f"{action}: {record['rel_path']}",
                            dataset_attrs=record["dataset_attrs"],
                        )
                    except (OSError, RuntimeError, ValueError):
                        log.warning(
                            "Failed to write metadata for %s",
                            record["fname"].name,
                        )

                    t10 = time.time()
                    log.info("Metadata written in %.2fs", t10 - t9)

                    # Timing summary
                    t_end = time.time()
                    log.info("\nTIMING BREAKDOWN:")
                    log.info("  Batch check:    %.2fs", t2 - t1)
                    log.info("  Open session:   %.2fs", t4 - t3)
                    log.info("  Process data:   %.2fs", t6 - t5)
                    log.info("  Commit:         %.2fs", t8 - t7)
                    log.info("  Metadata:       %.2fs", t10 - t9)
                    log.info("  TOTAL:          %.2fs", t_end - t_start)

                    log.info(
                        "Successfully processed %s files for '%s'",
                        len(augmented_datasets),
                        receiver_name,
                    )

            except (OSError, RuntimeError, ValueError):
                log.exception("Batch append failed")
                raise

        # Promote temp branch to main after successful commit
        if is_overwrite and temp_branch:
            try:
                new_tip = next(
                    self.site.rinex_store.repo.ancestry(branch=temp_branch)
                ).id
                self.site.rinex_store.repo.reset_branch("main", new_tip)
                log.info(
                    "Promoted %s to main (snapshot: %s...)",
                    temp_branch,
                    new_tip[:8],
                )
            finally:
                with contextlib.suppress(Exception):
                    self.site.rinex_store.repo.delete_branch(temp_branch)

    def _append_to_icechunk(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
        aux_datasets: dict[Path, dict[str, xr.Dataset]] | None = None,
        sid_issues: dict[str, list[str]] | None = None,
    ) -> None:
        """Batch append with single commit.

        This method:
        1. Opens ONE session for all data writes
        2. Uses only to_icechunk() within the session (no nested sessions)
        3. Makes ONE commit for all data
        4. Writes metadata separately after commit succeeds
        5. Writes any auxiliary datasets (e.g. SBF metadata) after commit
        """
        _ = rinex_files
        log = self._logger
        version = get_version_from_pyproject()

        t_start = time.time()

        self._icechunk_log.info(
            "batch_write_session_started",
            receiver=receiver_name,
            total_files=len(augmented_datasets),
            strategy="single_commit",
        )

        # STEP 1: Batch check which files exist
        log.info("Batch checking %s files...", len(augmented_datasets))
        t1 = time.time()

        self._icechunk_log.debug(
            "batch_check_started",
            receiver=receiver_name,
            files=len(augmented_datasets),
        )

        file_hash_map = {
            fname: ds.attrs.get("File Hash") for fname, ds in augmented_datasets
        }

        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )

        t2 = time.time()
        self._icechunk_log.info(
            "batch_check_complete",
            receiver=receiver_name,
            duration_seconds=round(t2 - t1, 2),
            existing=len(existing_hashes),
            total=len(augmented_datasets),
        )
        log.info(
            "Batch check complete in %.2fs: %s/%s existing",
            t2 - t1,
            len(existing_hashes),
            len(augmented_datasets),
        )

        # STEP 2: Branch-stage-promote for overwrite; direct main for others
        is_overwrite = self._rinex_store_strategy == "overwrite"
        temp_branch = None

        if is_overwrite:
            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            temp_branch = f"overwrite_{receiver_name}_{yyyydoy}"
            current_snapshot = next(
                self.site.rinex_store.repo.ancestry(branch="main")
            ).id
            try:
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            except Exception:
                # Branch may exist from a failed previous run; delete and recreate
                self.site.rinex_store.repo.delete_branch(temp_branch)
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            log.info(
                "Created temp branch '%s' for overwrite (snapshot: %s...)",
                temp_branch,
                current_snapshot[:8],
            )
            branch = temp_branch
        else:
            branch = "main"

        log.info("Opening Icechunk session...")
        t3 = time.time()
        with self.site.rinex_store.writable_session(branch) as session:
            groups = self.site.rinex_store.list_groups() or []
            t4 = time.time()
            log.info("Session opened in %.2fs", t4 - t3)

            if receiver_name in groups:
                self._check_store_vars_consistency(
                    session, receiver_name, augmented_datasets
                )

            # Prepare store for overwrite (remove old epochs, drop stale vars)
            if is_overwrite and receiver_name in groups:
                self._prepare_store_for_overwrite(
                    session,
                    receiver_name,
                    augmented_datasets,
                    existing_hashes,
                    file_hash_map,
                )

            actions = {
                "initial": 0,
                "skipped": 0,
                "appended": 0,
                "written": 0,
                "overwritten": 0,
            }
            metadata_records = []  # Collect metadata to write before commit

            try:
                # STEP 3: Process all datasets using ONLY to_icechunk()
                log.info(
                    "Processing %s datasets...",
                    len(augmented_datasets),
                )
                t5 = time.time()

                for idx, (fname, ds) in enumerate(augmented_datasets):
                    # Progress logging
                    if idx % 20 == 0 and idx > 0:
                        elapsed = time.time() - t5
                        rate = idx / elapsed if elapsed > 0 else 0
                        log.info(
                            "  Progress: %s/%s (%.1f files/s)",
                            idx,
                            len(augmented_datasets),
                            rate,
                        )

                    try:
                        rel_path = self.site.rinex_store.rel_path_for_commit(fname)
                        rinex_hash = file_hash_map[fname]

                        if not rinex_hash:
                            log.debug("No hash for %s, skipping", fname)
                            continue

                        # Get time range for metadata
                        start_epoch = np.datetime64(ds.epoch.min().values)
                        end_epoch = np.datetime64(ds.epoch.max().values)

                        # Fast hash check
                        exists = rinex_hash in existing_hashes

                        # Cleanse dataset
                        ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                            ds,
                        )

                        # Collect metadata for ALL files (write later)
                        metadata_records.append(
                            {
                                "fname": fname,
                                "rinex_hash": rinex_hash,
                                "start": start_epoch,
                                "end": end_epoch,
                                "dataset_attrs": ds.attrs.copy(),
                                "exists": exists,
                                "rel_path": rel_path,
                            }
                        )

                        # Handle data writes using ONLY to_icechunk() with our session
                        match (exists, self._rinex_store_strategy):
                            case (False, _) if receiver_name not in groups and idx == 0:
                                # Initial group creation
                                to_icechunk(ds_clean, session, group=receiver_name)
                                groups.append(receiver_name)
                                actions["initial"] += 1
                                log.debug("Initial: %s", rel_path)

                            case (True, "skip"):
                                # File exists, skip writing data
                                actions["skipped"] += 1
                                log.debug("Skipped: %s", rel_path)

                            case (True, "append"):
                                # File exists but append anyway
                                to_icechunk(
                                    ds_clean,
                                    session,
                                    group=receiver_name,
                                    append_dim="epoch",
                                )
                                actions["appended"] += 1
                                log.debug("Appended: %s", rel_path)

                            case (False, _):
                                # New file, write it
                                to_icechunk(
                                    ds_clean,
                                    session,
                                    group=receiver_name,
                                    append_dim="epoch",
                                )
                                actions["written"] += 1
                                log.debug("Wrote: %s", rel_path)

                            case (True, "overwrite"):
                                # Old data already removed by _prepare_store_for_overwrite
                                to_icechunk(
                                    ds_clean,
                                    session,
                                    group=receiver_name,
                                    append_dim="epoch",
                                )
                                actions["overwritten"] += 1
                                log.debug("Overwrote: %s", rel_path)

                            case _:
                                log.warning(
                                    "Unhandled strategy: exists=%s, strategy=%s for %s",
                                    exists,
                                    self._rinex_store_strategy,
                                    rel_path,
                                )

                    except (OSError, RuntimeError, ValueError):
                        log.exception("Failed to process %s", fname.name)

                t6 = time.time()
                log.info("Dataset processing complete in %.2fs", t6 - t5)

                # STEP 4: Write metadata, then single commit for data + metadata
                summary = ", ".join(f"{k}={v}" for k, v in actions.items() if v > 0)
                commit_msg = (
                    f"[v{version}] {receiver_name} "
                    f"{self.matched_data_dirs.yyyydoy}: {summary}"
                )

                log.info(
                    "Writing metadata for %s files...",
                    len(metadata_records),
                )
                t9 = time.time()
                try:
                    self.site.rinex_store.append_metadata_bulk(
                        group_name=receiver_name,
                        rows=metadata_records,
                        session=session,
                    )
                except (OSError, RuntimeError, ValueError):
                    log.warning("Metadata write failed, committing data only")
                t10 = time.time()
                log.info("Metadata write complete in %.2fs", t10 - t9)

                log.info("Committing: %s", summary)
                t7 = time.time()
                snapshot_id = session.commit(commit_msg)
                t8 = time.time()
                log.info(
                    "Commit complete in %.2fs (snapshot: %s...)",
                    t8 - t7,
                    snapshot_id[:8],
                )

                expired = self.site.rinex_store.expire_old_snapshots()

                if expired:
                    print(f"Expired {len(expired)} snapshots for cleanup.")

                # Timing summary
                t_end = time.time()

                self._icechunk_log.info(
                    "batch_write_complete",
                    receiver=receiver_name,
                    total_files=len(augmented_datasets),
                    duration_seconds=round(t_end - t_start, 2),
                    timings={
                        "batch_check": round(t2 - t1, 2),
                        "open_session": round(t4 - t3, 2),
                        "process_data": round(t6 - t5, 2),
                        "commit": round(t8 - t7, 2),
                        "metadata": round(t10 - t9, 2),
                    },
                    actions=actions,
                    throughput_files_per_sec=round(
                        len(augmented_datasets) / (t_end - t_start), 2
                    ),
                )

                log.info("\nTIMING BREAKDOWN:")
                log.info("  Batch check:    %.2fs", t2 - t1)
                log.info("  Open session:   %.2fs", t4 - t3)
                log.info("  Process data:   %.2fs", t6 - t5)
                log.info("  Commit:         %.2fs", t8 - t7)
                log.info("  Metadata:       %.2fs", t10 - t9)
                log.info("  TOTAL:          %.2fs", t_end - t_start)

                log.info(
                    "Successfully processed %s files for '%s'",
                    len(augmented_datasets),
                    receiver_name,
                )

            except (OSError, RuntimeError, ValueError):
                log.exception("Batch append failed")
                raise

        # Promote temp branch to main after successful commit
        if is_overwrite and temp_branch:
            try:
                new_tip = next(
                    self.site.rinex_store.repo.ancestry(branch=temp_branch)
                ).id
                self.site.rinex_store.repo.reset_branch("main", new_tip)
                log.info(
                    "Promoted %s to main (snapshot: %s...)",
                    temp_branch,
                    new_tip[:8],
                )
            finally:
                with contextlib.suppress(Exception):
                    self.site.rinex_store.repo.delete_branch(temp_branch)

    def _append_to_icechunk_parallel(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
    ) -> None:
        """Batch append with parallel writes and a single commit.

        May be slower than sequential writes due to locking overhead.

        Strategy:
        - One writable session
        - ThreadPoolExecutor for dataset writes (safe: GIL release in zarr/numcodecs IO)
        - One commit for data
        - One commit for metadata
        """
        _ = rinex_files
        log = self._logger
        version = get_version_from_pyproject()

        t_start = time.time()

        # STEP 1: Batch check which files exist
        log.info("Batch checking %s files...", len(augmented_datasets))
        t1 = time.time()
        file_hash_map = {
            fname: ds.attrs.get("File Hash") for fname, ds in augmented_datasets
        }
        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )
        t2 = time.time()
        log.info(
            "Batch check complete in %.2fs: %s/%s existing",
            t2 - t1,
            len(existing_hashes),
            len(augmented_datasets),
        )

        # STEP 2: Branch-stage-promote for overwrite; direct main for others
        is_overwrite = self._rinex_store_strategy == "overwrite"
        temp_branch = None

        if is_overwrite:
            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            temp_branch = f"overwrite_{receiver_name}_{yyyydoy}"
            current_snapshot = next(
                self.site.rinex_store.repo.ancestry(branch="main")
            ).id
            try:
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            except Exception:
                self.site.rinex_store.repo.delete_branch(temp_branch)
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            log.info(
                "Created temp branch '%s' for overwrite (snapshot: %s...)",
                temp_branch,
                current_snapshot[:8],
            )
            branch = temp_branch
        else:
            branch = "main"

        log.info("Opening Icechunk session...")
        t3 = time.time()
        session = self.site.rinex_store.repo.writable_session(branch=branch)
        groups = self.site.rinex_store.list_groups() or []
        t4 = time.time()
        log.info("Session opened in %.2fs", t4 - t3)

        # Prepare store for overwrite (remove old epochs, drop stale vars)
        if is_overwrite and receiver_name in groups:
            self._prepare_store_for_overwrite(
                session,
                receiver_name,
                augmented_datasets,
                existing_hashes,
                file_hash_map,
            )

        actions = {
            "initial": 0,
            "skipped": 0,
            "appended": 0,
            "written": 0,
            "overwritten": 0,
        }
        metadata_records = []  # Collect metadata to write after commit

        try:
            log.info(
                "Processing %s datasets...",
                len(augmented_datasets),
            )
            t5 = time.time()

            def write_one(
                fname: Path,
                ds: xr.Dataset,
                exists: bool,
                idx: int,
            ) -> str:
                ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                    ds,
                )
                rel_path = self.site.rinex_store.rel_path_for_commit(fname)

                # Collect metadata
                start_epoch = np.datetime64(ds.epoch.min().values)
                end_epoch = np.datetime64(ds.epoch.max().values)
                metadata_records.append(
                    {
                        "fname": fname,
                        "rinex_hash": file_hash_map[fname],
                        "start": start_epoch,
                        "end": end_epoch,
                        "dataset_attrs": ds.attrs.copy(),
                        "exists": exists,
                        "rel_path": rel_path,
                    }
                )

                # Decide write strategy
                match (exists, self._rinex_store_strategy):
                    case (False, _) if receiver_name not in groups and idx == 0:
                        to_icechunk(ds_clean, session, group=receiver_name)
                        groups.append(receiver_name)
                        return "initial"
                    case (True, "skip"):
                        return "skipped"
                    case (True, "append"):
                        to_icechunk(
                            ds_clean, session, group=receiver_name, append_dim="epoch"
                        )
                        return "appended"
                    case (False, _):
                        to_icechunk(
                            ds_clean, session, group=receiver_name, append_dim="epoch"
                        )
                        return "written"

                    case (True, "overwrite"):
                        # Old data already removed by _prepare_store_for_overwrite
                        to_icechunk(
                            ds_clean, session, group=receiver_name, append_dim="epoch"
                        )
                        return "overwritten"

                    case _:
                        log.warning(
                            "Unhandled strategy: exists=%s, strategy=%s for %s",
                            exists,
                            self._rinex_store_strategy,
                            rel_path,
                        )
                        return "skipped"

            # --- THREADPOOL EXECUTION ---
            futures = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                for idx, (fname, ds) in enumerate(augmented_datasets):
                    rinex_hash = file_hash_map[fname]
                    if not rinex_hash:
                        continue
                    exists = rinex_hash in existing_hashes
                    if exists and self._rinex_store_strategy == "skip":
                        actions["skipped"] += 1
                        continue
                    futures.append(pool.submit(write_one, fname, ds, exists, idx))

                for fut in as_completed(futures):
                    result = fut.result()
                    actions[result] += 1

            t6 = time.time()
            log.info("Dataset processing complete in %.2fs", t6 - t5)

            # STEP 4: Single commit for all data
            summary = ", ".join(f"{k}={v}" for k, v in actions.items() if v > 0)
            commit_msg = (
                f"[v{version}] {receiver_name} "
                f"{self.matched_data_dirs.yyyydoy}: {summary}"
            )
            log.info("Committing data: %s", summary)
            t7 = time.time()
            try:
                snapshot_id = session.commit(commit_msg)
            except Exception as e:
                if "no changes" in str(e).lower():
                    log.info("no_changes_to_commit (all files already ingested)")
                    snapshot_id = None
                else:
                    raise
            t8 = time.time()
            log.info(
                "Commit complete in %.2fs (snapshot: %s...)",
                t8 - t7,
                snapshot_id[:8] if snapshot_id else "no-op",
            )

            # STEP 5: Metadata in a separate commit
            log.info(
                "Writing metadata for %s files...",
                len(metadata_records),
            )
            t9 = time.time()
            try:
                meta_session = self.site.rinex_store.repo.writable_session(
                    branch=branch
                )
                self.site.rinex_store.append_metadata_bulk(
                    group_name=receiver_name,
                    rows=metadata_records,
                    session=meta_session,
                    snapshot_id=snapshot_id,
                )
                meta_commit_msg = (
                    f"[v{version}] metadata for {receiver_name} "
                    f"{self.matched_data_dirs.yyyydoy}"
                )
                meta_session.commit(meta_commit_msg)
            except (OSError, RuntimeError, ValueError):
                log.warning("Metadata commit failed")
            t10 = time.time()
            log.info("Metadata commit complete in %.2fs", t10 - t9)

            # Timing summary
            t_end = time.time()
            log.info("\nTIMING BREAKDOWN:")
            log.info("  Batch check:    %.2fs", t2 - t1)
            log.info("  Open session:   %.2fs", t4 - t3)
            log.info("  Process data:   %.2fs", t6 - t5)
            log.info("  Commit:         %.2fs", t8 - t7)
            log.info("  Metadata:       %.2fs", t10 - t9)
            log.info("  TOTAL:          %.2fs", t_end - t_start)
            log.info(
                "Successfully processed %s files for '%s'",
                len(augmented_datasets),
                receiver_name,
            )

        except (OSError, RuntimeError, ValueError):
            log.exception("Batch append failed")
            raise

        # Promote temp branch to main after successful commit
        if is_overwrite and temp_branch:
            try:
                new_tip = next(
                    self.site.rinex_store.repo.ancestry(branch=temp_branch)
                ).id
                self.site.rinex_store.repo.reset_branch("main", new_tip)
                log.info(
                    "Promoted %s to main (snapshot: %s...)",
                    temp_branch,
                    new_tip[:8],
                )
            finally:
                with contextlib.suppress(Exception):
                    self.site.rinex_store.repo.delete_branch(temp_branch)

    def _resolve_receiver_paths(self, receiver_type: str) -> tuple[Path, str | None]:
        """Resolve paths and receiver name for receiver type.

        Parameters
        ----------
        receiver_type : str
            Type of receiver ('canopy' or 'reference')

        Returns
        -------
        tuple[Path, str | None]
            (rinex_dir, receiver_name)

        """
        if receiver_type == "canopy":
            rinex_dir = self.matched_data_dirs.canopy_data_dir
        elif receiver_type == "reference":
            rinex_dir = self.matched_data_dirs.reference_data_dir
        else:
            msg = f"Unknown receiver type: {receiver_type}"
            raise ValueError(msg)

        # Get receiver name from site configuration
        receiver_name = None
        for name, config in self.site.active_receivers.items():
            if config.get("type") == receiver_type:
                receiver_name = name
                break

        return rinex_dir, receiver_name

    def parsed_rinex_data_gen_2_receivers(
        self,
        keep_vars: list[str] | None = None,
        receiver_types: list[str] | None = None,
    ) -> Generator[xr.Dataset]:
        """Generate datasets from RINEX files and append to Icechunk stores.

        Pipeline:
        1. Preprocess aux data ONCE per day with Hermite splines → Zarr
        2. Compute receiver position ONCE (shared for all receivers)
        3. For each receiver type (canopy, reference):
           a. Get list of RINEX files
           b. Parallel process via Dask distributed (or ProcessPoolExecutor fallback)
           c. Each worker: read RINEX + slice Zarr + compute φ, θ, r
           d. Sequential append to Icechunk store
           e. Yield final daily dataset

        Parameters
        ----------
        keep_vars : List[str], optional
            Variables to keep in datasets (default: from globals)
        receiver_types : List[str], optional
            Receiver types to process (default: ['canopy', 'reference'])

        Yields
        ------
        xr.Dataset
            Processed and augmented daily dataset for each receiver type

        """
        if receiver_types is None:
            receiver_types = ["canopy", "reference"]

        if keep_vars is None:
            keep_vars = load_config().processing.processing.keep_rnx_vars

        self._logger.info(
            "Starting RINEX processing pipeline for: %s",
            receiver_types,
        )

        # Pre-flight: Get canopy files to infer sampling and compute position
        canopy_dir = self.matched_data_dirs.canopy_data_dir
        canopy_files = self._get_rinex_files(canopy_dir)
        if not canopy_files:
            msg = "No canopy RINEX files found - cannot infer sampling rate"
            raise ValueError(msg)

        # ====================================================================
        # STEP 1: Preprocess aux data ONCE per day with Hermite splines
        # ====================================================================
        import shutil as _shutil

        _aux_base_dir = load_config().processing.storage.get_aux_data_dir()
        aux_zarr_path = _aux_base_dir / (
            f"aux_{self.matched_data_dirs.yyyydoy.to_str()}.zarr"
        )

        # Always reprocess from raw SP3/CLK files to avoid stale SID caches
        if aux_zarr_path.exists():
            _shutil.rmtree(aux_zarr_path)

        self._logger.info("Preprocessing aux data with Hermite splines (once per day)")
        _sampling_interval = self._preprocess_aux_data_with_hermite(
            canopy_files, aux_zarr_path
        )

        # ====================================================================
        # STEP 2: Compute receiver position
        # ====================================================================
        position_mode = self._config.processing.processing.receiver_position_mode
        first_rnx = self._make_reader(canopy_files[0])
        first_ds = first_rnx.to_ds(keep_data_vars=[], write_global_attrs=True)
        shared_position = ECEFPosition.from_ds_metadata(first_ds)

        if position_mode == "per_receiver":
            self._logger.warning(
                "receiver_position_mode='per_receiver': each receiver will use "
                "its own RINEX header position. This breaks direct SNR "
                "comparability between receivers."
            )
        else:
            self._logger.info(
                "Computed receiver position (shared): %s",
                shared_position,
            )

        # ====================================================================
        # STEP 3: Process each receiver type
        # ====================================================================
        for receiver_type in receiver_types:
            self._logger.info("Processing receiver type: %s", receiver_type)

            # 3a. Resolve directories and receiver name
            rinex_dir, receiver_name = self._resolve_receiver_paths(receiver_type)

            if not receiver_name:
                self._logger.warning(
                    "No configured receiver for %s, skipping",
                    receiver_type,
                )
                continue

            # 3b. Get RINEX files for this receiver type
            rinex_files = self._get_rinex_files(rinex_dir)
            if not rinex_files:
                self._logger.warning(
                    "No RINEX files found in %s",
                    rinex_dir,
                )
                continue

            self._logger.info(
                "Found %s RINEX files to process",
                len(rinex_files),
            )

            # 3b'. Determine receiver position for this receiver
            if position_mode == "per_receiver":
                receiver_position = self._compute_receiver_position(
                    rinex_files, receiver_name
                )
                if receiver_position is None:
                    self._logger.error(
                        "Could not compute position for %s, skipping",
                        receiver_name,
                    )
                    continue
            else:
                receiver_position = shared_position

            # 3c. Parallel process via Dask (or ProcessPoolExecutor fallback)
            augmented_datasets, aux_datasets, sid_issues = self._parallel_process_rinex(
                rinex_files=rinex_files,
                keep_vars=keep_vars,
                aux_zarr_path=aux_zarr_path,
                receiver_position=receiver_position,
                receiver_type=receiver_name,
            )

            # 3d. Sequential append to Icechunk store
            self._append_to_icechunk(
                augmented_datasets=augmented_datasets,
                receiver_name=receiver_name,
                rinex_files=rinex_files,
                aux_datasets=aux_datasets,
                sid_issues=sid_issues,
            )

            # 3e. Yield final daily dataset
            # Read back from store to get complete daily dataset
            date_obj = self.matched_data_dirs.yyyydoy.date
            start_time = datetime.combine(date_obj, datetime.min.time())
            end_time = datetime.combine(date_obj, datetime.max.time())
            time_range = (start_time, end_time)

            daily_dataset = self.site.read_receiver_data(
                receiver_name=receiver_name, time_range=time_range
            )

            self._logger.info(
                "Yielding daily dataset for %s ('%s'): %s",
                receiver_type,
                receiver_name,
                dict(daily_dataset.sizes),
            )

            yield daily_dataset

    def prepare_batch_tasks(
        self,
        keep_vars: list[str] | None,
        receiver_configs: list[tuple[str, str, Path, Path | None, str]],
    ) -> tuple[list[tuple], list[tuple[str, list[Path]]]]:
        """Prepare aux Zarr and task descriptors for flat Dask submission.

        Performs Phase 1 work for one DOY without submitting to Dask:
        normalize configs, preprocess aux data, compute positions, and
        build a flat list of task arguments.

        Parameters
        ----------
        keep_vars : list[str] | None
            Variables to keep in datasets.
        receiver_configs : list[tuple[str, str, Path, Path | None, str]]
            ``(receiver_name, receiver_type, data_dir, position_data_dir, reader_format)``
            tuples.

        Returns
        -------
        task_descriptors : list[tuple]
            Each tuple contains the args for ``preprocess_with_hermite_aux``:
            ``(rnx_file, keep_vars, aux_zarr_path, position, receiver_name, keep_sids)``.
        receiver_file_map : list[tuple[str, list[Path]]]
            ``(receiver_name, rinex_files)`` for each receiver — needed for the
            Icechunk write phase.

        """
        t_batch_start = time.perf_counter()
        if keep_vars is None:
            keep_vars = self._config.processing.processing.keep_rnx_vars

        # Get first receiver files to infer sampling rate for aux preprocessing
        first_receiver_name, _first_type, first_data_dir, _, first_fmt = (
            receiver_configs[0]
        )
        first_files = self._get_rinex_files(first_data_dir, first_fmt)

        if not first_files:
            msg = (
                f"No RINEX files found for {first_receiver_name} - "
                "cannot infer sampling rate"
            )
            self._logger.error(
                "prepare_batch_failed",
                reason="no_rinex_files",
                receiver=first_receiver_name,
            )
            raise ValueError(msg)

        date_str = self.matched_data_dirs.yyyydoy.to_str()
        aux_zarr_path = self._ensure_aux_data_preprocessed(
            first_files, date_str, reader_format=first_fmt
        )
        t_aux_done = time.perf_counter()

        task_descriptors: list[tuple] = []
        receiver_file_map: list[tuple[str, list[Path]]] = []

        for (
            receiver_name,
            _receiver_type,
            data_dir,
            position_data_dir,
            reader_format,
        ) in receiver_configs:
            rinex_files = self._get_rinex_files(data_dir, reader_format)
            if not rinex_files:
                self._logger.warning(
                    "no_rinex_files_found",
                    receiver=receiver_name,
                    data_dir=str(data_dir),
                    reader_format=reader_format,
                )
                continue

            position_mode = self._config.processing.processing.receiver_position_mode
            if position_mode == "per_receiver":
                position_files = rinex_files
                self._logger.warning(
                    "receiver_position_mode='per_receiver': using %s's own "
                    "position (breaks direct SNR comparability)",
                    receiver_name,
                )
            else:
                position_files = (
                    self._get_rinex_files(position_data_dir, reader_format)
                    if position_data_dir
                    else rinex_files
                )
            t_pos_start = time.perf_counter()
            receiver_position = self._compute_receiver_position(
                position_files, receiver_name, reader_format=reader_format
            )
            t_pos_end = time.perf_counter()
            self._logger.info(
                "position_computed",
                receiver=receiver_name,
                position_seconds=round(t_pos_end - t_pos_start, 4),
                success=receiver_position is not None,
            )
            if receiver_position is None:
                continue

            receiver_file_map.append((receiver_name, rinex_files))

            effective_reader = reader_format or self._reader_name
            for rnx_file in rinex_files:
                task_descriptors.append(
                    (
                        rnx_file,
                        keep_vars,
                        aux_zarr_path,
                        receiver_position,
                        receiver_name,
                        self.keep_sids,
                        effective_reader,
                        self.use_sbf_geometry,
                    )
                )

        t_batch_end = time.perf_counter()
        self._logger.info(
            "prepare_batch_tasks_complete",
            date=date_str,
            total_seconds=round(t_batch_end - t_batch_start, 2),
            aux_seconds=round(t_aux_done - t_batch_start, 2),
            receivers_prepared=len(receiver_file_map),
            total_tasks=len(task_descriptors),
        )
        return task_descriptors, receiver_file_map

    def parsed_rinex_data_gen(
        self,
        keep_vars: list[str] | None = None,
        receiver_configs: list[tuple[str, str, Path]]
        | list[tuple[str, str, Path, Path | None]]
        | list[tuple[str, str, Path, Path | None, str]]
        | None = None,
    ) -> Generator[tuple[str, xr.Dataset, float]]:
        """Generate datasets from RINEX files and append to Icechunk stores.

        Pipeline:
        1. Preprocess aux data ONCE per day with Hermite splines → Zarr
        2. For each receiver:
        a. Compute receiver position (from own files or position_data_dir)
        b. Parallel process RINEX files via Dask distributed (or ProcessPoolExecutor fallback)
        c. Append to Icechunk store with receiver_name as group
        d. Yield final daily dataset

        Parameters
        ----------
        keep_vars : list[str], optional
            Variables to keep in datasets (default: from globals)
        receiver_configs : list[tuple], optional
            List of (receiver_name, receiver_type, data_dir),
            (receiver_name, receiver_type, data_dir, position_data_dir), or
            (receiver_name, receiver_type, data_dir, position_data_dir, reader_format)
            tuples.
            When position_data_dir is provided, the receiver position is
            computed from files in that directory instead of data_dir.
            If None, uses default behavior with matched_data_dirs.

        Yields
        ------
        xr.Dataset
            Processed and augmented daily dataset for each receiver

        """
        if receiver_configs is None:
            receiver_configs = self._get_default_receiver_configs()

        # Normalize to 5-tuples
        normalized_configs: list[tuple[str, str, Path, Path | None, str]] = []
        for cfg in receiver_configs:
            if len(cfg) == 3:
                normalized_configs.append((*cfg, None, self._reader_name))
            elif len(cfg) == 4:
                normalized_configs.append((*cfg, self._reader_name))
            else:
                normalized_configs.append(cfg)

        if keep_vars is None:
            keep_vars = load_config().processing.processing.keep_rnx_vars

        pipeline_start = time.perf_counter()
        self._logger.info(
            "rinex_pipeline_started",
            receivers=len(normalized_configs),
            date=self.matched_data_dirs.yyyydoy.to_str(),
            keep_vars=keep_vars,
        )

        # ====================================================================
        # STEP 1: Preprocess aux data ONCE per day with Hermite splines
        # ====================================================================
        # Get first receiver files to infer sampling rate
        first_receiver_name, _first_receiver_type, first_data_dir, _, first_fmt = (
            normalized_configs[0]
        )
        first_files = self._get_rinex_files(first_data_dir, first_fmt)

        if not first_files:
            msg = (
                f"No RINEX files found for {first_receiver_name} - "
                "cannot infer sampling rate"
            )
            self._logger.error(
                "pipeline_failed",
                reason="no_rinex_files",
                receiver=first_receiver_name,
            )
            raise ValueError(msg)

        date_str = self.matched_data_dirs.yyyydoy.to_str()
        aux_zarr_path = self._ensure_aux_data_preprocessed(
            first_files, date_str, reader_format=first_fmt
        )

        # ====================================================================
        # STEP 2: Process each receiver, reusing RINEX parsing for
        #         reference variants that share the same data_dir
        # ====================================================================
        # Cache: data_dir -> (augmented_datasets, rinex_files)
        # When multiple store groups share one data_dir (reference variants
        # with different canopy positions), we parse RINEX once and recompute
        # only the SCS (theta, phi, r) for each position.
        _rinex_cache: dict[Path, tuple[list[tuple[Path, xr.Dataset]], list[Path]]] = {}

        for (
            receiver_name,
            receiver_type,
            data_dir,
            position_data_dir,
            reader_format,
        ) in normalized_configs:
            t_rcv_start = time.perf_counter()

            self._logger.info(
                "receiver_processing_started",
                receiver=receiver_name,
                receiver_type=receiver_type,
                data_dir=str(data_dir),
                position_from=str(position_data_dir) if position_data_dir else "self",
                reader_format=reader_format,
            )

            # Get GNSS files for this receiver (filtered by reader_format)
            rinex_files = self._get_rinex_files(data_dir, reader_format)
            if not rinex_files:
                self._logger.warning(
                    "no_rinex_files_found",
                    receiver=receiver_name,
                    data_dir=str(data_dir),
                    reader_format=reader_format,
                )
                continue

            # Compute receiver position
            t_pos_start = time.perf_counter()
            position_mode = self._config.processing.processing.receiver_position_mode
            if position_mode == "per_receiver":
                position_files = rinex_files
                self._logger.warning(
                    "receiver_position_mode='per_receiver': using %s's own "
                    "position (breaks direct SNR comparability)",
                    receiver_name,
                )
            else:
                position_files = (
                    self._get_rinex_files(position_data_dir, reader_format)
                    if position_data_dir
                    else rinex_files
                )
            receiver_position = self._compute_receiver_position(
                position_files, receiver_name, reader_format=reader_format
            )
            t_pos_end = time.perf_counter()
            if receiver_position is None:
                continue

            self._logger.info(
                "receiver_position_computed",
                receiver=receiver_name,
                duration_seconds=round(t_pos_end - t_pos_start, 2),
            )

            t_rinex_start = time.perf_counter()
            if data_dir not in _rinex_cache:
                # First time seeing this data_dir — full parallel processing
                self._logger.info(
                    "rinex_files_discovered",
                    receiver=receiver_name,
                    files=len(rinex_files),
                )

                augmented_datasets, aux_datasets, sid_issues = (
                    self._parallel_process_rinex(
                        rinex_files=rinex_files,
                        keep_vars=keep_vars,
                        aux_zarr_path=aux_zarr_path,
                        receiver_position=receiver_position,
                        receiver_type=receiver_name,
                        reader_format=reader_format,
                    )
                )

                # Cache obs datasets for reuse by other store groups with the same data_dir
                # (aux_datasets are written immediately; they don't need to be cached)
                _rinex_cache[data_dir] = (augmented_datasets, rinex_files)
            else:
                # Reuse cached RINEX data, only recompute SCS with new position
                cached_datasets, rinex_files = _rinex_cache[data_dir]
                aux_datasets = None
                sid_issues = None

                self._logger.info(
                    "recomputing_scs_from_cache",
                    receiver=receiver_name,
                    cached_files=len(cached_datasets),
                    new_position=str(receiver_position),
                )

                augmented_datasets = []
                for fpath, ds in cached_datasets:
                    # Drop old SCS vars and recompute with new position
                    scs_vars = [v for v in ("theta", "phi", "r") if v in ds.data_vars]
                    ds_no_scs = ds.drop_vars(scs_vars)

                    # Recompute SCS using the aux data
                    aux_store = xr.open_zarr(aux_zarr_path, decode_timedelta=True)
                    aux_slice = aux_store.sel(epoch=ds_no_scs.epoch, method="nearest")
                    common_sids = sorted(
                        set(ds_no_scs.sid.values) & set(aux_slice.sid.values)
                    )
                    aux_slice = aux_slice.sel(sid=common_sids)

                    ds_recomputed = _compute_spherical_coords_fast(
                        ds_no_scs, aux_slice, receiver_position
                    )
                    augmented_datasets.append((fpath, ds_recomputed))

            t_rinex_end = time.perf_counter()
            self._logger.info(
                "rinex_parallel_processing_complete",
                receiver=receiver_name,
                duration_seconds=round(t_rinex_end - t_rinex_start, 2),
                datasets=len(augmented_datasets),
            )

            # Append to Icechunk with receiver_name as group
            t_write_start = time.perf_counter()
            self._append_to_icechunk(
                augmented_datasets=augmented_datasets,
                receiver_name=receiver_name,
                rinex_files=rinex_files,
                aux_datasets=aux_datasets,
                sid_issues=sid_issues,
            )
            t_write_end = time.perf_counter()

            # Yield final daily dataset
            t_read_start = time.perf_counter()
            date_obj = self.matched_data_dirs.yyyydoy.date
            start_time = datetime.combine(date_obj, datetime.min.time())
            end_time = datetime.combine(date_obj, datetime.max.time())
            time_range = (start_time, end_time)

            daily_dataset = self.site.read_receiver_data(
                receiver_name=receiver_name, time_range=time_range
            )
            t_read_end = time.perf_counter()

            t_rcv_end = time.perf_counter()
            receiver_duration = t_rcv_end - t_rcv_start

            self._logger.info(
                "receiver_processing_complete",
                receiver=receiver_name,
                total_seconds=round(receiver_duration, 2),
                position_seconds=round(t_pos_end - t_pos_start, 2),
                rinex_parallel_seconds=round(t_rinex_end - t_rinex_start, 2),
                icechunk_write_seconds=round(t_write_end - t_write_start, 2),
                store_readback_seconds=round(t_read_end - t_read_start, 2),
                dataset_size=dict(daily_dataset.sizes),
                epochs=len(daily_dataset.epoch) if "epoch" in daily_dataset.dims else 0,
                sids=len(daily_dataset.sid) if "sid" in daily_dataset.dims else 0,
            )

            yield receiver_name, daily_dataset, receiver_duration

        pipeline_duration = time.perf_counter() - pipeline_start
        self._logger.info(
            "rinex_pipeline_complete",
            duration_seconds=round(pipeline_duration, 2),
            receivers=len(normalized_configs),
        )

    def _get_default_receiver_configs(
        self,
    ) -> list[tuple[str, str, Path, Path | None]]:
        """Get default receiver configs from matched_data_dirs.

        Returns a list of (store_group_name, receiver_type, data_dir,
        position_data_dir) tuples. For canopy receivers, position_data_dir
        is None (use own files). For reference receivers, one entry is
        created per canopy in scs_from, with position_data_dir pointing
        to the canopy's RINEX directory.

        Returns
        -------
        list[tuple[str, str, Path, Path | None]]
            Receiver processing configurations.
        """
        configs: list[tuple[str, str, Path, Path | None]] = []
        site_config = self.site._site_config

        # Collect canopy data dirs for resolving position sources
        canopy_data_dirs: dict[str, Path] = {}
        base_path = site_config.get_base_path()

        for name, cfg in site_config.receivers.items():
            if cfg.type == "canopy":
                canopy_data_dirs[name] = (
                    base_path / cfg.directory / self.matched_data_dirs.yyyydoy.yydoy
                )

        # Add all canopy receivers (each uses own position)
        for name, cfg in site_config.receivers.items():
            if cfg.type == "canopy" and name in canopy_data_dirs:
                configs.append((name, "canopy", canopy_data_dirs[name], None))

        # Add reference receivers — one entry per canopy in scs_from
        for name, cfg in site_config.receivers.items():
            if cfg.type != "reference":
                continue
            ref_data_dir = (
                base_path / cfg.directory / self.matched_data_dirs.yyyydoy.yydoy
            )
            canopy_names = site_config.resolve_scs_from(name)
            for canopy_name in canopy_names:
                store_group = f"{name}_{canopy_name}"
                position_dir = canopy_data_dirs.get(canopy_name)
                configs.append((store_group, "reference", ref_data_dir, position_dir))

        return configs

    def should_skip_day(
        self,
        receiver_types: list[str] | None = None,
        completeness_threshold: float = 1,
    ) -> tuple[bool, dict]:
        """Check if this day should be skipped based on existing data coverage.

        Parameters
        ----------
        receiver_types : list[str], optional
            Receiver types to check. Defaults to ['canopy', 'reference']
        completeness_threshold : float
            Fraction of expected epochs (default 0.95 = 95%)

        Returns
        -------
        tuple[bool, dict]
            (should_skip, coverage_info) where coverage_info contains details
            per receiver.

        """
        if receiver_types is None:
            receiver_types = ["canopy", "reference"]

        # Expected epochs for 24h at 30s sampling
        expected_epochs = int(24 * 3600 / 30)  # 2880
        required_epochs = int(expected_epochs * completeness_threshold)

        # Get datetime objects from YYYYDOY.date
        yyyydoy_date = self.matched_data_dirs.yyyydoy.date
        day_start = np.datetime64(
            datetime.combine(yyyydoy_date, dt_time.min),
            "ns",
        )
        day_end = np.datetime64(
            datetime.combine(yyyydoy_date, dt_time.max),
            "ns",
        )

        coverage_info = {}

        for receiver_type in receiver_types:
            # Get receiver name
            receiver_name = None
            for name, config in self.site.active_receivers.items():
                if config.get("type") == receiver_type:
                    receiver_name = name
                    break

            if not receiver_name:
                coverage_info[receiver_type] = {
                    "exists": False,
                    "reason": "No receiver configured",
                }
                return False, coverage_info

            try:
                # Read metadata table
                with self.site.rinex_store.readonly_session("main") as session:
                    zmeta = zarr.open_group(session.store, mode="r")[
                        f"{receiver_name}/metadata/table"
                    ]
                    data = {col: zmeta[col][:] for col in zmeta.array_keys()}
                    df = pl.DataFrame(data)

                # Cast datetime columns
                df = df.with_columns(
                    [
                        pl.col("start").cast(pl.Datetime("ns")),
                        pl.col("end").cast(pl.Datetime("ns")),
                    ]
                )

                # Filter to this day
                day_rows = df.filter(
                    (pl.col("start") >= day_start) & (pl.col("end") <= day_end)
                )

                if day_rows.is_empty():
                    coverage_info[receiver_type] = {
                        "exists": False,
                        "epochs": 0,
                        "expected": expected_epochs,
                        "percent": 0.0,
                    }
                    return False, coverage_info

                # Calculate total epochs
                day_rows = day_rows.with_columns(
                    [
                        (
                            (pl.col("end") - pl.col("start")).dt.total_seconds() / 30
                        ).alias("n_epochs")
                    ]
                )

                total_epochs = int(day_rows["n_epochs"].sum())
                percent = total_epochs / expected_epochs * 100

                coverage_info[receiver_type] = {
                    "exists": True,
                    "epochs": total_epochs,
                    "expected": expected_epochs,
                    "percent": percent,
                    "complete": total_epochs >= required_epochs,
                }

                if total_epochs < required_epochs:
                    return False, coverage_info

            except (KeyError, OSError, RuntimeError, ValueError) as e:
                coverage_info[receiver_type] = {
                    "exists": False,
                    "reason": str(e),
                    "epochs": 0,
                    "expected": expected_epochs,
                    "percent": 0.0,
                }
                return False, coverage_info

        # All receivers are complete
        return True, coverage_info

    def __repr__(self) -> str:
        return (
            "RinexDataProcessor("
            f"date={self.matched_data_dirs.yyyydoy.to_str()}, "
            f"site={self.site.site_name}, "
            f"aux_pipeline={self.aux_pipeline})"
        )


class DistributedRinexDataProcessor(RinexDataProcessor):
    """Under development. Use with caution.

    In `MyIcechunkStore`, attrs `MyIcechunkStore.compression_algorithm` and
    `MyIcechunkStore.config` must be disabled, so that any instance becomes
    serializable.

    Subclass of RinexDataProcessor that uses cooperative distributed writing.

    See:
        https://icechunk.io/en/latest/parallel/#cooperative-distributed-writes

    """

    def __init__(
        self,
        matched_data_dirs: MatchedDirs,
        site: GnssResearchSite,
        aux_file_path: Path | None = None,
        n_max_workers: int = 12,
    ) -> None:
        super().__init__(matched_data_dirs, site, aux_file_path, n_max_workers)

    def __repr__(self) -> str:
        return (
            "DistributedRinexDataProcessor("
            f"date={self.matched_data_dirs.yyyydoy.to_str()}, "
            f"site={self.site.site_name}, "
            f"aux_pipeline={self.aux_pipeline})"
        )

    def _cooperative_distributed_writing(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
        receiver_name: str,
    ) -> list[Path]:
        version = get_version_from_pyproject()
        repo = self.site.rinex_store.repo
        rinex_files_sorted = sorted(rinex_files, key=lambda p: p.name)

        # STEP 1: Initialize dataset structure with ALL files' time coordinates
        # This creates the full epoch dimension upfront
        session = repo.writable_session("main")

        # Collect all epochs from all files (or create empty structure)
        # Option A: Process all files first to get full time range
        all_epochs = []
        for rinex_file in rinex_files_sorted:
            _fname, ds, _aux, _sids = preprocess_with_hermite_aux(
                rinex_file,
                keep_vars,
                aux_zarr_path,
                receiver_position,
                receiver_type,
                self.keep_sids,
                self._reader_name,
            )
            all_epochs.extend(ds.epoch.values)

        # Create empty dataset with full structure
        _first_fname, first_ds, _aux, _sids = preprocess_with_hermite_aux(
            rinex_files_sorted[0],
            keep_vars,
            aux_zarr_path,
            receiver_position,
            receiver_type,
            self.keep_sids,
            self._reader_name,
        )

        # Initialize with full epoch dimension
        empty_ds = first_ds.isel(epoch=[]).expand_dims({"epoch": len(all_epochs)})
        empty_ds = empty_ds.assign_coords({"epoch": np.sort(all_epochs)})

        to_icechunk(empty_ds, session, group=receiver_name, mode="w")
        session.commit(f"Initialize {receiver_name} structure")

        # STEP 2: Now do cooperative distributed writes
        session = repo.writable_session("main")
        fork = session.fork()  # ONE fork

        remote_sessions = []

        if self._dask_client is not None and _HAS_DISTRIBUTED:
            client = self._dask_client
            self._logger.info(
                "cooperative_writing_started",
                executor_type="dask.distributed",
                files=len(rinex_files_sorted),
            )
            futures = [
                client.submit(
                    worker_task_with_region_auto,
                    rinex_file,
                    keep_vars,
                    aux_zarr_path,
                    receiver_position,
                    receiver_type,
                    receiver_name,
                    fork,
                    self.keep_sids,
                    self._reader_name,
                    pure=False,
                )
                for rinex_file in rinex_files_sorted
            ]

            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            desc = f"{yyyydoy} Writing {receiver_name}"
            with _processing_progress() as progress:
                task = progress.add_task(desc, total=len(futures))
                for fut in dask_as_completed(futures):
                    returned_fork = fut.result()
                    remote_sessions.append(returned_fork)
                    progress.advance(task)
        else:
            self._logger.info(
                "cooperative_writing_started",
                executor_type="ProcessPoolExecutor",
                files=len(rinex_files_sorted),
            )
            with ProcessPoolExecutor(max_workers=self.n_max_workers) as ex:
                futures = [
                    ex.submit(
                        worker_task_with_region_auto,
                        rinex_file,
                        keep_vars,
                        aux_zarr_path,
                        receiver_position,
                        receiver_type,
                        receiver_name,
                        fork,
                        self.keep_sids,
                    )
                    for rinex_file in rinex_files_sorted
                ]

                yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
                desc = f"{yyyydoy} Writing {receiver_name}"
                with _processing_progress() as progress:
                    task = progress.add_task(desc, total=len(futures))
                    for fut in as_completed(futures):
                        returned_fork = fut.result()
                        remote_sessions.append(returned_fork)
                        progress.advance(task)

        # Merge all remote sessions
        session.merge(*remote_sessions)
        _snapshot_id = session.commit(
            f"[v{version}] Cooperative write for {receiver_name}"
        )

        return [f.name for f in rinex_files_sorted]

    def _append_to_icechunk_native_context_manager(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
    ) -> None:
        _ = rinex_files
        log = self._logger
        version = get_version_from_pyproject()

        # 1) Pre-check which hashes already exist
        file_hash_map = {
            fname: ds.attrs.get("File Hash") for fname, ds in augmented_datasets
        }
        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )

        actions = {
            "initial": 0,
            "skipped": 0,
            "appended": 0,
            "written": 0,
            "overwritten": 0,
        }
        metadata_records: list[dict] = []

        # 2) Open native Icechunk transaction (auto-commit)
        commit_msg = f"[v{version}] {receiver_name} {self.matched_data_dirs.yyyydoy}"

        with self.site.rinex_store.repo.transaction(
            branch="main", message=commit_msg
        ) as store:
            groups = self.site.rinex_store.list_groups() or []

            # 2a) Synchronous initial write if group does not exist (avoid race)
            if receiver_name not in groups:
                for fname, ds in augmented_datasets:
                    rinex_hash = file_hash_map.get(fname)
                    if rinex_hash and rinex_hash not in existing_hashes:
                        ds_init = self.site.rinex_store._cleanse_dataset_attrs(
                            ds,
                        )
                        ds_init.to_zarr(store, group=receiver_name, mode="a")

                        actions["initial"] += 1
                        groups.append(receiver_name)
                        break

            # 3) Prepare metadata rows and thread tasks
            def write_one(
                _fname: Path,
                ds: xr.Dataset,
                exists: bool,
                _rel_path: str,
                receiver_name: str,
                store: zarr.storage.BaseStore,
            ) -> str:
                ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                    ds,
                )

                if not exists and receiver_name not in groups:
                    ds_clean.to_zarr(store, group=receiver_name, mode="w")
                    return "initial"
                if exists and self._rinex_store_strategy == "skip":
                    return "skipped"
                if exists and self._rinex_store_strategy == "overwrite":
                    ds_clean.to_zarr(
                        store, group=receiver_name, mode="a", append_dim="epoch"
                    )
                    return "overwritten"
                if exists and self._rinex_store_strategy == "append":
                    ds_clean.to_zarr(
                        store, group=receiver_name, mode="a", append_dim="epoch"
                    )
                    return "appended"
                ds_clean.to_zarr(
                    store, group=receiver_name, mode="a", append_dim="epoch"
                )
                return "written"

            with ThreadPoolExecutor(max_workers=12) as pool:
                futures = []
                for fname, ds in augmented_datasets:
                    rinex_hash = file_hash_map.get(fname)
                    if not rinex_hash:
                        continue

                    exists = rinex_hash in existing_hashes
                    start_epoch = np.datetime64(ds.epoch.min().values)
                    end_epoch = np.datetime64(ds.epoch.max().values)
                    rel_path = self.site.rinex_store.rel_path_for_commit(fname)

                    # full-schema metadata row (snapshot_id can stay None)
                    metadata_records.append(
                        {
                            "hash": rinex_hash,
                            "start": start_epoch,
                            "end": end_epoch,
                            "action": "skip" if exists else "write",
                            "commit_msg": f"{'skip' if exists else 'write'}: {rel_path}",
                            "written_at": datetime.now(UTC).isoformat(),
                            "attrs": json.dumps(ds.attrs),
                            "snapshot_id": None,
                            "write_strategy": "skip" if exists else "append",
                        }
                    )

                    # skip writing if exists & skip strategy
                    if exists and self._rinex_store_strategy == "skip":
                        actions["skipped"] += 1
                        continue

                    # IMPORTANT: pass store explicitly; do NOT close over outer name
                    futures.append(
                        pool.submit(
                            write_one, fname, ds, exists, rel_path, receiver_name, store
                        )
                    )

                for fut in as_completed(futures):
                    result = fut.result()
                    actions[result] += 1

            # 4) Bulk metadata into SAME transaction
            self.site.rinex_store.append_metadata_bulk_store(
                receiver_name, metadata_records, store
            )

        # 5) committed on exit
        log.info("Committed: %s", actions)

    def _append_to_icechunk_coord_distrbtd(
        self,
        augmented_datasets: list[tuple[Path, xr.Dataset]],
        receiver_name: str,
        rinex_files: list[Path],
    ) -> None:
        """Cooperative distributed append with Icechunk.

        - Uses cooperative_transaction so multiple workers can contribute.
        - True parallel writes via Dask distributed (or ProcessPoolExecutor fallback).
        - Produces a single commit at the end.
        """
        _ = rinex_files
        log = self._logger
        version = get_version_from_pyproject()

        t_start = time.time()

        # STEP 1: Batch check which files exist
        log.info("Batch checking %s files...", len(augmented_datasets))
        t1 = time.time()

        file_hash_map = {
            fname: ds.attrs.get("File Hash") for fname, ds in augmented_datasets
        }

        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )

        t2 = time.time()
        log.info(
            "Batch check complete in %.2fs: %s/%s existing",
            t2 - t1,
            len(existing_hashes),
            len(augmented_datasets),
        )

        # STEP 2: Branch-stage-promote for overwrite; direct main for others
        is_overwrite = self._rinex_store_strategy == "overwrite"
        temp_branch = None

        if is_overwrite:
            yyyydoy = self.matched_data_dirs.yyyydoy.to_str()
            temp_branch = f"overwrite_{receiver_name}_{yyyydoy}"
            current_snapshot = next(
                self.site.rinex_store.repo.ancestry(branch="main")
            ).id
            try:
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            except Exception:
                self.site.rinex_store.repo.delete_branch(temp_branch)
                self.site.rinex_store.repo.create_branch(temp_branch, current_snapshot)
            log.info(
                "Created temp branch '%s' for overwrite (snapshot: %s...)",
                temp_branch,
                current_snapshot[:8],
            )
            branch = temp_branch
        else:
            branch = "main"

        log.info("Opening Icechunk session...")
        t3 = time.time()
        session = self.site.rinex_store.repo.writable_session(branch=branch)
        groups = self.site.rinex_store.list_groups() or []
        t4 = time.time()
        log.info("Session opened in %.2fs", t4 - t3)

        # Prepare store for overwrite (remove old epochs, drop stale vars)
        if is_overwrite and receiver_name in groups:
            self._prepare_store_for_overwrite(
                session,
                receiver_name,
                augmented_datasets,
                existing_hashes,
                file_hash_map,
            )

        actions = {
            "initial": 0,
            "skipped": 0,
            "appended": 0,
            "written": 0,
            "overwritten": 0,
        }
        metadata_records = []  # Collect metadata to write after commit

        try:
            # STEP 3: Process all datasets using ONLY to_icechunk()
            log.info(
                "Processing %s datasets...",
                len(augmented_datasets),
            )
            t5 = time.time()

            for idx, (fname, ds) in enumerate(augmented_datasets):
                # Progress logging
                if idx % 20 == 0 and idx > 0:
                    elapsed = time.time() - t5
                    rate = idx / elapsed if elapsed > 0 else 0
                    log.info(
                        "  Progress: %s/%s (%.1f files/s)",
                        idx,
                        len(augmented_datasets),
                        rate,
                    )

                try:
                    rel_path = self.site.rinex_store.rel_path_for_commit(fname)
                    rinex_hash = file_hash_map[fname]

                    if not rinex_hash:
                        log.debug("No hash for %s, skipping", fname)
                        continue

                    # Get time range for metadata
                    start_epoch = np.datetime64(ds.epoch.min().values)
                    end_epoch = np.datetime64(ds.epoch.max().values)

                    # Fast hash check
                    exists = rinex_hash in existing_hashes

                    # Cleanse dataset
                    ds_clean = self.site.rinex_store._cleanse_dataset_attrs(
                        ds,
                    )

                    # Collect metadata for ALL files (write later)
                    metadata_records.append(
                        {
                            "fname": fname,
                            "rinex_hash": rinex_hash,
                            "start": start_epoch,
                            "end": end_epoch,
                            "dataset_attrs": ds.attrs.copy(),
                            "exists": exists,
                            "rel_path": rel_path,
                        }
                    )

                    # Handle data writes using ONLY to_icechunk() with our session
                    match (exists, self._rinex_store_strategy):
                        case (False, _) if receiver_name not in groups and idx == 0:
                            # Initial group creation
                            to_icechunk(ds_clean, session, group=receiver_name)
                            groups.append(receiver_name)
                            actions["initial"] += 1
                            log.debug("Initial: %s", rel_path)

                        case (True, "skip"):
                            # File exists, skip writing data
                            actions["skipped"] += 1
                            log.debug("Skipped: %s", rel_path)

                        case (True, "append"):
                            # File exists but append anyway
                            to_icechunk(
                                ds_clean,
                                session,
                                group=receiver_name,
                                append_dim="epoch",
                            )
                            actions["appended"] += 1
                            log.debug("Appended: %s", rel_path)

                        case (False, _):
                            # New file, write it
                            to_icechunk(
                                ds_clean,
                                session,
                                group=receiver_name,
                                append_dim="epoch",
                            )
                            actions["written"] += 1
                            log.debug("Wrote: %s", rel_path)

                        case (True, "overwrite"):
                            # Old data already removed by _prepare_store_for_overwrite
                            to_icechunk(
                                ds_clean,
                                session,
                                group=receiver_name,
                                append_dim="epoch",
                            )
                            actions["overwritten"] += 1
                            log.debug("Overwrote: %s", rel_path)

                        case _:
                            log.warning(
                                "Unhandled strategy: exists=%s, strategy=%s for %s",
                                exists,
                                self._rinex_store_strategy,
                                rel_path,
                            )

                except (OSError, RuntimeError, ValueError):
                    log.exception("Failed to process %s", fname.name)

            t6 = time.time()
            log.info("Dataset processing complete in %.2fs", t6 - t5)

            # STEP 4: Single commit for all data
            summary = ", ".join(f"{k}={v}" for k, v in actions.items() if v > 0)
            commit_msg = (
                f"[v{version}] {receiver_name} "
                f"{self.matched_data_dirs.yyyydoy}: {summary}"
            )

            # STEP 5: Write metadata (separate transactions after data commit)
            log.info(
                "Writing metadata for %s files...",
                len(metadata_records),
            )
            t9 = time.time()

            self.site.rinex_store.append_metadata_bulk(
                group_name=receiver_name,
                rows=metadata_records,
                session=session,
            )

            t10 = time.time()
            log.info("Metadata written in %.2fs", t10 - t9)

            log.info("Committing: %s", summary)
            t7 = time.time()
            try:
                snapshot_id = session.commit(commit_msg)
                t8 = time.time()
                log.info(
                    "Commit complete in %.2fs (snapshot: %s...)",
                    t8 - t7,
                    snapshot_id[:8],
                )
            except Exception as e:
                t8 = time.time()
                if "no changes" in str(e).lower():
                    log.info(
                        "no_changes_to_commit (all files already ingested): %.2fs",
                        t8 - t7,
                    )
                else:
                    raise

            # Timing summary
            t_end = time.time()
            log.info("\nTIMING BREAKDOWN:")
            log.info("  Batch check:    %.2fs", t2 - t1)
            log.info("  Open session:   %.2fs", t4 - t3)
            log.info("  Process data:   %.2fs", t6 - t5)
            log.info("  Commit:         %.2fs", t8 - t7)
            log.info("  Metadata:       %.2fs", t10 - t9)
            log.info("  TOTAL:          %.2fs", t_end - t_start)

            log.info(
                "Successfully processed %s files for '%s'",
                len(augmented_datasets),
                receiver_name,
            )

        except (OSError, RuntimeError, ValueError):
            log.exception("Batch append failed")
            raise

        # Promote temp branch to main after successful commit
        if is_overwrite and temp_branch:
            try:
                new_tip = next(
                    self.site.rinex_store.repo.ancestry(branch=temp_branch)
                ).id
                self.site.rinex_store.repo.reset_branch("main", new_tip)
                log.info(
                    "Promoted %s to main (snapshot: %s...)",
                    temp_branch,
                    new_tip[:8],
                )
            finally:
                with contextlib.suppress(Exception):
                    self.site.rinex_store.repo.delete_branch(temp_branch)

    def parsed_rinex_data_gen_parallel(
        self,
        keep_vars: list[str] | None = None,
        receiver_types: list[str] | None = None,
    ) -> Generator[xr.Dataset]:
        """Generate datasets from RINEX files and append to Icechunk stores.

        Pipeline:
        1. Preprocess aux data ONCE per day with Hermite splines → Zarr
        2. Compute receiver position ONCE (shared for all receivers)
        3. For each receiver type (canopy, reference):
           a. Get list of RINEX files
           b. Parallel process via Dask distributed (or ProcessPoolExecutor fallback)
           c. Each worker: read RINEX + slice Zarr + compute φ, θ, r
           d. Sequential append to Icechunk store
           e. Yield final daily dataset

        Parameters
        ----------
        keep_vars : List[str], optional
            Variables to keep in datasets (default: from globals)
        receiver_types : List[str], optional
            Receiver types to process (default: ['canopy', 'reference'])

        Yields
        ------
        xr.Dataset
            Processed and augmented daily dataset for each receiver type

        """
        if receiver_types is None:
            receiver_types = ["canopy", "reference"]

        if keep_vars is None:
            keep_vars = load_config().processing.processing.keep_rnx_vars

        self._logger.info(
            "Starting RINEX processing pipeline for: %s",
            receiver_types,
        )

        # Pre-flight: Get canopy files to infer sampling and compute position
        canopy_dir = self.matched_data_dirs.canopy_data_dir
        canopy_files = self._get_rinex_files(canopy_dir)
        if not canopy_files:
            msg = "No canopy RINEX files found - cannot infer sampling rate"
            raise ValueError(msg)

        # ====================================================================
        # STEP 1: Preprocess aux data ONCE per day with Hermite splines
        # ====================================================================
        import shutil as _shutil

        _aux_base_dir = load_config().processing.storage.get_aux_data_dir()
        aux_zarr_path = _aux_base_dir / (
            f"aux_{self.matched_data_dirs.yyyydoy.to_str()}.zarr"
        )

        # Always reprocess from raw SP3/CLK files to avoid stale SID caches
        if aux_zarr_path.exists():
            _shutil.rmtree(aux_zarr_path)

        self._logger.info("Preprocessing aux data with Hermite splines (once per day)")
        _sampling_interval = self._preprocess_aux_data_with_hermite(
            canopy_files, aux_zarr_path
        )

        # ====================================================================
        # STEP 2: Compute receiver position
        # ====================================================================
        position_mode = self._config.processing.processing.receiver_position_mode
        first_rnx = self._make_reader(canopy_files[0])
        first_ds = first_rnx.to_ds(keep_data_vars=[], write_global_attrs=True)
        shared_position = ECEFPosition.from_ds_metadata(first_ds)

        if position_mode == "per_receiver":
            self._logger.warning(
                "receiver_position_mode='per_receiver': each receiver will use "
                "its own RINEX header position. This breaks direct SNR "
                "comparability between receivers."
            )
        else:
            self._logger.info(
                "Computed receiver position (shared): %s",
                shared_position,
            )

        # ====================================================================
        # STEP 3: Process each receiver type
        # ====================================================================
        for receiver_type in receiver_types:
            self._logger.info("Processing receiver type: %s", receiver_type)

            # 3a. Resolve directories and receiver name
            rinex_dir, receiver_name = self._resolve_receiver_paths(receiver_type)

            if not receiver_name:
                self._logger.warning(
                    "No configured receiver for %s, skipping",
                    receiver_type,
                )
                continue

            # 3b. Get RINEX files for this receiver type
            rinex_files = self._get_rinex_files(rinex_dir)
            if not rinex_files:
                self._logger.warning(
                    "No RINEX files found in %s",
                    rinex_dir,
                )
                continue

            self._logger.info(
                "Found %s RINEX files to process",
                len(rinex_files),
            )

            # 3b'. Determine receiver position for this receiver
            if position_mode == "per_receiver":
                receiver_position = self._compute_receiver_position(
                    rinex_files, receiver_name
                )
                if receiver_position is None:
                    self._logger.error(
                        "Could not compute position for %s, skipping",
                        receiver_name,
                    )
                    continue
            else:
                receiver_position = shared_position

            # 3c. Parallel process via Dask (or ProcessPoolExecutor fallback)
            _ = self._cooperative_distributed_writing(
                rinex_files=rinex_files,
                keep_vars=keep_vars,
                aux_zarr_path=aux_zarr_path,
                receiver_position=receiver_position,
                receiver_type=receiver_type,
                receiver_name=receiver_name,
            )

            # 3e. Yield final daily dataset
            # Read back from store to get complete daily dataset
            date_obj = self.matched_data_dirs.yyyydoy.date
            start_time = datetime.combine(date_obj, datetime.min.time())
            end_time = datetime.combine(date_obj, datetime.max.time())
            time_range = (start_time, end_time)

            daily_dataset = self.site.read_receiver_data(
                receiver_name=receiver_name, time_range=time_range
            )

            self._logger.info(
                "Yielding daily dataset for %s ('%s'): %s",
                receiver_type,
                receiver_name,
                dict(daily_dataset.sizes),
            )

            yield daily_dataset


if __name__ == "__main__":
    print(f"stared main block at {datetime.now(UTC)}")

    matcher = DataDirMatcher(
        sky_dir_pattern=Path("01_reference/01_GNSS/01_raw"),
        canopy_dir_pattern=Path("02_canopy/01_GNSS/01_raw"),
    )

    site = GnssResearchSite(site_name="Rosalia")

    stats = {"processed": 0, "skipped": 0, "failed": 0}

    for md in matcher:
        yyyydoy_str = md.yyyydoy.to_str()

        if yyyydoy_str != "2024258":
            continue

        try:
            print(f"instantiating processor for {yyyydoy_str}: {datetime.now(UTC)}")
            # Create processor first to check completeness
            processor = RinexDataProcessor(
                matched_data_dirs=md, site=site, n_max_workers=12
            )

            # Check if should skip
            if processor._rinex_store_strategy in ["skip", "append"]:
                should_skip, coverage = processor.should_skip_day()

                if should_skip:
                    print(f"✓ Skipping {yyyydoy_str} - already complete:")
                    for receiver_type, info in coverage.items():
                        print(
                            f"  {receiver_type}: {info['epochs']}/"
                            f"{info['expected']} ({info['percent']:.1f}%)"
                        )
                    stats["skipped"] += 1
                    continue
                else:
                    print(f"⚠ Processing {yyyydoy_str} - incomplete coverage:")
                    for receiver_type, info in coverage.items():
                        if info["exists"]:
                            print(
                                f"  {receiver_type}: {info['epochs']}/"
                                f"{info['expected']} ({info['percent']:.1f}%)"
                            )
                        else:
                            print(f"  {receiver_type}: No data")

            # Process data
            print(
                f"about to call parsed_rinex_data_gen for {yyyydoy_str}: "
                f"{datetime.now(UTC)}"
            )
            data_generator = processor.parsed_rinex_data_gen()
            print(f"calling next for canopy: {datetime.now(UTC)}")
            canopy_ds = next(data_generator)
            print(f"calling next for reference: {datetime.now(UTC)}")
            reference_ds = next(data_generator)

            stats["processed"] += 1
            print(f"✓ Processed {yyyydoy_str}")

        except (OSError, RuntimeError, ValueError) as e:
            print(f"✗ Failed {yyyydoy_str}: {e}")
            stats["failed"] += 1
