"""RINEX processing orchestration and Icechunk writing helpers."""

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
from datetime import datetime, timezone
from datetime import time as dt_time
from pathlib import Path

import numpy as np
import polars as pl
import pydantic_core
import xarray as xr
import zarr
from canvod.auxiliary.pipeline import AuxDataPipeline
from canvod.auxiliary.position import (
    ECEFPosition,
    add_spherical_coords_to_dataset,
    compute_spherical_coordinates,
)
from canvod.readers import DataDirMatcher, MatchedDirs, Rnxv3Obs
from canvod.store import GnssResearchSite
from canvod.utils.config import load_config
from canvod.utils.tools import get_version_from_pyproject
from icechunk.session import ForkSession
from icechunk.xarray import to_icechunk
from natsort import natsorted
from pydantic import ValidationError
from tqdm import tqdm

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


def preprocess_with_hermite_aux(
    rnx_file: Path,
    keep_vars: list[str] | None,
    aux_zarr_path: Path,
    receiver_position: ECEFPosition,
    receiver_type: str,
    keep_sids: list[str] | None = None,
) -> tuple[Path, xr.Dataset]:
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

    Returns
    -------
    tuple[Path, xr.Dataset]
        File path and augmented dataset with phi, theta, r

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
            start_time = time.time()
            log.info("rinex_preprocessing_started")
            log.debug(
                "preprocessing_config",
                keep_vars=keep_vars,
                keep_sids_count=len(keep_sids) if keep_sids else "all",
                aux_zarr_path=str(aux_zarr_path.name) if aux_zarr_path else None,
            )

            # 1. Read RINEX file
            log.debug("reading_rinex_file", file=str(rnx_file.name))
            rnx = Rnxv3Obs(fpath=rnx_file, include_auxiliary=False)
            ds = rnx.to_ds(
                keep_rnx_data_vars=keep_vars,
                write_global_attrs=True,
                keep_sids=keep_sids,
            )
            ds.attrs["RINEX File Hash"] = rnx.file_hash
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
                original_vars = list(ds.data_vars.keys())
                available_vars = [var for var in keep_vars if var in ds.data_vars]
                if available_vars:
                    ds = ds[available_vars]
                    log.debug(
                        "variables_filtered",
                        requested=keep_vars,
                        available=available_vars,
                        removed=list(set(original_vars) - set(available_vars)),
                    )
                else:
                    log.warning(
                        "no_requested_variables_found",
                        requested=keep_vars,
                        available=original_vars,
                    )

            # 2. Open preprocessed aux data (lazy - doesn't load everything into memory)
            log.debug("loading_auxiliary_data", aux_zarr=str(aux_zarr_path))
            aux_store = xr.open_zarr(aux_zarr_path, decode_timedelta=True)
            log.debug(
                "auxiliary_data_loaded",
                aux_dims=dict(aux_store.sizes),
                aux_vars=list(aux_store.data_vars.keys()),
            )

            # 3. Select only the epochs matching this RINEX file (fast slice operation)
            # The aux data is already properly interpolated with Hermite splines
            log.debug("selecting_matching_epochs", rinex_epochs=len(ds.epoch))
            aux_slice = aux_store.sel(epoch=ds.epoch, method="nearest")
            log.debug(
                "epochs_selected",
                selected_epochs=len(aux_slice.epoch),
                time_range=(
                    str(aux_slice.epoch.values[0])[:19],
                    str(aux_slice.epoch.values[-1])[:19],
                ),
            )

            # 4. Find common SIDs between RINEX and aux data (inner join)
            # Some satellites in RINEX may not have ephemeris data, and vice versa
            rinex_sids = set(ds.sid.values)
            aux_sids = set(aux_slice.sid.values)
            common_sids = sorted(rinex_sids.intersection(aux_sids))

            rinex_only = rinex_sids - aux_sids
            aux_only = aux_sids - rinex_sids

            if not common_sids:
                log.error(
                    "sid_intersection_empty",
                    rinex_sids=len(rinex_sids),
                    aux_sids=len(aux_sids),
                    rinex_only_sids=sorted(rinex_only)[:10],  # Show first 10
                    aux_only_sids=sorted(aux_only)[:10],
                )
                raise ValueError(
                    f"No common SIDs found between RINEX ({len(rinex_sids)} sids) "
                    f"and aux data ({len(aux_sids)} sids)"
                )

            # Filter both datasets to common SIDs
            ds = ds.sel(sid=common_sids)
            aux_slice = aux_slice.sel(sid=common_sids)

            log.debug(
                "sid_filtering_complete",
                rinex_sids=len(rinex_sids),
                aux_sids=len(aux_sids),
                common_sids=len(common_sids),
                rinex_only=len(rinex_only),
                aux_only=len(aux_only),
                common_sid_examples=common_sids[:5] if len(common_sids) > 0 else [],
            )

            # 5. Compute spherical coordinates (φ, θ, r) from ephemerides
            log.debug("computing_spherical_coordinates")
            ds_augmented = _compute_spherical_coords_fast(
                ds,
                aux_slice,
                receiver_position,
            )
            log.debug(
                "spherical_coords_computed",
                added_vars=["phi", "theta", "r"],
                final_dims=dict(ds_augmented.sizes),
            )

            duration = time.time() - start_time
            log.info(
                "rinex_preprocessing_complete",
                duration_seconds=round(duration, 2),
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

    return rnx_file, ds_augmented


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
) -> tuple[Path, ForkSession]:
    """Build an augmented dataset and write it to the given fork."""
    # 1) build augmented dataset
    fname, ds_augmented = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
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
) -> tuple[Path, ForkSession]:
    """Worker that only appends (group already exists)."""
    fname, ds_augmented = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
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
) -> ForkSession:
    """Worker uses region='auto' to write to correct position."""
    _fname, ds = preprocess_with_hermite_aux(
        rinex_file,
        keep_vars,
        aux_zarr_path,
        receiver_position,
        receiver_type,
        keep_sids,
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
    n_max_workers : int, default 12
        Maximum parallel workers (CPUs) for RINEX processing
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
        n_max_workers: int = 12,
        dask_client: Client | None = None,
    ) -> None:
        self.matched_data_dirs = matched_data_dirs
        self.site = site
        self.aux_file_path = aux_file_path
        self.n_max_workers = min(n_max_workers, os.cpu_count() or 12)
        self._dask_client = dask_client
        self._logger = get_logger(__name__).bind(
            site=site.site_name,
            workers=self.n_max_workers,
            component="processor",  # Enable component-specific logging
        )
        # Dedicated logger for icechunk store operations
        self._icechunk_log = get_logger(__name__).bind(
            site=site.site_name,
            component="icechunk",
        )

        config = load_config()
        self.keep_sids = config.sids.get_sids()

        # Cache config values formerly in globals
        aux_cfg = config.processing.aux_data
        self._agency = aux_cfg.agency
        self._product_type = aux_cfg.product_type
        servers = aux_cfg.get_ftp_servers(config.nasa_earthdata_acc_mail)
        self._ftp_server = servers[0][0]
        self._rinex_store_strategy = config.processing.storage.rinex_store_strategy

        self._logger.info(
            "processor_initialized",
            aux_file_path=str(aux_file_path) if aux_file_path else None,
            sid_filtering=len(self.keep_sids) if self.keep_sids else "all",
            cpu_count=os.cpu_count(),
        )

        # Initialize auxiliary data pipeline (loads SP3 and CLK files)
        self.aux_pipeline = self._initialize_aux_pipeline()

    def _initialize_aux_pipeline(self) -> AuxDataPipeline:
        """Initialize and load auxiliary data pipeline.

        Returns
        -------
        AuxDataPipeline
            Loaded pipeline with ephemerides and clock data

        """
        start_time = time.time()
        self._logger.info(
            "aux_pipeline_initialization_started",
            agency=self._agency,
            product_type=self._product_type,
        )

        # Get credentials from YAML config
        config = load_config()
        user_email = config.nasa_earthdata_acc_mail

        # Determine aux_file_path: explicit > config aux_data_dir > site data root
        aux_file_path = self.aux_file_path
        if aux_file_path is None:
            configured_aux_dir = config.processing.storage.aux_data_dir
            if configured_aux_dir is not None:
                aux_file_path = configured_aux_dir
            else:
                aux_file_path = Path(self.site.site_config["gnss_site_data_root"])

        pipeline = AuxDataPipeline.create_standard(
            matched_dirs=self.matched_data_dirs,
            aux_file_path=aux_file_path,
            agency=self._agency,
            product_type=self._product_type,
            ftp_server=self._ftp_server,
            user_email=user_email,
            keep_sids=self.keep_sids,
        )

        # Load all auxiliary files into memory
        pipeline.load_all()

        duration = time.time() - start_time
        self._logger.info(
            "aux_pipeline_initialization_complete",
            duration_seconds=round(duration, 2),
            products=list(pipeline._cache.keys())
            if hasattr(pipeline, "_cache")
            else [],
        )
        return pipeline

    def _preprocess_aux_data_with_hermite(
        self,
        rinex_files: list[Path],
        output_path: Path,
    ) -> float:
        """Preprocess auxiliary data using proper interpolation strategies."""
        start_time = time.time()
        self._logger.info(
            "aux_preprocessing_started",
            rinex_files=len(rinex_files),
            output_path=str(output_path),
            interpolation_method="hermite_cubic",
        )

        # 1. Read first RINEX file to infer temporal properties
        self._logger.debug(
            "sampling_detection_started",
            sample_file=rinex_files[0].name,
        )
        first_rnx = Rnxv3Obs(fpath=rinex_files[0], include_auxiliary=False)
        first_ds = first_rnx.to_ds(keep_rnx_data_vars=[], write_global_attrs=True)

        # 2. Detect sampling interval
        time_diff = (first_ds.epoch[1] - first_ds.epoch[0]).values
        sampling_interval = float(time_diff / np.timedelta64(1, "s"))
        self._logger.info(
            "sampling_detected",
            sampling_interval_seconds=sampling_interval,
        )

        # 3. ✅ FIX: Generate complete 24-hour epoch grid instead of just first file
        # Get the day boundaries
        first_epoch = first_ds.epoch.values[0]
        day_start = np.datetime64(first_epoch.astype("datetime64[D]"))
        _day_end = day_start + np.timedelta64(1, "D")

        self._logger.debug(
            "day_boundaries_detected",
            first_epoch=str(first_epoch),
            day_start=str(day_start),
            sampling_interval=sampling_interval,
        )

        # Create uniform epoch grid for entire day
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
        self._logger.debug("fetching_auxiliary_datasets")
        ephem_ds = self.aux_pipeline.get("ephemerides")
        clock_ds = self.aux_pipeline.get("clock")
        self._logger.debug(
            "auxiliary_datasets_fetched",
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

        interp_start = time.time()
        ephem_interp = sp3_interpolator.interpolate(ephem_ds, target_epochs)
        interp_duration = time.time() - interp_start

        self._logger.debug(
            "ephemeris_interpolation_complete",
            duration_seconds=round(interp_duration, 2),
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

        clock_start = time.time()
        clock_interp = clock_interpolator.interpolate(clock_ds, target_epochs)
        clock_duration = time.time() - clock_start

        self._logger.debug(
            "clock_interpolation_complete",
            duration_seconds=round(clock_duration, 2),
            output_shape=dict(clock_interp.sizes),
        )

        # Store interpolation metadata
        clock_interp.attrs["interpolator_config"] = clock_interpolator.to_attrs()

        # 7. Merge ephemerides and clock into single dataset
        self._logger.debug("merging_auxiliary_datasets")
        aux_processed = xr.merge([ephem_interp, clock_interp])
        self._logger.debug(
            "merge_complete",
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

        duration = time.time() - start_time
        self._logger.info(
            "aux_preprocessing_complete",
            duration_seconds=round(duration, 2),
            data_size=dict(aux_processed.sizes),
            output_path=str(output_path),
        )

        return sampling_interval

    def _get_rinex_files(self, rinex_dir: Path) -> list[Path]:
        """Get sorted list of RINEX files from directory."""
        if not rinex_dir.exists():
            self._logger.warning("Directory does not exist: %s", rinex_dir)
            return []

        patterns = ["*.??o", "*.??O", "*.rnx", "*.RNX"]
        rinex_files = []

        for pattern in patterns:
            files = list(rinex_dir.glob(pattern))
            rinex_files.extend(files)

        return natsorted(rinex_files)

    def _ensure_aux_data_preprocessed(
        self,
        canopy_files: list[Path],
        date_str: str,
    ) -> Path:
        """Ensure auxiliary data is preprocessed and available.

        Parameters
        ----------
        canopy_files : list[Path]
            Canopy RINEX files for sampling detection
        date_str : str
            Date string (e.g., '2025213')

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

        config = load_config()
        aux_base_dir = config.processing.storage.get_aux_data_dir()
        aux_zarr_path = aux_base_dir / f"aux_{date_str}.zarr"

        # Always reprocess from raw SP3/CLK files — the Hermite interpolation
        # is cheap and this avoids stale caches when SIDs change.
        if aux_zarr_path.exists():
            shutil.rmtree(aux_zarr_path)

        self._logger.info(
            "aux_preprocessing_required",
            output_path=str(aux_zarr_path),
            interpolation="hermite_cubic",
        )
        try:
            self._preprocess_aux_data_with_hermite(canopy_files, aux_zarr_path)

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
    ) -> list[tuple[Path, xr.Dataset]]:
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

        Returns
        -------
        list[tuple[Path, xr.Dataset]]
            List of (filename, augmented_dataset) tuples, sorted chronologically

        """
        if self._dask_client is not None and _HAS_DISTRIBUTED:
            return self._parallel_process_rinex_dask(
                rinex_files, keep_vars, aux_zarr_path, receiver_position, receiver_type
            )
        return self._parallel_process_rinex_pool(
            rinex_files, keep_vars, aux_zarr_path, receiver_position, receiver_type
        )

    def _parallel_process_rinex_dask(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
    ) -> list[tuple[Path, xr.Dataset]]:
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

        self._logger.debug(
            "parallel_config",
            max_workers=self.n_max_workers,
            cpu_count=os.cpu_count(),
            files_per_worker=round(len(rinex_files) / self.n_max_workers, 1),
        )

        results: list[tuple[Path, xr.Dataset]] = []
        task_submission_start = time.time()

        # Submit all tasks to the Dask cluster
        future_to_file = {
            client.submit(
                preprocess_with_hermite_aux,
                rinex_file,
                keep_vars,
                aux_zarr_path,
                receiver_position,
                receiver_type,
                self.keep_sids,
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

        for fut in tqdm(
            dask_as_completed(future_to_file),
            total=len(future_to_file),
            desc=f"Processing {receiver_type}",
            unit="file",
        ):
            try:
                fname, ds_augmented = fut.result()
                results.append((fname, ds_augmented))
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
        return results

    def _parallel_process_rinex_pool(
        self,
        rinex_files: list[Path],
        keep_vars: list[str],
        aux_zarr_path: Path,
        receiver_position: ECEFPosition,
        receiver_type: str,
    ) -> list[tuple[Path, xr.Dataset]]:
        """Fallback: process RINEX files via ProcessPoolExecutor."""
        start_time = time.time()
        self._logger.info(
            "parallel_processing_started",
            workers=self.n_max_workers,
            files=len(rinex_files),
            receiver_type=receiver_type,
            executor_type="ProcessPoolExecutor",
        )

        self._logger.debug(
            "parallel_config",
            max_workers=self.n_max_workers,
            cpu_count=os.cpu_count(),
            files_per_worker=round(len(rinex_files) / self.n_max_workers, 1),
        )

        results: list[tuple[Path, xr.Dataset]] = []
        task_submission_start = time.time()

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

            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Processing {receiver_type}",
                unit="file",
            ):
                try:
                    fname, ds_augmented = fut.result()
                    results.append((fname, ds_augmented))
                    completed_count += 1

                    if completed_count % 10 == 0:
                        self._logger.debug(
                            "processing_progress",
                            completed=completed_count,
                            total=len(futures),
                            failed=failed_count,
                            progress_pct=round(100 * completed_count / len(futures), 1),
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
        return results

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

        for idx, (fname, ds) in enumerate(
            tqdm(augmented_datasets, desc=f"Appending {receiver_name}")
        ):
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
                rinex_hash = ds.attrs.get("RINEX File Hash")
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
            fname: ds.attrs.get("RINEX File Hash") for fname, ds in augmented_datasets
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

        # STEP 2: Open session ONCE
        log.info("Opening Icechunk session...")
        t3 = time.time()
        with self.site.rinex_store.writable_session() as session:
            groups = self.site.rinex_store.list_groups() or []
            t4 = time.time()
            log.info("Session opened in %.2fs", t4 - t3)

            actions = {"initial": 0, "skipped": 0, "appended": 0, "written": 0}
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
                snapshot_id = session.commit(commit_msg)
                t8 = time.time()
                log.info(
                    "Commit complete in %.2fs (snapshot: %s...)",
                    t8 - t7,
                    snapshot_id[:8],
                )

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

    def _append_to_icechunk(
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
            fname: ds.attrs.get("RINEX File Hash") for fname, ds in augmented_datasets
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

        # STEP 2: Open session ONCE
        log.info("Opening Icechunk session...")
        t3 = time.time()
        session = self.site.rinex_store.repo.writable_session(branch="main")
        groups = self.site.rinex_store.list_groups() or []
        t4 = time.time()
        log.info("Session opened in %.2fs", t4 - t3)

        actions = {"initial": 0, "skipped": 0, "appended": 0, "written": 0}
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

            log.info("Committing data: %s", summary)
            t7 = time.time()
            t8 = time.time()

            # STEP 5: Metadata in a separate commit
            log.info(
                "Writing metadata for %s files...",
                len(metadata_records),
            )
            t9 = time.time()
            try:
                self.site.rinex_store.append_metadata_bulk(
                    group_name=receiver_name,
                    rows=metadata_records,
                    session=session,  # link metadata to the data commit
                )
            except (OSError, RuntimeError, ValueError):
                log.warning("Metadata commit failed")
            t10 = time.time()
            log.info("Metadata commit complete in %.2fs", t10 - t9)

            log.info("Committing: %s", summary)
            t7 = time.time()
            snapshot_id = session.commit(commit_msg)
            t8 = time.time()
            log.info(
                "Commit complete in %.2fs (snapshot: %s...)",
                t8 - t7,
                snapshot_id[:8],
            )

            expired = self.site.rinex_store.expire_old_snapshots(days=7)

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
            fname: ds.attrs.get("RINEX File Hash") for fname, ds in augmented_datasets
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

        # STEP 2: Open session ONCE
        log.info("Opening Icechunk session...")
        t3 = time.time()
        session = self.site.rinex_store.repo.writable_session(branch="main")
        groups = self.site.rinex_store.list_groups() or []
        t4 = time.time()
        log.info("Session opened in %.2fs", t4 - t3)

        actions = {"initial": 0, "skipped": 0, "appended": 0, "written": 0}
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
            snapshot_id = session.commit(commit_msg)
            t8 = time.time()
            log.info(
                "Commit complete in %.2fs (snapshot: %s...)",
                t8 - t7,
                snapshot_id[:8],
            )

            # STEP 5: Metadata in a separate commit
            log.info(
                "Writing metadata for %s files...",
                len(metadata_records),
            )
            t9 = time.time()
            try:
                meta_session = self.site.rinex_store.repo.writable_session(
                    branch="main"
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
        # STEP 2: Compute receiver position ONCE (same for all receivers)
        # ====================================================================
        first_rnx = Rnxv3Obs(fpath=canopy_files[0], include_auxiliary=False)
        first_ds = first_rnx.to_ds(keep_rnx_data_vars=[], write_global_attrs=True)
        receiver_position = ECEFPosition.from_ds_metadata(first_ds)
        self._logger.info(
            "Computed receiver position (shared): %s",
            receiver_position,
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

            # 3c. Parallel process via Dask (or ProcessPoolExecutor fallback)
            augmented_datasets = self._parallel_process_rinex(
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

    def parsed_rinex_data_gen(
        self,
        keep_vars: list[str] | None = None,
        receiver_configs: list[tuple[str, str, Path]]
        | list[tuple[str, str, Path, Path | None]]
        | None = None,
    ) -> Generator[tuple[str, xr.Dataset, float], None, None]:
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
            List of (receiver_name, receiver_type, data_dir) or
            (receiver_name, receiver_type, data_dir, position_data_dir) tuples.
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

        # Normalize to 4-tuples
        normalized_configs: list[tuple[str, str, Path, Path | None]] = []
        for cfg in receiver_configs:
            if len(cfg) == 3:
                normalized_configs.append((*cfg, None))
            else:
                normalized_configs.append(cfg)

        if keep_vars is None:
            keep_vars = load_config().processing.processing.keep_rnx_vars

        pipeline_start = time.time()
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
        first_receiver_name, _first_receiver_type, first_data_dir, _ = (
            normalized_configs[0]
        )
        first_files = self._get_rinex_files(first_data_dir)

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
        aux_zarr_path = self._ensure_aux_data_preprocessed(first_files, date_str)

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
        ) in normalized_configs:
            receiver_start = time.time()

            self._logger.info(
                "receiver_processing_started",
                receiver=receiver_name,
                receiver_type=receiver_type,
                data_dir=str(data_dir),
                position_from=str(position_data_dir) if position_data_dir else "self",
            )

            # Get RINEX files for this receiver
            rinex_files = self._get_rinex_files(data_dir)
            if not rinex_files:
                self._logger.warning(
                    "no_rinex_files_found",
                    receiver=receiver_name,
                    data_dir=str(data_dir),
                )
                continue

            # Compute receiver position: from position_data_dir if set,
            # otherwise from this receiver's own first file
            position_files = (
                self._get_rinex_files(position_data_dir)
                if position_data_dir
                else rinex_files
            )
            first_rnx = None
            for _f, ff in enumerate(position_files):
                try:
                    first_rnx = Rnxv3Obs(fpath=ff, include_auxiliary=False)
                    break
                except ValidationError as e:
                    self._logger.warning(
                        "Validation error for %s: %s",
                        ff.name,
                        e,
                    )
                    for error in e.errors():
                        self._logger.debug("Field: %s", error["loc"])
                        self._logger.debug("Message: %s", error["msg"])
                        self._logger.debug("Type: %s", error["type"])
                except pydantic_core.ValidationError as e:
                    self._logger.warning(
                        "Core validation error for %s: %s",
                        ff.name,
                        e,
                    )
                except (OSError, RuntimeError, ValueError) as e:
                    self._logger.warning(
                        "Unexpected error for %s: %s",
                        ff.name,
                        e,
                    )

            if first_rnx is None:
                self._logger.error(
                    "No valid RINEX files found for %s",
                    receiver_name,
                )
                continue

            first_ds = first_rnx.to_ds(keep_rnx_data_vars=[], write_global_attrs=True)
            receiver_position = ECEFPosition.from_ds_metadata(first_ds)
            self._logger.info(
                "Computed receiver position for %s: %s (from %s)",
                receiver_name,
                receiver_position,
                str(position_data_dir) if position_data_dir else "own files",
            )

            if data_dir not in _rinex_cache:
                # First time seeing this data_dir — full parallel processing
                self._logger.info(
                    "rinex_files_discovered",
                    receiver=receiver_name,
                    files=len(rinex_files),
                )

                augmented_datasets = self._parallel_process_rinex(
                    rinex_files=rinex_files,
                    keep_vars=keep_vars,
                    aux_zarr_path=aux_zarr_path,
                    receiver_position=receiver_position,
                    receiver_type=receiver_name,
                )

                # Cache for reuse by other store groups with the same data_dir
                _rinex_cache[data_dir] = (augmented_datasets, rinex_files)
            else:
                # Reuse cached RINEX data, only recompute SCS with new position
                cached_datasets, rinex_files = _rinex_cache[data_dir]

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

            # Append to Icechunk with receiver_name as group
            self._append_to_icechunk(
                augmented_datasets=augmented_datasets,
                receiver_name=receiver_name,
                rinex_files=rinex_files,
            )

            # Yield final daily dataset
            date_obj = self.matched_data_dirs.yyyydoy.date
            start_time = datetime.combine(date_obj, datetime.min.time())
            end_time = datetime.combine(date_obj, datetime.max.time())
            time_range = (start_time, end_time)

            daily_dataset = self.site.read_receiver_data(
                receiver_name=receiver_name, time_range=time_range
            )

            receiver_duration = time.time() - receiver_start

            self._logger.info(
                "receiver_processing_complete",
                receiver=receiver_name,
                duration_seconds=round(receiver_duration, 2),
                dataset_size=dict(daily_dataset.sizes),
                epochs=len(daily_dataset.epoch) if "epoch" in daily_dataset.dims else 0,
                sids=len(daily_dataset.sid) if "sid" in daily_dataset.dims else 0,
            )

            yield receiver_name, daily_dataset, receiver_duration

        pipeline_duration = time.time() - pipeline_start
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
            _fname, ds = preprocess_with_hermite_aux(
                rinex_file,
                keep_vars,
                aux_zarr_path,
                receiver_position,
                receiver_type,
                self.keep_sids,
            )
            all_epochs.extend(ds.epoch.values)

        # Create empty dataset with full structure
        _first_fname, first_ds = preprocess_with_hermite_aux(
            rinex_files_sorted[0],
            keep_vars,
            aux_zarr_path,
            receiver_position,
            receiver_type,
            self.keep_sids,
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
                    pure=False,
                )
                for rinex_file in rinex_files_sorted
            ]

            for fut in tqdm(
                dask_as_completed(futures),
                total=len(futures),
                desc=f"Writing {receiver_name}",
                unit="file",
            ):
                returned_fork = fut.result()
                remote_sessions.append(returned_fork)
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

                for fut in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc=f"Writing {receiver_name}",
                    unit="file",
                ):
                    returned_fork = fut.result()
                    remote_sessions.append(returned_fork)

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
            fname: ds.attrs.get("RINEX File Hash") for fname, ds in augmented_datasets
        }
        valid_hashes = [h for h in file_hash_map.values() if h]
        existing_hashes = self.site.rinex_store.batch_check_existing(
            receiver_name, valid_hashes
        )

        actions = {"initial": 0, "skipped": 0, "appended": 0, "written": 0}
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
                            "written_at": datetime.now(timezone.utc).isoformat(),
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
            fname: ds.attrs.get("RINEX File Hash") for fname, ds in augmented_datasets
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

        # STEP 2: Open session ONCE
        log.info("Opening Icechunk session...")
        t3 = time.time()
        session = self.site.rinex_store.repo.writable_session(branch="main")
        groups = self.site.rinex_store.list_groups() or []
        t4 = time.time()
        log.info("Session opened in %.2fs", t4 - t3)

        actions = {"initial": 0, "skipped": 0, "appended": 0, "written": 0}
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
            snapshot_id = session.commit(commit_msg)
            t8 = time.time()
            log.info(
                "Commit complete in %.2fs (snapshot: %s...)",
                t8 - t7,
                snapshot_id[:8],
            )

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

    def parsed_rinex_data_gen_parallel(
        self,
        keep_vars: list[str] | None = None,
        receiver_types: list[str] | None = None,
    ) -> Generator[xr.Dataset, None, None]:
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
        # STEP 2: Compute receiver position ONCE (same for all receivers)
        # ====================================================================
        first_rnx = Rnxv3Obs(fpath=canopy_files[0], include_auxiliary=False)
        first_ds = first_rnx.to_ds(keep_rnx_data_vars=[], write_global_attrs=True)
        receiver_position = ECEFPosition.from_ds_metadata(first_ds)
        self._logger.info(
            "Computed receiver position (shared): %s",
            receiver_position,
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
    print(f"stared main block at {datetime.now(timezone.utc)}")

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
            print(
                f"instantiating processor for {yyyydoy_str}: "
                f"{datetime.now(timezone.utc)}"
            )
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
                f"{datetime.now(timezone.utc)}"
            )
            data_generator = processor.parsed_rinex_data_gen()
            print(f"calling next for canopy: {datetime.now(timezone.utc)}")
            canopy_ds = next(data_generator)
            print(f"calling next for reference: {datetime.now(timezone.utc)}")
            reference_ds = next(data_generator)

            stats["processed"] += 1
            print(f"✓ Processed {yyyydoy_str}")

        except (OSError, RuntimeError, ValueError) as e:
            print(f"✗ Failed {yyyydoy_str}: {e}")
            stats["failed"] += 1
