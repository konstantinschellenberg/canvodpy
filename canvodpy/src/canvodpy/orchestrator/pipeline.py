"""Processing pipeline orchestration for site-level workflows."""

from __future__ import annotations

import time as _time
from collections import defaultdict
from collections.abc import Generator
from pathlib import Path

import pint
import xarray as xr
from canvod.readers import MatchedDirs, PairDataDirMatcher
from canvod.readers.gnss_specs.constants import UREG
from canvod.store import GnssResearchSite
from canvod.utils.tools import YYYYDOY

from canvodpy.logging import get_logger
from canvodpy.orchestrator.processor import RinexDataProcessor
from canvodpy.orchestrator.resources import DaskClusterManager, MemoryMonitor


class PipelineOrchestrator:
    """Orchestrate RINEX processing pipeline for all receiver pairs at a site.

    Processes each unique receiver once per day, regardless of how many
    pairs it's involved in.

    Parameters
    ----------
    site : GnssResearchSite
        Research site configuration
    n_max_workers : int
        Maximum parallel workers per day
    dry_run : bool
        If True, only simulate processing without executing
    batch_hours : float
        Hours of data per processing batch (default: 24.0)
    max_memory_gb : float | None
        Soft RAM limit in GB (None = no limit)
    cpu_affinity : list[int] | None
        Pin workers to specific CPU core IDs (None = no restriction)
    nice_priority : int
        Process nice value (0=normal, 19=lowest)

    """

    def __init__(
        self,
        site: GnssResearchSite,
        n_max_workers: int = 12,
        dry_run: bool = False,
        batch_hours: float = 24.0,
        max_memory_gb: float | None = None,
        cpu_affinity: list[int] | None = None,
        nice_priority: int = 0,
    ) -> None:
        self.site = site
        self.n_max_workers = n_max_workers
        self.dry_run = dry_run
        self.batch_hours = batch_hours
        self._batch_duration: pint.Quantity = batch_hours * UREG.hour
        self._max_memory_gb = max_memory_gb
        self._cpu_affinity = cpu_affinity
        self._nice_priority = nice_priority
        self._memory_monitor = MemoryMonitor(max_memory_gb=max_memory_gb)
        self._logger = get_logger(__name__).bind(site=site.site_name)

        # Create Dask LocalCluster for parallel RINEX processing
        memory_limit: str | float = "auto"
        if max_memory_gb is not None and n_max_workers > 0:
            memory_limit = max_memory_gb / n_max_workers * (1024**3)  # bytes

        try:
            self._cluster_manager = DaskClusterManager(
                n_workers=n_max_workers,
                memory_limit_per_worker=memory_limit,
                cpu_affinity=cpu_affinity,
                nice_priority=nice_priority,
            )
        except ImportError:
            self._logger.warning(
                "dask_distributed_unavailable",
                message="Falling back to ProcessPoolExecutor",
            )
            self._cluster_manager = None

        self.pair_matcher = PairDataDirMatcher(
            base_dir=site.site_config["gnss_site_data_root"],
            receivers=site.receivers,
            analysis_pairs=site.vod_analyses,
        )

        self._logger.info(
            "pipeline_initialized",
            site=site.site_name,
            analysis_pairs=len(site.active_vod_analyses),
            n_max_workers=n_max_workers,
            dry_run=dry_run,
            batch_hours=batch_hours,
            executor="dask.distributed"
            if self._cluster_manager is not None
            else "ProcessPoolExecutor",
        )

    def close(self) -> None:
        """Shut down the Dask cluster if active."""
        if self._cluster_manager is not None:
            self._logger.info("pipeline_orchestrator_closing")
            self._cluster_manager.close()
            self._logger.info("pipeline_orchestrator_closed")

    def __enter__(self) -> PipelineOrchestrator:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _group_by_date_and_receiver(
        self,
    ) -> dict[str, dict[str, tuple[Path, str, Path | None]]]:
        """Group receivers by date, expanding references per canopy via scs_from.

        Canopy receivers are deduplicated (processed once with own position).
        Reference receivers are expanded: one entry per canopy in scs_from,
        stored as ``{ref_name}_{canopy_name}`` with position_data_dir pointing
        to the canopy's RINEX directory.

        Returns
        -------
        dict[str, dict[str, tuple[Path, str, Path | None]]]
            {date: {store_group_name: (data_dir, receiver_type, position_data_dir)}}

        """
        grouped: dict[str, dict[str, tuple[Path, str, Path | None]]] = defaultdict(dict)
        site_config = self.site._site_config

        for pair_dirs in self.pair_matcher:
            date_key = pair_dirs.yyyydoy.to_str()

            # Add canopy receiver if not already present (uses own position)
            if pair_dirs.canopy_receiver not in grouped[date_key]:
                grouped[date_key][pair_dirs.canopy_receiver] = (
                    pair_dirs.canopy_data_dir,
                    "canopy",
                    None,
                )

            # Expand reference receiver per canopy in scs_from
            ref_name = pair_dirs.reference_receiver
            ref_cfg = site_config.receivers.get(ref_name)
            if ref_cfg and ref_cfg.type == "reference":
                canopy_names = site_config.resolve_scs_from(ref_name)
                for canopy_name in canopy_names:
                    store_group = f"{ref_name}_{canopy_name}"
                    if store_group not in grouped[date_key]:
                        # Get canopy data dir for position computation
                        canopy_cfg = site_config.receivers.get(canopy_name)
                        if canopy_cfg:
                            canopy_position_dir = (
                                site_config.get_base_path()
                                / canopy_cfg.directory
                                / pair_dirs.yyyydoy.yydoy
                            )
                        else:
                            canopy_position_dir = None
                        grouped[date_key][store_group] = (
                            pair_dirs.reference_data_dir,
                            "reference",
                            canopy_position_dir,
                        )

        return grouped

    def preview_processing_plan(self) -> dict:
        """Preview what would be processed without executing.

        Returns
        -------
        dict
            Summary of dates, receivers, and files to process

        """
        grouped = self._group_by_date_and_receiver()

        plan = {
            "site": self.site.site_name,
            "dates": [],
            "total_receivers": 0,
            "total_files": 0,
        }

        for date_key, receivers in sorted(grouped.items()):
            date_info = {"date": date_key, "receivers": []}

            for receiver_name, (data_dir, receiver_type, _pos_dir) in sorted(
                receivers.items()
            ):
                files = list(data_dir.glob("*.2*o"))

                receiver_info = {
                    "name": receiver_name,
                    "type": receiver_type,
                    "files": len(files),
                    "dir": str(data_dir),
                }

                date_info["receivers"].append(receiver_info)
                plan["total_files"] += len(files)

            plan["dates"].append(date_info)
            plan["total_receivers"] += len(receivers)

        return plan

    def print_preview(self) -> None:
        """Print a formatted preview of the processing plan."""
        plan = self.preview_processing_plan()

        print(f"\n{'=' * 70}")
        print(f"PROCESSING PLAN FOR SITE: {plan['site']}")
        print(f"{'=' * 70}")
        print(f"Total unique receivers to process: {plan['total_receivers']}")
        print(f"Total RINEX files: {plan['total_files']}")
        print(f"{'=' * 70}\n")

        for date_info in plan["dates"]:
            print(f"Date: {date_info['date']}")
            for receiver_info in date_info["receivers"]:
                print(
                    f"  {receiver_info['name']} ({receiver_info['type']}): "
                    f"{receiver_info['files']} files"
                )
                print(f"    {receiver_info['dir']}")
            print()

    def _filter_dates(
        self,
        grouped: dict[str, dict[str, tuple[Path, str, Path | None]]],
        start_from: str | None,
        end_at: str | None,
    ) -> list[tuple[str, dict[str, tuple[Path, str, Path | None]]]]:
        """Filter and sort dates within the requested range.

        Parameters
        ----------
        grouped : dict
            Date-grouped receiver configs from ``_group_by_date_and_receiver()``.
        start_from : str | None
            YYYYDOY string to start from (inclusive).
        end_at : str | None
            YYYYDOY string to end at (inclusive).

        Returns
        -------
        list[tuple[str, dict]]
            Filtered and sorted ``(date_key, receivers)`` pairs.

        """
        filtered = []
        for date_key, receivers in sorted(grouped.items()):
            if start_from and date_key < start_from:
                self._logger.info(
                    "date_skipped_before_range",
                    date=date_key,
                    start_from=start_from,
                )
                continue
            if end_at and date_key > end_at:
                self._logger.info(
                    "date_range_complete",
                    date=date_key,
                    end_at=end_at,
                )
                break
            filtered.append((date_key, receivers))
        return filtered

    def _validate_batch_floor(
        self,
        n_files: int,
    ) -> pint.Quantity:
        """Validate batch_hours is at least the duration of one file.

        If the configured batch duration is smaller than one file's duration,
        clamp up to the file duration with a warning.

        Parameters
        ----------
        n_files : int
            Number of RINEX files per day for a receiver.

        Returns
        -------
        pint.Quantity
            Effective batch duration (in hours), clamped if necessary.

        """
        file_duration: pint.Quantity = (24 * UREG.hour) / n_files
        batch_duration = self._batch_duration

        if batch_duration < file_duration:
            self._logger.warning(
                "batch_duration_clamped",
                requested=str(batch_duration),
                min_file_duration=str(file_duration),
                n_files=n_files,
            )
            batch_duration = file_duration
        else:
            self._logger.info(
                "batch_duration_validated",
                batch_duration=str(batch_duration),
                file_duration=str(file_duration),
                n_files=n_files,
            )

        return batch_duration

    def _process_single_date(
        self,
        date_key: str,
        receivers: dict[str, tuple[Path, str, Path | None]],
        keep_vars: list[str] | None,
    ) -> tuple[str, dict[str, xr.Dataset], dict[str, float]] | None:
        """Process all receivers for a single date (one DOY).

        Parameters
        ----------
        date_key : str
            YYYYDOY string.
        receivers : dict
            ``{store_group: (data_dir, receiver_type, position_data_dir)}``.
        keep_vars : list[str] | None
            Variables to keep in datasets.

        Returns
        -------
        tuple or None
            ``(date_key, datasets, timings)`` or None if processing failed.

        """
        date_start = _time.monotonic()

        self._logger.info(
            "date_processing_started",
            date=date_key,
            receivers=len(receivers),
            receiver_names=sorted(receivers.keys()),
        )

        receiver_configs = [
            (receiver_name, receiver_type, data_dir, position_data_dir)
            for receiver_name, (
                data_dir,
                receiver_type,
                position_data_dir,
            ) in sorted(receivers.items())
        ]

        first_data_dir = receiver_configs[0][2]
        matched_dirs = MatchedDirs(
            canopy_data_dir=first_data_dir,
            reference_data_dir=first_data_dir,
            yyyydoy=YYYYDOY.from_str(date_key),
        )

        dask_client = (
            self._cluster_manager.client if self._cluster_manager is not None else None
        )

        try:
            processor = RinexDataProcessor(
                matched_data_dirs=matched_dirs,
                site=self.site,
                n_max_workers=self.n_max_workers,
                dask_client=dask_client,
            )
        except RuntimeError as e:
            if "Failed to download" in str(e):
                self._logger.warning(
                    "auxiliary_download_failed",
                    date=date_key,
                    error=str(e),
                    exception=type(e).__name__,
                    elapsed_seconds=round(_time.monotonic() - date_start, 2),
                )
                return None
            raise

        datasets: dict[str, xr.Dataset] = {}
        timings: dict[str, float] = {}
        try:
            for receiver_name, ds, proc_time in processor.parsed_rinex_data_gen(
                keep_vars=keep_vars, receiver_configs=receiver_configs
            ):
                datasets[receiver_name] = ds
                timings[receiver_name] = proc_time
                self._logger.debug(
                    "receiver_result_collected",
                    date=date_key,
                    receiver=receiver_name,
                    dataset_dims=dict(ds.sizes) if hasattr(ds, "sizes") else {},
                    proc_time_seconds=round(proc_time, 2),
                )
        except (OSError, RuntimeError, ValueError) as e:
            self._logger.error(
                "rinex_processing_failed",
                date=date_key,
                error=str(e),
                exception=type(e).__name__,
                elapsed_seconds=round(_time.monotonic() - date_start, 2),
            )
            return None

        date_elapsed = _time.monotonic() - date_start
        self._logger.info(
            "date_processing_complete",
            date=date_key,
            receivers_processed=len(datasets),
            receiver_names=sorted(datasets.keys()),
            total_seconds=round(date_elapsed, 2),
            per_receiver_seconds={k: round(v, 2) for k, v in timings.items()},
        )

        return date_key, datasets, timings

    def _process_multi_day_batches(
        self,
        filtered_dates: list[tuple[str, dict[str, tuple[Path, str, Path | None]]]],
        keep_vars: list[str] | None,
    ) -> Generator[tuple[str, dict[str, xr.Dataset], dict[str, float]], None, None]:
        """Process dates in multi-day batches (batch_hours >= 24).

        Accumulates multiple calendar days into one batch, but still yields
        per-DOY and commits to Icechunk per-DOY (sequential writes).

        Parameters
        ----------
        filtered_dates : list
            Filtered ``(date_key, receivers)`` pairs.
        keep_vars : list[str] | None
            Variables to keep in datasets.

        Yields
        ------
        tuple[str, dict[str, xr.Dataset], dict[str, float]]
            ``(date_key, datasets, timings)`` per DOY.

        """
        days_per_batch = max(1, round(self.batch_hours / 24))
        total_batches = (len(filtered_dates) + days_per_batch - 1) // days_per_batch

        self._logger.info(
            "multi_day_batch_strategy",
            batch_hours=self.batch_hours,
            days_per_batch=days_per_batch,
            total_dates=len(filtered_dates),
            total_batches=total_batches,
        )

        # Partition dates into batches
        for batch_idx, batch_start in enumerate(
            range(0, len(filtered_dates), days_per_batch)
        ):
            batch = filtered_dates[batch_start : batch_start + days_per_batch]
            batch_date_keys = [dk for dk, _ in batch]
            batch_start_time = _time.monotonic()

            self._logger.info(
                "batch_started",
                batch_index=batch_idx + 1,
                total_batches=total_batches,
                batch_dates=batch_date_keys,
                batch_size=len(batch),
            )

            self._memory_monitor.log_memory_stats(
                context=f"before_batch_{batch_idx + 1}"
            )

            # Process each DOY in the batch sequentially
            # (Icechunk commits must be sequential for local store)
            doys_succeeded = 0
            doys_failed = 0
            for date_key, receivers in batch:
                result = self._process_single_date(date_key, receivers, keep_vars)
                if result is not None:
                    doys_succeeded += 1
                    self._memory_monitor.log_memory_stats(
                        context=f"after_doy_{date_key}"
                    )
                    yield result
                else:
                    doys_failed += 1
                    self._logger.warning(
                        "doy_failed_in_batch",
                        date=date_key,
                        batch_index=batch_idx + 1,
                    )

            batch_elapsed = _time.monotonic() - batch_start_time
            self._logger.info(
                "batch_complete",
                batch_index=batch_idx + 1,
                total_batches=total_batches,
                batch_dates=batch_date_keys,
                doys_succeeded=doys_succeeded,
                doys_failed=doys_failed,
                batch_seconds=round(batch_elapsed, 2),
            )

    def _process_sub_day_batches(
        self,
        filtered_dates: list[tuple[str, dict[str, tuple[Path, str, Path | None]]]],
        keep_vars: list[str] | None,
    ) -> Generator[tuple[str, dict[str, xr.Dataset], dict[str, float]], None, None]:
        """Process dates with sub-day file batching (batch_hours < 24).

        Splits RINEX files within each day into smaller chunks based on
        ``batch_hours``, processing each chunk separately. Still yields
        per-DOY and commits to Icechunk per-DOY.

        Parameters
        ----------
        filtered_dates : list
            Filtered ``(date_key, receivers)`` pairs.
        keep_vars : list[str] | None
            Variables to keep in datasets.

        Yields
        ------
        tuple[str, dict[str, xr.Dataset], dict[str, float]]
            ``(date_key, datasets, timings)`` per DOY.

        """
        self._logger.info(
            "sub_day_batch_strategy",
            batch_hours=self.batch_hours,
            total_dates=len(filtered_dates),
        )

        # For sub-day batches, we still process per-date but the processor
        # handles smaller file chunks. The current processor already processes
        # all files for a date, so we delegate to the single-date processor
        # which internally uses ProcessPoolExecutor for parallelism.
        # The sub-day batch_hours primarily controls how many files are
        # submitted to the pool at once.
        dates_succeeded = 0
        dates_failed = 0
        for date_idx, (date_key, receivers) in enumerate(filtered_dates):
            self._memory_monitor.log_memory_stats(context=f"before_sub_day_{date_key}")
            self._logger.info(
                "sub_day_date_started",
                date=date_key,
                date_index=date_idx + 1,
                total_dates=len(filtered_dates),
            )

            result = self._process_single_date(date_key, receivers, keep_vars)
            if result is not None:
                dates_succeeded += 1
                yield result
            else:
                dates_failed += 1
                self._logger.warning(
                    "sub_day_date_failed",
                    date=date_key,
                    date_index=date_idx + 1,
                )

        self._logger.info(
            "sub_day_strategy_complete",
            dates_succeeded=dates_succeeded,
            dates_failed=dates_failed,
            total_dates=len(filtered_dates),
        )

    def process_by_date(
        self,
        keep_vars: list[str] | None = None,
        start_from: str | None = None,
        end_at: str | None = None,
    ) -> Generator[tuple[str, dict[str, xr.Dataset], dict[str, float]], None, None]:
        """Process all receivers grouped by date.

        Each unique receiver is processed once per day with its actual name
        as the Icechunk group name. Dispatches to multi-day or sub-day batch
        strategies based on ``batch_hours``.

        Parameters
        ----------
        keep_vars : list[str], optional
            Variables to keep in datasets
        start_from : str, optional
            YYYYDOY string to start from
        end_at : str, optional
            YYYYDOY string to end at

        Yields
        ------
        tuple[str, dict[str, xr.Dataset], dict[str, float]]
            Date string, dict of {receiver_name: dataset}, and timings

        """
        if self.dry_run:
            self._logger.info(
                "dry_run_mode", message="Simulating processing without execution"
            )
            self.print_preview()
            return

        grouped = self._group_by_date_and_receiver()
        filtered_dates = self._filter_dates(grouped, start_from, end_at)

        if not filtered_dates:
            self._logger.warning(
                "no_dates_in_range", start_from=start_from, end_at=end_at
            )
            return

        self._logger.info(
            "process_by_date_started",
            total_dates=len(filtered_dates),
            date_range_start=filtered_dates[0][0],
            date_range_end=filtered_dates[-1][0],
            batch_hours=self.batch_hours,
            n_max_workers=self.n_max_workers,
        )

        overall_start = _time.monotonic()

        # Validate batch floor against first receiver's file count
        _first_date_key, first_receivers = filtered_dates[0]
        first_receiver_info = next(iter(first_receivers.values()))
        first_data_dir = first_receiver_info[0]
        n_files = len(
            list(first_data_dir.glob("*.2*o")) or list(first_data_dir.glob("*.??o"))
        )
        if n_files > 0:
            self._validate_batch_floor(n_files)

        strategy = "multi_day" if self.batch_hours >= 24 else "sub_day"
        self._logger.info(
            "batch_strategy_selected",
            strategy=strategy,
            batch_hours=self.batch_hours,
        )

        if self.batch_hours >= 24:
            yield from self._process_multi_day_batches(filtered_dates, keep_vars)
        else:
            yield from self._process_sub_day_batches(filtered_dates, keep_vars)

        overall_elapsed = _time.monotonic() - overall_start
        self._logger.info(
            "process_by_date_complete",
            total_dates=len(filtered_dates),
            strategy=strategy,
            total_seconds=round(overall_elapsed, 2),
        )


class SingleReceiverProcessor:
    """Process a single receiver for one day.

    Parameters
    ----------
    receiver_name : str
        Actual receiver name (e.g., 'canopy_01', 'reference_01')
    receiver_type : str
        Receiver type ('canopy' or 'reference')
    data_dir : Path
        Directory containing RINEX files
    yyyydoy : YYYYDOY
        Date to process
    site : GnssResearchSite
        Research site
    n_max_workers : int
        Maximum parallel workers

    """

    def __init__(
        self,
        receiver_name: str,
        receiver_type: str,
        data_dir: Path,
        yyyydoy: YYYYDOY,
        site: GnssResearchSite,
        n_max_workers: int = 12,
    ) -> None:
        self.receiver_name = receiver_name
        self.receiver_type = receiver_type
        self.data_dir = data_dir
        self.yyyydoy = yyyydoy
        self.site = site
        self.n_max_workers = n_max_workers
        self._logger = get_logger().bind(
            receiver=receiver_name,
            date=yyyydoy.to_str(),
        )

    def _get_rinex_files(self) -> list[Path]:
        """Get sorted list of RINEX files."""
        return sorted(self.data_dir.glob("*.2*o"))

    def process(self, keep_vars: list[str] | None = None) -> xr.Dataset:
        """Process all RINEX files for this receiver and write to Icechunk.

        Parameters
        ----------
        keep_vars : list[str], optional
            Variables to keep in datasets

        Returns
        -------
        xr.Dataset
            Final daily dataset for this receiver

        """
        rinex_files = self._get_rinex_files()

        if not rinex_files:
            self._logger.error(
                "no_rinex_files_found",
                data_dir=str(self.data_dir),
            )
            msg = f"No RINEX files found in {self.data_dir}"
            raise ValueError(msg)

        self._logger.info(
            "receiver_processing_started",
            rinex_files=len(rinex_files),
        )

        # Create matched dirs for aux data (using first available dir as dummy)
        matched_dirs = MatchedDirs(
            canopy_data_dir=self.data_dir,
            reference_data_dir=self.data_dir,  # Dummy, aux data is date-based
            yyyydoy=self.yyyydoy,
        )

        # Initialize processor with receiver name override
        processor = RinexDataProcessor(
            matched_data_dirs=matched_dirs,
            site=self.site,
            n_max_workers=self.n_max_workers,
        )

        # Process with actual receiver name (NOT type)
        # This requires modifying RinexDataProcessor to accept receiver_name parameter
        return processor._process_receiver(
            rinex_files=rinex_files,
            receiver_name=self.receiver_name,  # Use actual name as group
            receiver_type=self.receiver_type,
            keep_vars=keep_vars,
        )


if __name__ == "__main__":
    from canvod.store import GnssResearchSite
    from canvod.utils.config import load_config

    cfg = load_config()
    site = GnssResearchSite(site_name="Rosalia")

    # Process all dates
    keep_vars = cfg.processing.processing.keep_rnx_vars
    with PipelineOrchestrator(site=site, dry_run=False) as orchestrator:
        for date_key, datasets in orchestrator.process_by_date(keep_vars=keep_vars):
            print(f"\nProcessed date: {date_key}")
            for receiver_name, ds in datasets.items():
                print(f"  {receiver_name}: {dict(ds.sizes)}")
